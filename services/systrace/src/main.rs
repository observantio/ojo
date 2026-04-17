use anyhow::{anyhow, Context, Result};
use host_collectors::{
    default_protocol_for_endpoint, init_meter_provider, init_tracer_provider, ArchiveCompression,
    ArchiveFormat, ArchiveMode, ArchiveStorageConfig, ArchiveWriter, JsonArchiveWriter,
    OtlpSettings, PrefixFilter,
};
use opentelemetry::metrics::{Counter, Gauge};
use opentelemetry::trace::{Span, SpanKind, TraceContextExt, Tracer};
use opentelemetry::KeyValue;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::env;
#[cfg(test)]
use std::fs;
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

mod platform;

#[derive(Clone, Debug)]
struct Config {
    service_name: String,
    instance_id: String,
    poll_interval: Duration,
    otlp_endpoint: String,
    otlp_protocol: String,
    otlp_timeout: Option<Duration>,
    export_interval: Option<Duration>,
    export_timeout: Option<Duration>,
    metrics_include: Vec<String>,
    metrics_exclude: Vec<String>,
    trace_enabled: bool,
    trace_include: Vec<String>,
    trace_exclude: Vec<String>,
    archive_enabled: bool,
    archive_dir: String,
    archive_max_file_bytes: u64,
    archive_retain_files: usize,
    archive_format: ArchiveFormat,
    archive_mode: ArchiveMode,
    archive_window_secs: u64,
    archive_compression: ArchiveCompression,
    trace_stream_max_lines: usize,
    privileged_expected: bool,
    ebpf_enabled: bool,
    uprobes_enabled: bool,
    usdt_enabled: bool,
    runtime_probe_profiles: Vec<String>,
    once: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExportState {
    Pending,
    Connected,
    Reconnecting,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FlushEvent {
    None,
    Connected,
    Reconnected,
    Reconnecting,
    StillUnavailable,
}

fn advance_export_state(current: ExportState, flush_succeeded: bool) -> (ExportState, FlushEvent) {
    if flush_succeeded {
        let event = match current {
            ExportState::Pending => FlushEvent::Connected,
            ExportState::Reconnecting => FlushEvent::Reconnected,
            ExportState::Connected => FlushEvent::None,
        };
        (ExportState::Connected, event)
    } else {
        let event = match current {
            ExportState::Connected => FlushEvent::Reconnecting,
            ExportState::Pending | ExportState::Reconnecting => FlushEvent::StillUnavailable,
        };
        (ExportState::Reconnecting, event)
    }
}

fn log_flush_result(started_at: Instant, flush_succeeded: bool) {
    let elapsed_ms = started_at.elapsed().as_millis();
    if flush_succeeded {
        debug!(elapsed_ms, "force_flush ok");
    } else {
        debug!(elapsed_ms, "force_flush err");
    }
}

fn handle_flush_event(event: FlushEvent, flush_error: Option<&dyn std::fmt::Display>) {
    if let Some(err) = flush_error {
        match event {
            FlushEvent::Reconnecting => warn!(error = %err, "Exporter flush failed; reconnecting"),
            FlushEvent::StillUnavailable => warn!(error = %err, "Exporter still unavailable"),
            FlushEvent::None | FlushEvent::Connected | FlushEvent::Reconnected => {}
        }
    } else {
        match event {
            FlushEvent::Connected => info!("Exporter connected successfully"),
            FlushEvent::Reconnected => info!("Exporter reconnected successfully"),
            FlushEvent::None | FlushEvent::Reconnecting | FlushEvent::StillUnavailable => {}
        }
    }
}

fn record_exporter_state(instruments: &Instruments, filter: &PrefixFilter, state: ExportState) {
    let connected = matches!(state, ExportState::Connected) as u64;
    let reconnecting = matches!(state, ExportState::Reconnecting) as u64;
    record_u64(
        &instruments.exporter_available,
        filter,
        "system.systrace.exporter.available",
        connected,
    );
    record_u64(
        &instruments.exporter_reconnecting,
        filter,
        "system.systrace.exporter.reconnecting",
        reconnecting,
    );
}

#[derive(Clone)]
struct Instruments {
    source_available: Gauge<u64>,
    up: Gauge<u64>,
    tracefs_available: Gauge<u64>,
    etw_available: Gauge<u64>,
    tracing_on: Gauge<u64>,
    tracers_available: Gauge<u64>,
    events_total: Gauge<u64>,
    events_enabled: Gauge<u64>,
    buffer_total_kb: Gauge<u64>,
    etw_sessions_total: Gauge<u64>,
    etw_sessions_running: Gauge<u64>,
    etw_providers_total: Gauge<u64>,
    event_categories_total: Gauge<u64>,
    trace_sample_lines_total: Gauge<u64>,
    exporter_available: Gauge<u64>,
    exporter_reconnecting: Gauge<u64>,
    exporter_errors_total: Counter<u64>,
    context_switches_per_sec: Gauge<f64>,
    interrupts_per_sec: Gauge<f64>,
    system_calls_per_sec: Gauge<f64>,
    system_calls_source: Gauge<u64>,
    system_calls_coverage_ratio: Gauge<f64>,
    dpcs_per_sec: Gauge<f64>,
    process_forks_per_sec: Gauge<f64>,
    run_queue_depth: Gauge<f64>,
    processes_total: Gauge<u64>,
    threads_total: Gauge<u64>,
    kernel_stack_samples_total: Gauge<u64>,
    user_stack_samples_total: Gauge<u64>,
    collection_errors: Gauge<u64>,
    enabled_events_inventory_total: Gauge<u64>,
    high_value_categories_targeted: Gauge<u64>,
    high_value_categories_enabled: Gauge<u64>,
    trace_stream_lines_captured_total: Gauge<u64>,
    trace_stream_continuity: Gauge<u64>,
    trace_dropped_events_total: Gauge<u64>,
    trace_overrun_events_total: Gauge<u64>,
    syscall_enter_enabled: Gauge<u64>,
    syscall_exit_enabled: Gauge<u64>,
    privileged_mode: Gauge<u64>,
    ebpf_available: Gauge<u64>,
    uprobes_available: Gauge<u64>,
    usdt_available: Gauge<u64>,
    symbolizer_available: Gauge<u64>,
    archive_writer_healthy: Gauge<u64>,
    archive_events_total: Gauge<u64>,
    archive_bytes_total: Gauge<u64>,
    runtime_probes_configured_total: Gauge<u64>,
}

impl Instruments {
    fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self {
            source_available: meter.u64_gauge("system.systrace.source.available").build(),
            up: meter.u64_gauge("system.systrace.up").build(),
            tracefs_available: meter.u64_gauge("system.systrace.tracefs.available").build(),
            etw_available: meter.u64_gauge("system.systrace.etw.available").build(),
            tracing_on: meter.u64_gauge("system.systrace.tracing.on").build(),
            tracers_available: meter.u64_gauge("system.systrace.tracers.available").build(),
            events_total: meter.u64_gauge("system.systrace.events.total").build(),
            events_enabled: meter.u64_gauge("system.systrace.events.enabled").build(),
            buffer_total_kb: meter.u64_gauge("system.systrace.buffer.total_kb").build(),
            etw_sessions_total: meter
                .u64_gauge("system.systrace.etw.sessions.total")
                .build(),
            etw_sessions_running: meter
                .u64_gauge("system.systrace.etw.sessions.running")
                .build(),
            etw_providers_total: meter
                .u64_gauge("system.systrace.etw.providers.total")
                .build(),
            event_categories_total: meter
                .u64_gauge("system.systrace.event.categories.total")
                .build(),
            trace_sample_lines_total: meter
                .u64_gauge("system.systrace.trace.sample_lines.total")
                .build(),
            exporter_available: meter
                .u64_gauge("system.systrace.exporter.available")
                .build(),
            exporter_reconnecting: meter
                .u64_gauge("system.systrace.exporter.reconnecting")
                .build(),
            exporter_errors_total: meter
                .u64_counter("system.systrace.exporter.errors.total")
                .build(),
            context_switches_per_sec: meter
                .f64_gauge("system.systrace.context_switches_per_sec")
                .with_unit("{switches}/s")
                .build(),
            interrupts_per_sec: meter
                .f64_gauge("system.systrace.interrupts_per_sec")
                .with_unit("{interrupts}/s")
                .build(),
            system_calls_per_sec: meter
                .f64_gauge("system.systrace.system_calls_per_sec")
                .with_unit("{syscalls}/s")
                .build(),
            system_calls_source: meter
                .u64_gauge("system.systrace.system_calls.source")
                .build(),
            system_calls_coverage_ratio: meter
                .f64_gauge("system.systrace.system_calls.coverage_ratio")
                .build(),
            dpcs_per_sec: meter
                .f64_gauge("system.systrace.dpcs_per_sec")
                .with_unit("{dpc}/s")
                .build(),
            process_forks_per_sec: meter
                .f64_gauge("system.systrace.process_forks_per_sec")
                .with_unit("{forks}/s")
                .build(),
            run_queue_depth: meter
                .f64_gauge("system.systrace.run_queue.depth")
                .with_unit("{tasks}")
                .build(),
            processes_total: meter.u64_gauge("system.systrace.processes.total").build(),
            threads_total: meter.u64_gauge("system.systrace.threads.total").build(),
            kernel_stack_samples_total: meter
                .u64_gauge("system.systrace.trace.kernel_stack_samples.total")
                .build(),
            user_stack_samples_total: meter
                .u64_gauge("system.systrace.trace.user_stack_samples.total")
                .build(),
            collection_errors: meter.u64_gauge("system.systrace.collection.errors").build(),
            enabled_events_inventory_total: meter
                .u64_gauge("system.systrace.events.enabled_inventory.total")
                .build(),
            high_value_categories_targeted: meter
                .u64_gauge("system.systrace.coverage.high_value_categories.targeted")
                .build(),
            high_value_categories_enabled: meter
                .u64_gauge("system.systrace.coverage.high_value_categories.enabled")
                .build(),
            trace_stream_lines_captured_total: meter
                .u64_gauge("system.systrace.trace.stream.lines_captured.total")
                .build(),
            trace_stream_continuity: meter
                .u64_gauge("system.systrace.trace.stream.continuity")
                .build(),
            trace_dropped_events_total: meter
                .u64_gauge("system.systrace.trace.dropped_events.total")
                .build(),
            trace_overrun_events_total: meter
                .u64_gauge("system.systrace.trace.overrun_events.total")
                .build(),
            syscall_enter_enabled: meter
                .u64_gauge("system.systrace.syscalls.enter.enabled")
                .build(),
            syscall_exit_enabled: meter
                .u64_gauge("system.systrace.syscalls.exit.enabled")
                .build(),
            privileged_mode: meter.u64_gauge("system.systrace.privileged.mode").build(),
            ebpf_available: meter.u64_gauge("system.systrace.ebpf.available").build(),
            uprobes_available: meter.u64_gauge("system.systrace.uprobes.available").build(),
            usdt_available: meter.u64_gauge("system.systrace.usdt.available").build(),
            symbolizer_available: meter
                .u64_gauge("system.systrace.symbolizer.available")
                .build(),
            archive_writer_healthy: meter
                .u64_gauge("system.systrace.archive.writer.healthy")
                .build(),
            archive_events_total: meter
                .u64_gauge("system.systrace.archive.events.total")
                .build(),
            archive_bytes_total: meter
                .u64_gauge("system.systrace.archive.bytes.total")
                .build(),
            runtime_probes_configured_total: meter
                .u64_gauge("system.systrace.runtime.probes.configured.total")
                .build(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct SystraceSnapshot {
    pub(crate) available: bool,
    pub(crate) tracefs_available: bool,
    pub(crate) etw_available: bool,
    pub(crate) tracing_on: bool,
    pub(crate) current_tracer: String,
    pub(crate) tracers_available: u64,
    pub(crate) events_total: u64,
    pub(crate) events_enabled: u64,
    pub(crate) buffer_total_kb: u64,
    pub(crate) etw_sessions_total: u64,
    pub(crate) etw_sessions_running: u64,
    pub(crate) etw_providers_total: u64,
    pub(crate) event_categories_total: u64,
    pub(crate) trace_sample_lines_total: u64,
    pub(crate) trace_sample: Vec<String>,
    pub(crate) context_switches_per_sec: f64,
    pub(crate) interrupts_per_sec: f64,
    pub(crate) system_calls_per_sec: f64,
    pub(crate) system_calls_source: String,
    pub(crate) system_calls_source_code: u64,
    pub(crate) system_calls_coverage_ratio: f64,
    pub(crate) dpcs_per_sec: f64,
    pub(crate) process_forks_per_sec: f64,
    pub(crate) run_queue_depth: f64,
    pub(crate) processes_total: u64,
    pub(crate) threads_total: u64,
    pub(crate) kernel_stack_samples_total: u64,
    pub(crate) user_stack_samples_total: u64,
    pub(crate) collection_errors: u64,
    pub(crate) enabled_events_inventory_total: u64,
    pub(crate) enabled_events_inventory_sample: Vec<String>,
    pub(crate) high_value_categories_targeted: u64,
    pub(crate) high_value_categories_enabled: u64,
    pub(crate) trace_stream_lines_captured_total: u64,
    pub(crate) trace_stream_continuity: bool,
    pub(crate) trace_dropped_events_total: u64,
    pub(crate) trace_overrun_events_total: u64,
    pub(crate) syscall_enter_enabled: bool,
    pub(crate) syscall_exit_enabled: bool,
    pub(crate) privileged_mode: bool,
    pub(crate) ebpf_available: bool,
    pub(crate) uprobes_available: bool,
    pub(crate) usdt_available: bool,
    pub(crate) symbolizer_available: bool,
    pub(crate) archive_writer_healthy: bool,
    pub(crate) archive_events_total: u64,
    pub(crate) archive_bytes_total: u64,
    pub(crate) runtime_probes_configured_total: u64,
}

struct ArchivePipeline {
    enabled: bool,
    #[cfg(test)]
    max_file_bytes: u64,
    #[cfg(test)]
    retain_files: usize,
    writer: JsonArchiveWriter,
    total_events: u64,
    total_bytes: u64,
    healthy: bool,
    last_error: Option<String>,
}

impl ArchivePipeline {
    fn from_config(cfg: &Config) -> Self {
        let writer_cfg = ArchiveStorageConfig {
            enabled: cfg.archive_enabled,
            archive_dir: cfg.archive_dir.clone(),
            max_file_bytes: cfg.archive_max_file_bytes,
            retain_files: cfg.archive_retain_files,
            file_stem: "systrace".to_string(),
            format: cfg.archive_format.clone(),
            mode: cfg.archive_mode.clone(),
            window_secs: cfg.archive_window_secs,
            compression: cfg.archive_compression.clone(),
        };
        Self {
            enabled: cfg.archive_enabled,
            #[cfg(test)]
            max_file_bytes: cfg.archive_max_file_bytes,
            #[cfg(test)]
            retain_files: cfg.archive_retain_files,
            writer: {
                let mut writer = JsonArchiveWriter::from_config(&writer_cfg);
                writer.set_default_identity(&cfg.service_name, &cfg.instance_id);
                writer
            },
            total_events: 0,
            total_bytes: 0,
            healthy: true,
            last_error: None,
        }
    }

    fn write_snapshot(&mut self, snapshot: &SystraceSnapshot) {
        if !self.enabled {
            return;
        }

        if let Err(err) = self.write_snapshot_impl(snapshot) {
            self.healthy = false;
            self.last_error = Some(err.to_string());
            warn!(error = %err, "archive pipeline write failed");
        } else if !self.healthy {
            let detail = self
                .last_error
                .clone()
                .unwrap_or_else(|| "unknown archive writer error".to_string());
            warn!(error = %detail, "archive pipeline write failed");
        }
    }

    fn write_snapshot_impl(&mut self, snapshot: &SystraceSnapshot) -> Result<()> {
        let snapshot_json = serde_json::to_value(snapshot).context("snapshot serialize failed")?;
        self.writer.write_snapshot(&snapshot_json);
        if !snapshot.trace_sample.is_empty() {
            let rows = snapshot
                .trace_sample
                .iter()
                .map(|line| {
                    serde_json::json!({
                        "severity_text": "trace",
                        "source": "systrace",
                        "watch_target": "trace_sample",
                        "body": line,
                        "system_calls_source": snapshot.system_calls_source,
                        "processes_total": snapshot.processes_total,
                        "threads_total": snapshot.threads_total,
                    })
                })
                .collect::<Vec<_>>();
            self.writer.write_log_batch(&rows);
        }
        self.writer.flush();
        self.total_events = self.writer.total_records;
        self.total_bytes = self.writer.total_bytes;
        self.healthy = self.writer.healthy;
        self.last_error = self.writer.last_error.clone();
        Ok(())
    }

    #[cfg(test)]
    fn rotate_if_needed(&self, path: &str) -> Result<()> {
        let metadata = match fs::metadata(path) {
            Ok(md) => md,
            Err(_) => return Ok(()),
        };
        if metadata.len() < self.max_file_bytes {
            return Ok(());
        }

        for idx in (1..=self.retain_files).rev() {
            let src = format!("{}.{}", path, idx);
            let dst = format!("{}.{}", path, idx + 1);
            if std::path::Path::new(&src).exists() {
                let _ = fs::rename(&src, &dst);
            }
        }
        let first = format!("{}.1", path);
        let _ = fs::rename(path, first);
        Ok(())
    }

    #[cfg(test)]
    fn prune_prefix(&self, prefix: &str) -> Result<()> {
        if self.retain_files == 0 {
            return Ok(());
        }
        let cutoff = self.retain_files + 1;
        let mut idx = cutoff + 1;
        loop {
            let candidate = format!("{}.{}", prefix, idx);
            if !std::path::Path::new(&candidate).exists() {
                break;
            }
            fs::remove_file(&candidate)
                .with_context(|| format!("failed to remove rotated archive '{}'", candidate))?;
            idx += 1;
        }
        Ok(())
    }
}

fn bool_as_u64(value: bool) -> u64 {
    if value {
        1
    } else {
        0
    }
}

fn record_u64(instrument: &Gauge<u64>, filter: &PrefixFilter, name: &str, value: u64) {
    if filter.allows(name) {
        instrument.record(value, &[] as &[KeyValue]);
    }
}

fn record_f64(instrument: &Gauge<f64>, filter: &PrefixFilter, name: &str, value: f64) {
    if filter.allows(name) {
        instrument.record(value, &[] as &[KeyValue]);
    }
}

fn record_snapshot(instruments: &Instruments, filter: &PrefixFilter, snapshot: &SystraceSnapshot) {
    record_u64(
        &instruments.source_available,
        filter,
        "system.systrace.source.available",
        bool_as_u64(snapshot.available),
    );
    record_u64(
        &instruments.up,
        filter,
        "system.systrace.up",
        bool_as_u64(snapshot.available),
    );
    record_u64(
        &instruments.tracefs_available,
        filter,
        "system.systrace.tracefs.available",
        bool_as_u64(snapshot.tracefs_available),
    );
    record_u64(
        &instruments.etw_available,
        filter,
        "system.systrace.etw.available",
        bool_as_u64(snapshot.etw_available),
    );
    record_u64(
        &instruments.tracing_on,
        filter,
        "system.systrace.tracing.on",
        bool_as_u64(snapshot.tracing_on),
    );
    record_u64(
        &instruments.tracers_available,
        filter,
        "system.systrace.tracers.available",
        snapshot.tracers_available,
    );
    record_u64(
        &instruments.events_total,
        filter,
        "system.systrace.events.total",
        snapshot.events_total,
    );
    record_u64(
        &instruments.events_enabled,
        filter,
        "system.systrace.events.enabled",
        snapshot.events_enabled,
    );
    record_u64(
        &instruments.buffer_total_kb,
        filter,
        "system.systrace.buffer.total_kb",
        snapshot.buffer_total_kb,
    );
    record_u64(
        &instruments.etw_sessions_total,
        filter,
        "system.systrace.etw.sessions.total",
        snapshot.etw_sessions_total,
    );
    record_u64(
        &instruments.etw_sessions_running,
        filter,
        "system.systrace.etw.sessions.running",
        snapshot.etw_sessions_running,
    );
    record_u64(
        &instruments.etw_providers_total,
        filter,
        "system.systrace.etw.providers.total",
        snapshot.etw_providers_total,
    );
    record_u64(
        &instruments.event_categories_total,
        filter,
        "system.systrace.event.categories.total",
        snapshot.event_categories_total,
    );
    record_u64(
        &instruments.trace_sample_lines_total,
        filter,
        "system.systrace.trace.sample_lines.total",
        snapshot.trace_sample_lines_total,
    );
    record_f64(
        &instruments.context_switches_per_sec,
        filter,
        "system.systrace.context_switches_per_sec",
        snapshot.context_switches_per_sec,
    );
    record_f64(
        &instruments.interrupts_per_sec,
        filter,
        "system.systrace.interrupts_per_sec",
        snapshot.interrupts_per_sec,
    );
    record_f64(
        &instruments.system_calls_per_sec,
        filter,
        "system.systrace.system_calls_per_sec",
        snapshot.system_calls_per_sec,
    );
    record_u64(
        &instruments.system_calls_source,
        filter,
        "system.systrace.system_calls.source",
        snapshot.system_calls_source_code,
    );
    record_f64(
        &instruments.system_calls_coverage_ratio,
        filter,
        "system.systrace.system_calls.coverage_ratio",
        snapshot.system_calls_coverage_ratio,
    );
    record_f64(
        &instruments.dpcs_per_sec,
        filter,
        "system.systrace.dpcs_per_sec",
        snapshot.dpcs_per_sec,
    );
    record_f64(
        &instruments.process_forks_per_sec,
        filter,
        "system.systrace.process_forks_per_sec",
        snapshot.process_forks_per_sec,
    );
    record_f64(
        &instruments.run_queue_depth,
        filter,
        "system.systrace.run_queue.depth",
        snapshot.run_queue_depth,
    );
    record_u64(
        &instruments.processes_total,
        filter,
        "system.systrace.processes.total",
        snapshot.processes_total,
    );
    record_u64(
        &instruments.threads_total,
        filter,
        "system.systrace.threads.total",
        snapshot.threads_total,
    );
    record_u64(
        &instruments.kernel_stack_samples_total,
        filter,
        "system.systrace.trace.kernel_stack_samples.total",
        snapshot.kernel_stack_samples_total,
    );
    record_u64(
        &instruments.user_stack_samples_total,
        filter,
        "system.systrace.trace.user_stack_samples.total",
        snapshot.user_stack_samples_total,
    );
    record_u64(
        &instruments.collection_errors,
        filter,
        "system.systrace.collection.errors",
        snapshot.collection_errors,
    );
    record_u64(
        &instruments.enabled_events_inventory_total,
        filter,
        "system.systrace.events.enabled_inventory.total",
        snapshot.enabled_events_inventory_total,
    );
    record_u64(
        &instruments.high_value_categories_targeted,
        filter,
        "system.systrace.coverage.high_value_categories.targeted",
        snapshot.high_value_categories_targeted,
    );
    record_u64(
        &instruments.high_value_categories_enabled,
        filter,
        "system.systrace.coverage.high_value_categories.enabled",
        snapshot.high_value_categories_enabled,
    );
    record_u64(
        &instruments.trace_stream_lines_captured_total,
        filter,
        "system.systrace.trace.stream.lines_captured.total",
        snapshot.trace_stream_lines_captured_total,
    );
    record_u64(
        &instruments.trace_stream_continuity,
        filter,
        "system.systrace.trace.stream.continuity",
        bool_as_u64(snapshot.trace_stream_continuity),
    );
    record_u64(
        &instruments.trace_dropped_events_total,
        filter,
        "system.systrace.trace.dropped_events.total",
        snapshot.trace_dropped_events_total,
    );
    record_u64(
        &instruments.trace_overrun_events_total,
        filter,
        "system.systrace.trace.overrun_events.total",
        snapshot.trace_overrun_events_total,
    );
    record_u64(
        &instruments.syscall_enter_enabled,
        filter,
        "system.systrace.syscalls.enter.enabled",
        bool_as_u64(snapshot.syscall_enter_enabled),
    );
    record_u64(
        &instruments.syscall_exit_enabled,
        filter,
        "system.systrace.syscalls.exit.enabled",
        bool_as_u64(snapshot.syscall_exit_enabled),
    );
    record_u64(
        &instruments.privileged_mode,
        filter,
        "system.systrace.privileged.mode",
        bool_as_u64(snapshot.privileged_mode),
    );
    record_u64(
        &instruments.ebpf_available,
        filter,
        "system.systrace.ebpf.available",
        bool_as_u64(snapshot.ebpf_available),
    );
    record_u64(
        &instruments.uprobes_available,
        filter,
        "system.systrace.uprobes.available",
        bool_as_u64(snapshot.uprobes_available),
    );
    record_u64(
        &instruments.usdt_available,
        filter,
        "system.systrace.usdt.available",
        bool_as_u64(snapshot.usdt_available),
    );
    record_u64(
        &instruments.symbolizer_available,
        filter,
        "system.systrace.symbolizer.available",
        bool_as_u64(snapshot.symbolizer_available),
    );
    record_u64(
        &instruments.archive_writer_healthy,
        filter,
        "system.systrace.archive.writer.healthy",
        bool_as_u64(snapshot.archive_writer_healthy),
    );
    record_u64(
        &instruments.archive_events_total,
        filter,
        "system.systrace.archive.events.total",
        snapshot.archive_events_total,
    );
    record_u64(
        &instruments.archive_bytes_total,
        filter,
        "system.systrace.archive.bytes.total",
        snapshot.archive_bytes_total,
    );
    record_u64(
        &instruments.runtime_probes_configured_total,
        filter,
        "system.systrace.runtime.probes.configured.total",
        snapshot.runtime_probes_configured_total,
    );
}

fn emit_trace_snapshot<T: Tracer>(
    tracer: &T,
    root_span: &mut T::Span,
    filter: &PrefixFilter,
    snapshot: &SystraceSnapshot,
) {
    if !filter.allows("systrace.collect") {
        return;
    }

    root_span.set_attribute(KeyValue::new(
        "systrace.collect.platform",
        if snapshot.tracefs_available {
            "linux"
        } else if snapshot.etw_available {
            "windows"
        } else {
            "unavailable"
        },
    ));
    root_span.set_attribute(KeyValue::new("systrace.available", snapshot.available));
    root_span.set_attribute(KeyValue::new(
        "systrace.tracefs.available",
        snapshot.tracefs_available,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.etw.available",
        snapshot.etw_available,
    ));
    root_span.set_attribute(KeyValue::new("systrace.tracing.on", snapshot.tracing_on));
    root_span.set_attribute(KeyValue::new(
        "systrace.current_tracer",
        snapshot.current_tracer.clone(),
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.events.total",
        snapshot.events_total as i64,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.events.enabled",
        snapshot.events_enabled as i64,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.event.categories.total",
        snapshot.event_categories_total as i64,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.etw.providers.total",
        snapshot.etw_providers_total as i64,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.trace.sample_lines.total",
        snapshot.trace_sample_lines_total as i64,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.context_switches_per_sec",
        snapshot.context_switches_per_sec,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.interrupts_per_sec",
        snapshot.interrupts_per_sec,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.system_calls_per_sec",
        snapshot.system_calls_per_sec,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.system_calls.source",
        snapshot.system_calls_source.clone(),
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.system_calls.coverage_ratio",
        snapshot.system_calls_coverage_ratio,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.dpcs_per_sec",
        snapshot.dpcs_per_sec,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.process_forks_per_sec",
        snapshot.process_forks_per_sec,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.run_queue.depth",
        snapshot.run_queue_depth,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.processes.total",
        snapshot.processes_total as i64,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.threads.total",
        snapshot.threads_total as i64,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.trace.kernel_stack_samples.total",
        snapshot.kernel_stack_samples_total as i64,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.trace.user_stack_samples.total",
        snapshot.user_stack_samples_total as i64,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.events.enabled_inventory.total",
        snapshot.enabled_events_inventory_total as i64,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.coverage.high_value_categories.targeted",
        snapshot.high_value_categories_targeted as i64,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.coverage.high_value_categories.enabled",
        snapshot.high_value_categories_enabled as i64,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.trace.stream.lines_captured.total",
        snapshot.trace_stream_lines_captured_total as i64,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.trace.stream.continuity",
        snapshot.trace_stream_continuity,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.trace.dropped_events.total",
        snapshot.trace_dropped_events_total as i64,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.trace.overrun_events.total",
        snapshot.trace_overrun_events_total as i64,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.syscalls.enter.enabled",
        snapshot.syscall_enter_enabled,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.syscalls.exit.enabled",
        snapshot.syscall_exit_enabled,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.privileged.mode",
        snapshot.privileged_mode,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.ebpf.available",
        snapshot.ebpf_available,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.uprobes.available",
        snapshot.uprobes_available,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.usdt.available",
        snapshot.usdt_available,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.symbolizer.available",
        snapshot.symbolizer_available,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.archive.writer.healthy",
        snapshot.archive_writer_healthy,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.archive.events.total",
        snapshot.archive_events_total as i64,
    ));
    root_span.set_attribute(KeyValue::new(
        "systrace.archive.bytes.total",
        snapshot.archive_bytes_total as i64,
    ));

    let root_cx = opentelemetry::Context::current()
        .with_remote_span_context(root_span.span_context().clone());
    let platform_component = infer_platform_component(snapshot);
    let mut summary = tracer
        .span_builder("systrace.snapshot")
        .with_kind(SpanKind::Internal)
        .start_with_context(tracer, &root_cx);
    summary.set_attribute(KeyValue::new(
        "systrace.component",
        platform_component.clone(),
    ));
    summary.set_attribute(KeyValue::new("systrace.available", snapshot.available));
    summary.set_attribute(KeyValue::new(
        "systrace.trace.sample_lines.total",
        snapshot.trace_sample_lines_total as i64,
    ));
    summary.set_attribute(KeyValue::new(
        "systrace.events.total",
        snapshot.events_total as i64,
    ));
    summary.set_attribute(KeyValue::new(
        "systrace.processes.total",
        snapshot.processes_total as i64,
    ));
    summary.set_attribute(KeyValue::new(
        "systrace.threads.total",
        snapshot.threads_total as i64,
    ));
    summary.set_attribute(KeyValue::new(
        "systrace.events.enabled_inventory.total",
        snapshot.enabled_events_inventory_total as i64,
    ));
    summary.set_attribute(KeyValue::new(
        "systrace.trace.stream.lines_captured.total",
        snapshot.trace_stream_lines_captured_total as i64,
    ));
    summary.set_attribute(KeyValue::new(
        "systrace.trace.stream.continuity",
        snapshot.trace_stream_continuity,
    ));
    let parent_cx =
        opentelemetry::Context::current().with_remote_span_context(summary.span_context().clone());
    summary.end();

    let sampled_lines: Vec<&str> = snapshot
        .trace_sample
        .iter()
        .take(20)
        .map(String::as_str)
        .collect();
    let line_delta_us = derive_trace_line_delta_us(&sampled_lines);

    let mut component_summaries: BTreeMap<String, ComponentTraceSummary> = BTreeMap::new();
    for (index, line) in sampled_lines.iter().enumerate() {
        let component =
            infer_trace_line_component(line).unwrap_or_else(|| platform_component.clone());
        let summary = component_summaries.entry(component).or_default();
        summary.lines_total = summary.lines_total.saturating_add(1);
        summary.delta_us_total = summary
            .delta_us_total
            .saturating_add(line_delta_us.get(index).copied().unwrap_or(1));
        if summary.sample_line.is_empty() {
            summary.sample_line = (*line).to_string();
        }
    }

    let entry_key = find_component_key(
        &component_summaries,
        &["kernel.entry_syscall", "kernel.entry_sysca", "kernel.entry"],
    );
    let do_syscall_key = find_component_key(
        &component_summaries,
        &["kernel.do_syscall", "kernel.do_syscal", "kernel.do_sys"],
    );
    let syscall_trace_key = find_component_key(
        &component_summaries,
        &[
            "kernel.syscall_trace",
            "kernel.syscall_tra",
            "kernel.syscall",
        ],
    );
    let code_key = find_component_key(&component_summaries, &["kernel.code"]);
    let traceiter_key = find_component_key(
        &component_summaries,
        &["kernel.__traceiter", "kernel.traceiter"],
    );
    let userstack_key = find_component_key(&component_summaries, &["kernel.userstack"]);

    if let Some(key) = userstack_key.as_deref() {
        if let Some(summary_stats) = component_summaries.remove(key) {
            emit_component_summary_span(tracer, &parent_cx, key, &summary_stats, false);
        }
    }

    let mut syscall_parent_cx = parent_cx.clone();
    let mut emitted_syscall_chain = false;
    for key in [
        entry_key.as_deref(),
        do_syscall_key.as_deref(),
        syscall_trace_key.as_deref(),
        code_key.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if let Some(summary_stats) = component_summaries.remove(key) {
            syscall_parent_cx =
                emit_component_summary_span(tracer, &syscall_parent_cx, key, &summary_stats, false);
            emitted_syscall_chain = true;
        }
    }

    if let Some(key) = traceiter_key.as_deref() {
        if let Some(summary_stats) = component_summaries.remove(key) {
            let traceiter_cx = emit_component_summary_span(
                tracer,
                if emitted_syscall_chain {
                    &syscall_parent_cx
                } else {
                    &parent_cx
                },
                key,
                &summary_stats,
                false,
            );
            let mut probe_components: Vec<String> = component_summaries
                .keys()
                .filter(|name| name.starts_with("kernel.") && !is_reserved_component(name))
                .cloned()
                .collect();
            probe_components.sort_unstable();
            for component in probe_components {
                if let Some(probe_summary) = component_summaries.remove(&component) {
                    let _ = emit_component_summary_span(
                        tracer,
                        &traceiter_cx,
                        &component,
                        &probe_summary,
                        true,
                    );
                }
            }
        }
    }

    if emitted_syscall_chain {
        let return_summary = ComponentTraceSummary {
            lines_total: 1,
            delta_us_total: line_delta_us.iter().copied().sum::<u64>().max(1),
            sample_line: "synthetic syscall return to userspace".to_string(),
        };
        let _ = emit_component_summary_span(
            tracer,
            &syscall_parent_cx,
            "kernel.return_to_userspace",
            &return_summary,
            true,
        );
    }

    let mut remaining_components: Vec<String> = component_summaries.keys().cloned().collect();
    remaining_components.sort_unstable();
    for component in remaining_components {
        if let Some(summary_stats) = component_summaries.remove(&component) {
            let _ =
                emit_component_summary_span(tracer, &parent_cx, &component, &summary_stats, false);
        }
    }
}

fn emit_component_summary_span<T: Tracer>(
    tracer: &T,
    parent_cx: &opentelemetry::Context,
    component: &str,
    summary_stats: &ComponentTraceSummary,
    synthetic: bool,
) -> opentelemetry::Context {
    let mut child = tracer
        .span_builder("systrace.trace.component")
        .with_kind(SpanKind::Client)
        .start_with_context(tracer, parent_cx);
    child.set_attribute(KeyValue::new("peer.service", component.to_string()));
    child.set_attribute(KeyValue::new("systrace.component", component.to_string()));
    child.set_attribute(KeyValue::new(
        "systrace.trace.lines.total",
        summary_stats.lines_total as i64,
    ));
    child.set_attribute(KeyValue::new(
        "systrace.trace.lines.delta_us_total",
        summary_stats.delta_us_total as i64,
    ));
    child.set_attribute(KeyValue::new(
        "systrace.trace.line.sample",
        summary_stats.sample_line.clone(),
    ));
    child.set_attribute(KeyValue::new("systrace.synthetic", synthetic));
    let next_cx =
        opentelemetry::Context::current().with_remote_span_context(child.span_context().clone());
    child.end();
    next_cx
}

#[derive(Default)]
struct ComponentTraceSummary {
    lines_total: u64,
    delta_us_total: u64,
    sample_line: String,
}

fn find_component_key(
    summaries: &BTreeMap<String, ComponentTraceSummary>,
    preferred_prefixes: &[&str],
) -> Option<String> {
    preferred_prefixes.iter().find_map(|prefix| {
        summaries
            .keys()
            .find(|key| key.starts_with(prefix))
            .map(|s| s.to_string())
    })
}

fn is_reserved_component(component: &str) -> bool {
    [
        "kernel.entry_syscall",
        "kernel.entry_sysca",
        "kernel.entry",
        "kernel.do_syscall",
        "kernel.do_syscal",
        "kernel.do_sys",
        "kernel.syscall_trace",
        "kernel.syscall_tra",
        "kernel.syscall",
        "kernel.code",
        "kernel.__traceiter",
        "kernel.traceiter",
        "kernel.userstack",
        "kernel.return_to_userspace",
    ]
    .iter()
    .any(|prefix| component.starts_with(prefix))
}

fn derive_trace_line_delta_us(lines: &[&str]) -> Vec<u64> {
    if lines.is_empty() {
        return Vec::new();
    }
    let mut derived = vec![1u64; lines.len()];
    let offsets: Vec<Option<f64>> = lines
        .iter()
        .map(|line| parse_trace_line_seconds_token(line))
        .collect();
    let known: Vec<usize> = offsets
        .iter()
        .enumerate()
        .filter_map(|(i, ts)| ts.map(|_| i))
        .collect();

    for window in known.windows(2) {
        let left = window[0];
        let right = window[1];
        let left_ts = offsets[left].expect("known index must have a timestamp");
        let right_ts = offsets[right].expect("known index must have a timestamp");
        let width = (right - left) as f64;
        let delta_us = ((right_ts - left_ts) * 1_000_000.0).round();
        let per_line_us = if delta_us.is_finite() && delta_us > 0.0 {
            (delta_us / width).round().max(1.0) as u64
        } else {
            1
        };
        for slot in derived.iter_mut().take(right).skip(left) {
            *slot = per_line_us;
        }
    }
    derived
}

fn parse_trace_line_seconds_token(line: &str) -> Option<f64> {
    line.split_whitespace().find_map(|part| {
        let candidate = part.trim_end_matches(':');
        if !candidate.contains('.') {
            return None;
        }
        if !candidate.chars().any(|ch| ch.is_ascii_digit()) {
            return None;
        }
        if !candidate.chars().all(|ch| ch.is_ascii_digit() || ch == '.') {
            return None;
        }
        candidate.parse::<f64>().ok()
    })
}

#[cfg(test)]
fn parse_trace_line_timestamp(line: &str) -> Option<std::time::SystemTime> {
    let token = line.split_whitespace().find_map(|part| {
        let candidate = part.trim_end_matches(':');
        if candidate.contains('.')
            && candidate.chars().all(|ch| ch.is_ascii_digit() || ch == '.')
            && candidate.chars().any(|ch| ch.is_ascii_digit())
        {
            Some(candidate)
        } else {
            None
        }
    })?;
    let event_offset = parse_trace_line_seconds(token)?;
    let boot_time = uptime_boot_time()?;
    let timestamp = boot_time.checked_add(event_offset)?;
    let now = std::time::SystemTime::now();
    Some(if timestamp > now { now } else { timestamp })
}

#[cfg(test)]
fn parse_trace_line_seconds(value: &str) -> Option<Duration> {
    let mut parts = value.splitn(2, '.');
    let secs = parts.next()?.parse::<u64>().ok()?;
    let nanos = if let Some(frac) = parts.next() {
        let mut digits = frac
            .chars()
            .take(9)
            .filter(|ch| ch.is_ascii_digit())
            .collect::<String>();
        if digits.is_empty() {
            return None;
        }
        while digits.len() < 9 {
            digits.push('0');
        }
        digits.parse::<u32>().ok()?
    } else {
        0
    };
    Some(Duration::new(secs, nanos))
}

#[cfg(test)]
fn uptime_boot_time() -> Option<std::time::SystemTime> {
    let contents = fs::read_to_string("/proc/uptime").ok()?;
    let uptime = contents.split_whitespace().next()?;
    std::time::SystemTime::now().checked_sub(parse_trace_line_seconds(uptime)?)
}

fn infer_platform_component(snapshot: &SystraceSnapshot) -> String {
    if snapshot.tracefs_available {
        "kernel.linux".to_string()
    } else if snapshot.etw_available {
        "kernel.windows".to_string()
    } else {
        "kernel.unknown".to_string()
    }
}

fn infer_trace_line_component(line: &str) -> Option<String> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some(rest) = trimmed.strip_prefix("=>") {
        let symbol = rest.split_whitespace().next().unwrap_or("").trim();
        if symbol.is_empty() {
            return None;
        }
        let stack_component = if symbol.starts_with('<') && symbol.ends_with('>') {
            "userstack"
        } else {
            symbol
        };
        return normalize_component_stem(stack_component);
    }

    let token = trimmed.split_whitespace().next()?;
    let prefix = token.split(':').next().unwrap_or(token);
    let stem = if let Some((name, suffix)) = prefix.rsplit_once('-') {
        if !name.is_empty() && suffix.chars().all(|c| c.is_ascii_digit()) {
            name
        } else {
            prefix
        }
    } else {
        prefix
    };

    normalize_component_stem(stem)
}

fn normalize_component_stem(stem: &str) -> Option<String> {
    let mut normalized = String::new();
    let mut prev_dot = false;
    for ch in stem.chars() {
        let next = match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => Some(ch.to_ascii_lowercase()),
            '/' | ':' | '.' => Some('.'),
            _ => None,
        };
        if let Some(out) = next {
            if out == '.' {
                if prev_dot || normalized.is_empty() {
                    continue;
                }
                prev_dot = true;
            } else {
                prev_dot = false;
            }
            normalized.push(out);
        }
    }

    while normalized.ends_with('.') {
        normalized.pop();
    }
    if normalized.is_empty() {
        return None;
    }
    Some(format!("kernel.{normalized}"))
}

fn parse_bool_env(name: &str) -> Option<bool> {
    env::var(name).ok().and_then(|v| {
        let n = v.trim().to_ascii_lowercase();
        if matches!(n.as_str(), "1" | "true" | "yes" | "on") {
            Some(true)
        } else if matches!(n.as_str(), "0" | "false" | "no" | "off") {
            Some(false)
        } else {
            None
        }
    })
}

fn resolve_default_config_path(local_name: &str, repo_relative: &str) -> String {
    if Path::new(local_name).exists() {
        local_name.to_string()
    } else {
        repo_relative.to_string()
    }
}

fn load_yaml_config_file(config_path: &str) -> Result<FileConfig> {
    let path = Path::new(config_path);
    if !path.exists() {
        return Err(anyhow!("config file '{}' was not found", config_path));
    }
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config file '{}'", config_path))?;
    if contents.trim().is_empty() {
        return Err(anyhow!("config file '{}' is empty", config_path));
    }
    serde_yaml::from_str::<FileConfig>(&contents)
        .with_context(|| format!("failed to parse YAML in '{}'", config_path))
}

fn default_traces_endpoint(metrics_endpoint: &str) -> String {
    if metrics_endpoint.ends_with("/v1/metrics") {
        metrics_endpoint.replace("/v1/metrics", "/v1/traces")
    } else {
        metrics_endpoint.to_string()
    }
}

impl Config {
    fn load() -> Result<Self> {
        let args = env::args().collect::<Vec<_>>();
        Self::load_from_args(&args)
    }

    fn load_from_args(args: &[String]) -> Result<Self> {
        let once =
            args.iter().any(|a| a == "--once") || parse_bool_env("OJO_RUN_ONCE").unwrap_or(false);
        let config_path = args
            .windows(2)
            .find(|p| p[0] == "--config")
            .map(|p| p[1].clone())
            .or_else(|| env::var("OJO_SYSTRACE_CONFIG").ok())
            .unwrap_or_else(|| {
                resolve_default_config_path("systrace.yaml", "services/systrace/systrace.yaml")
            });

        let file_cfg = load_yaml_config_file(&config_path)?;
        let service = file_cfg.service.unwrap_or_default();
        let collection = file_cfg.collection.unwrap_or_default();
        let export = file_cfg.export.unwrap_or_default();
        let otlp = export.otlp.unwrap_or_default();
        let batch = export.batch.unwrap_or_default();
        let metrics = file_cfg.metrics.unwrap_or_default();
        let traces = file_cfg.traces.unwrap_or_default();

        let otlp_endpoint = otlp
            .endpoint
            .or_else(|| env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok())
            .unwrap_or_else(|| "http://127.0.0.1:4318/v1/metrics".to_string());

        let otlp_protocol = otlp
            .protocol
            .or_else(|| env::var("OTEL_EXPORTER_OTLP_PROTOCOL").ok())
            .unwrap_or_else(|| default_protocol_for_endpoint(Some(&otlp_endpoint)));

        Ok(Self {
            service_name: service.name.unwrap_or_else(|| "ojo-systrace".to_string()),
            instance_id: service
                .instance_id
                .unwrap_or_else(host_collectors::hostname),
            poll_interval: Duration::from_secs(collection.poll_interval_secs.unwrap_or(5).max(1)),
            otlp_endpoint,
            otlp_protocol,
            otlp_timeout: otlp.timeout_secs.map(Duration::from_secs),
            export_interval: batch.interval_secs.map(Duration::from_secs),
            export_timeout: batch.timeout_secs.map(Duration::from_secs),
            metrics_include: metrics
                .include
                .unwrap_or_else(|| vec!["system.systrace.".to_string()]),
            metrics_exclude: metrics.exclude.unwrap_or_default(),
            trace_enabled: traces.enabled.unwrap_or(true),
            trace_include: traces
                .include
                .unwrap_or_else(|| vec!["systrace.".to_string()]),
            trace_exclude: traces.exclude.unwrap_or_default(),
            archive_enabled: file_cfg
                .storage
                .as_ref()
                .and_then(|s| s.archive_enabled.or(s.enabled))
                .unwrap_or(true),
            archive_dir: file_cfg
                .storage
                .as_ref()
                .and_then(|s| s.archive_dir.clone())
                .unwrap_or_else(|| "services/systrace/data".to_string()),
            archive_max_file_bytes: file_cfg
                .storage
                .as_ref()
                .and_then(|s| s.archive_max_file_bytes.or(s.max_file_bytes))
                .unwrap_or(64 * 1024 * 1024),
            archive_retain_files: file_cfg
                .storage
                .as_ref()
                .and_then(|s| s.archive_retain_files.or(s.retain_files))
                .unwrap_or(8),
            archive_format: file_cfg
                .storage
                .as_ref()
                .and_then(|s| s.archive_format.clone())
                .map(|s| ArchiveFormat::parse(Some(&s)))
                .unwrap_or_default(),
            archive_mode: file_cfg
                .storage
                .as_ref()
                .and_then(|s| s.archive_mode.clone())
                .map(|s| ArchiveMode::parse(Some(&s)))
                .unwrap_or_default(),
            archive_window_secs: file_cfg
                .storage
                .as_ref()
                .and_then(|s| s.archive_window_secs)
                .unwrap_or(60),
            archive_compression: file_cfg
                .storage
                .as_ref()
                .and_then(|s| s.archive_compression.clone())
                .map(|s| ArchiveCompression::parse(Some(&s)))
                .unwrap_or_default(),
            trace_stream_max_lines: collection.trace_stream_max_lines.unwrap_or(2048).max(1)
                as usize,
            privileged_expected: file_cfg
                .instrumentation
                .as_ref()
                .and_then(|i| i.privileged_expected)
                .unwrap_or(true),
            ebpf_enabled: file_cfg
                .instrumentation
                .as_ref()
                .and_then(|i| i.ebpf_enabled)
                .unwrap_or(true),
            uprobes_enabled: file_cfg
                .instrumentation
                .as_ref()
                .and_then(|i| i.uprobes_enabled)
                .unwrap_or(true),
            usdt_enabled: file_cfg
                .instrumentation
                .as_ref()
                .and_then(|i| i.usdt_enabled)
                .unwrap_or(true),
            runtime_probe_profiles: file_cfg
                .instrumentation
                .as_ref()
                .and_then(|i| i.runtime_probe_profiles.clone())
                .unwrap_or_default(),
            once,
        })
    }
}

fn make_stop_handler(signal: Arc<AtomicBool>) -> impl Fn() + Send + 'static {
    move || {
        signal.store(false, Ordering::SeqCst);
    }
}

fn run() -> Result<()> {
    let dump_snapshot = env::args().any(|arg| arg == "--dump-snapshot");
    let cfg = Config::load()?;
    if dump_snapshot {
        let snapshot = platform::collect_snapshot();
        let snapshot_json = serde_json::to_string_pretty(&snapshot)
            .expect("snapshot serialization should not fail");
        println!("{snapshot_json}");
        return Ok(());
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init()
        .ok();

    let metric_provider = init_meter_provider(&OtlpSettings {
        service_name: cfg.service_name.clone(),
        instance_id: cfg.instance_id.clone(),
        otlp_endpoint: cfg.otlp_endpoint.clone(),
        otlp_protocol: cfg.otlp_protocol.clone(),
        otlp_headers: BTreeMap::new(),
        otlp_compression: None,
        otlp_timeout: cfg.otlp_timeout,
        export_interval: cfg.export_interval,
        export_timeout: cfg.export_timeout,
    })?;

    let trace_provider = if cfg.trace_enabled {
        let traces_endpoint = env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
            .ok()
            .unwrap_or_else(|| default_traces_endpoint(&cfg.otlp_endpoint));
        let traces_protocol = env::var("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
            .ok()
            .unwrap_or_else(|| cfg.otlp_protocol.clone());
        info!(endpoint = %traces_endpoint, protocol = %traces_protocol, "Initializing trace provider");
        Some(init_tracer_provider(&OtlpSettings {
            service_name: cfg.service_name.clone(),
            instance_id: cfg.instance_id.clone(),
            otlp_endpoint: traces_endpoint,
            otlp_protocol: traces_protocol,
            otlp_headers: BTreeMap::new(),
            otlp_compression: None,
            otlp_timeout: cfg.otlp_timeout,
            export_interval: cfg.export_interval,
            export_timeout: cfg.export_timeout,
        })?)
    } else {
        info!("Tracing disabled for systrace service");
        None
    };

    let meter = opentelemetry::global::meter("ojo-systrace");
    let tracer = opentelemetry::global::tracer("ojo-systrace");
    let instruments = Instruments::new(&meter);
    let metric_filter = PrefixFilter::new(cfg.metrics_include.clone(), cfg.metrics_exclude.clone());
    let trace_filter = PrefixFilter::new(cfg.trace_include.clone(), cfg.trace_exclude.clone());
    let mut export_state = ExportState::Pending;
    let mut archive = ArchivePipeline::from_config(&cfg);

    std::env::set_var(
        "OJO_SYSTRACE_TRACE_STREAM_MAX_LINES",
        cfg.trace_stream_max_lines.to_string(),
    );

    let running = Arc::new(AtomicBool::new(true));
    if !cfg.once {
        if let Err(err) = ctrlc::set_handler(make_stop_handler(Arc::clone(&running))) {
            warn!(error = %err, "failed to install signal handler");
        }
    }
    #[cfg(test)]
    let mut iterations = 0u64;
    #[cfg(test)]
    let max_iterations = env::var("OJO_TEST_MAX_ITERATIONS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1);

    while running.load(Ordering::SeqCst) {
        let started_at = Instant::now();
        #[cfg(test)]
        {
            iterations = iterations.saturating_add(1);
        }
        let mut snapshot = if cfg.trace_enabled {
            let mut root_span = tracer.start("systrace.collect");
            let snapshot = platform::collect_snapshot();
            emit_trace_snapshot(&tracer, &mut root_span, &trace_filter, &snapshot);
            root_span.end();
            snapshot
        } else {
            platform::collect_snapshot()
        };

        if !cfg.ebpf_enabled {
            snapshot.ebpf_available = false;
        }
        if !cfg.uprobes_enabled {
            snapshot.uprobes_available = false;
        }
        if !cfg.usdt_enabled {
            snapshot.usdt_available = false;
        }
        snapshot.runtime_probes_configured_total = cfg.runtime_probe_profiles.len() as u64;

        if cfg.privileged_expected && !snapshot.privileged_mode {
            warn!("systrace configured for privileged mode but current process is unprivileged");
        }

        archive.write_snapshot(&snapshot);
        snapshot.archive_writer_healthy = archive.healthy;
        snapshot.archive_events_total = archive.total_events;
        snapshot.archive_bytes_total = archive.total_bytes;

        record_snapshot(&instruments, &metric_filter, &snapshot);
        let _ = metric_provider.force_flush();
        if let Some(provider) = &trace_provider {
            let flush_result = provider.force_flush();
            log_flush_result(started_at, flush_result.is_ok());
            let (next_state, event) = advance_export_state(export_state, flush_result.is_ok());
            if flush_result.is_err() {
                instruments.exporter_errors_total.add(1, &[]);
            }
            record_exporter_state(&instruments, &metric_filter, next_state);
            handle_flush_event(
                event,
                flush_result
                    .as_ref()
                    .err()
                    .map(|err| err as &dyn std::fmt::Display),
            );
            export_state = next_state;
        } else {
            record_exporter_state(&instruments, &metric_filter, ExportState::Pending);
        }

        if cfg.once {
            break;
        }

        let deadline = started_at + cfg.poll_interval;
        while running.load(Ordering::SeqCst) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(100));
        }
        #[cfg(test)]
        if iterations >= max_iterations {
            break;
        }
    }

    let _ = metric_provider.shutdown();
    if let Some(provider) = trace_provider {
        let _ = provider.shutdown();
    }
    Ok(())
}

fn main() -> Result<()> {
    run()
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileConfig {
    service: Option<ServiceSection>,
    collection: Option<CollectionSection>,
    export: Option<ExportSection>,
    storage: Option<StorageSection>,
    instrumentation: Option<InstrumentationSection>,
    metrics: Option<MetricSection>,
    traces: Option<TraceSection>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ServiceSection {
    name: Option<String>,
    instance_id: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct CollectionSection {
    poll_interval_secs: Option<u64>,
    trace_stream_max_lines: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct StorageSection {
    archive_enabled: Option<bool>,
    enabled: Option<bool>,
    archive_dir: Option<String>,
    archive_max_file_bytes: Option<u64>,
    max_file_bytes: Option<u64>,
    archive_retain_files: Option<usize>,
    retain_files: Option<usize>,
    archive_format: Option<String>,
    archive_mode: Option<String>,
    archive_window_secs: Option<u64>,
    archive_compression: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct InstrumentationSection {
    privileged_expected: Option<bool>,
    ebpf_enabled: Option<bool>,
    uprobes_enabled: Option<bool>,
    usdt_enabled: Option<bool>,
    runtime_probe_profiles: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ExportSection {
    otlp: Option<OtlpSection>,
    batch: Option<BatchSection>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct OtlpSection {
    endpoint: Option<String>,
    protocol: Option<String>,
    timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct BatchSection {
    interval_secs: Option<u64>,
    timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct MetricSection {
    include: Option<Vec<String>>,
    exclude: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct TraceSection {
    enabled: Option<bool>,
    include: Option<Vec<String>>,
    exclude: Option<Vec<String>>,
}

#[cfg(test)]
#[path = "tests/main_tests.rs"]
mod tests;
