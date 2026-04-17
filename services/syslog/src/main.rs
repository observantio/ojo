use anyhow::{anyhow, Context, Result};
use host_collectors::{
    default_protocol_for_endpoint, init_meter_provider, ArchiveCompression, ArchiveFormat,
    ArchiveMode, ArchiveStorageConfig, ArchiveWriter, JsonArchiveWriter, OtlpSettings,
    PrefixFilter,
};
use opentelemetry::metrics::{Counter, Gauge};
use opentelemetry::KeyValue;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, VecDeque};
use std::env;
#[cfg(test)]
use std::fs::{self as stdfs};
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
#[cfg(not(coverage))]
use tracing::debug;
use tracing::{info, warn};
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
    logs_endpoint: String,
    logs_timeout: Duration,
    metrics_include: Vec<String>,
    metrics_exclude: Vec<String>,
    max_lines_per_source: usize,
    max_message_bytes: usize,
    watch_files: Vec<WatchedFileConfig>,
    buffer_capacity_records: usize,
    export_batch_size: usize,
    retry_backoff: Duration,
    archive_enabled: bool,
    archive_dir: String,
    archive_max_file_bytes: u64,
    archive_retain_files: usize,
    archive_format: ArchiveFormat,
    archive_mode: ArchiveMode,
    archive_window_secs: u64,
    archive_compression: ArchiveCompression,
    once: bool,
}

#[derive(Clone, Debug, Deserialize)]
struct WatchedFileConfig {
    name: String,
    path: String,
    #[serde(default)]
    source: WatchSource,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WatchSource {
    #[default]
    Application,
    Process,
}

#[derive(Clone)]
struct Instruments {
    source_available: Gauge<u64>,
    up: Gauge<u64>,
    journald_available: Gauge<u64>,
    etw_available: Gauge<u64>,
    dmesg_available: Gauge<u64>,
    process_logs_available: Gauge<u64>,
    application_logs_available: Gauge<u64>,
    file_watch_targets_configured: Gauge<u64>,
    file_watch_targets_active: Gauge<u64>,
    buffer_capacity_records: Gauge<u64>,
    buffer_queued_records: Gauge<u64>,
    exporter_available: Gauge<u64>,
    exporter_reconnecting: Gauge<u64>,
    last_batch_size: Gauge<u64>,
    last_payload_bytes: Gauge<u64>,
    collection_errors: Gauge<u64>,
    logs_collected_total: Counter<u64>,
    logs_exported_total: Counter<u64>,
    logs_retry_total: Counter<u64>,
    logs_export_errors_total: Counter<u64>,
    buffer_dropped_total: Counter<u64>,
}

impl Instruments {
    fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self {
            source_available: meter.u64_gauge("system.syslog.source.available").build(),
            up: meter.u64_gauge("system.syslog.up").build(),
            journald_available: meter.u64_gauge("system.syslog.journald.available").build(),
            etw_available: meter.u64_gauge("system.syslog.etw.available").build(),
            dmesg_available: meter
                .u64_gauge("system.syslog.kernel.dmesg.available")
                .build(),
            process_logs_available: meter
                .u64_gauge("system.syslog.process.logs.available")
                .build(),
            application_logs_available: meter
                .u64_gauge("system.syslog.application.logs.available")
                .build(),
            file_watch_targets_configured: meter
                .u64_gauge("system.syslog.file.watch.targets.configured")
                .build(),
            file_watch_targets_active: meter
                .u64_gauge("system.syslog.file.watch.targets.active")
                .build(),
            buffer_capacity_records: meter
                .u64_gauge("system.syslog.buffer.capacity.records")
                .build(),
            buffer_queued_records: meter
                .u64_gauge("system.syslog.buffer.queued.records")
                .build(),
            exporter_available: meter.u64_gauge("system.syslog.exporter.available").build(),
            exporter_reconnecting: meter
                .u64_gauge("system.syslog.exporter.reconnecting")
                .build(),
            last_batch_size: meter.u64_gauge("system.syslog.logs.batch.size").build(),
            last_payload_bytes: meter.u64_gauge("system.syslog.logs.payload.bytes").build(),
            collection_errors: meter.u64_gauge("system.syslog.collection.errors").build(),
            logs_collected_total: meter
                .u64_counter("system.syslog.logs.collected.total")
                .build(),
            logs_exported_total: meter
                .u64_counter("system.syslog.logs.exported.total")
                .build(),
            logs_retry_total: meter.u64_counter("system.syslog.logs.retry.total").build(),
            logs_export_errors_total: meter
                .u64_counter("system.syslog.logs.export.errors.total")
                .build(),
            buffer_dropped_total: meter
                .u64_counter("system.syslog.buffer.dropped.total")
                .build(),
        }
    }
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

#[derive(Clone, Debug, Default, Serialize)]
struct RuntimeSnapshot {
    available: bool,
    journald_available: bool,
    etw_available: bool,
    dmesg_available: bool,
    process_logs_available: bool,
    application_logs_available: bool,
    file_watch_targets_configured: u64,
    file_watch_targets_active: u64,
    buffer_capacity_records: u64,
    buffer_queued_records: u64,
    exporter_available: bool,
    exporter_reconnecting: bool,
    last_batch_size: u64,
    last_payload_bytes: u64,
    collection_errors: u64,
}

#[derive(Clone, Debug, Serialize)]
struct LogRecord {
    observed_time_unix_nano: u64,
    severity_text: String,
    body: String,
    source: String,
    stream: String,
    watch_target: String,
}

#[derive(Clone, Debug)]
struct ExportTelemetry {
    exported_records: u64,
    retries: u64,
    errors: u64,
    last_batch_size: u64,
    last_payload_bytes: u64,
}

impl ExportTelemetry {
    fn none() -> Self {
        Self {
            exported_records: 0,
            retries: 0,
            errors: 0,
            last_batch_size: 0,
            last_payload_bytes: 0,
        }
    }
}

#[derive(Debug)]
struct LogBuffer {
    capacity: usize,
    queue: VecDeque<LogRecord>,
}

impl LogBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            queue: VecDeque::new(),
        }
    }

    fn len(&self) -> usize {
        self.queue.len()
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn push_many(&mut self, records: impl IntoIterator<Item = LogRecord>) -> u64 {
        let mut dropped = 0u64;
        for record in records {
            if self.queue.len() >= self.capacity {
                let _ = self.queue.pop_front();
                dropped = dropped.saturating_add(1);
            }
            self.queue.push_back(record);
        }
        dropped
    }

    fn pop_batch(&mut self, batch_size: usize) -> Vec<LogRecord> {
        let take = batch_size.max(1).min(self.queue.len());
        let mut out = Vec::with_capacity(take);
        for _ in 0..take {
            if let Some(item) = self.queue.pop_front() {
                out.push(item);
            }
        }
        out
    }

    fn push_front_batch(&mut self, batch: Vec<LogRecord>) {
        for record in batch.into_iter().rev() {
            self.queue.push_front(record);
        }
        while self.queue.len() > self.capacity {
            let _ = self.queue.pop_back();
        }
    }
}

#[derive(Clone, Debug)]
struct OtlpLogExporter {
    endpoint: String,
    service_name: String,
    instance_id: String,
    client: reqwest::blocking::Client,
}

impl OtlpLogExporter {
    fn new(cfg: &Config) -> Result<Self> {
        let client = reqwest::blocking::Client::builder()
            .timeout(cfg.logs_timeout)
            .build()
            .context("failed to build OTLP log exporter client")?;
        Ok(Self {
            endpoint: cfg.logs_endpoint.clone(),
            service_name: cfg.service_name.clone(),
            instance_id: cfg.instance_id.clone(),
            client,
        })
    }

    fn export_batch(&self, batch: &[LogRecord]) -> Result<u64> {
        if batch.is_empty() {
            return Ok(0);
        }

        let payload = build_otlp_logs_payload(&self.service_name, &self.instance_id, batch);
        let body =
            serde_json::to_vec(&payload).expect("OTLP logs payload serialization should not fail");
        let bytes_len = body.len() as u64;

        let response = self
            .client
            .post(&self.endpoint)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .with_context(|| format!("failed to send OTLP log batch to {}", self.endpoint))?;

        if !response.status().is_success() {
            return Err(anyhow!(
                "OTLP log exporter returned non-success status {}",
                response.status()
            ));
        }

        Ok(bytes_len)
    }
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
            file_stem: "syslog".to_string(),
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

    fn write_batch(&mut self, batch: &[LogRecord]) {
        if !self.enabled || batch.is_empty() {
            return;
        }

        if let Err(err) = self.write_batch_impl(batch) {
            self.healthy = false;
            self.last_error = Some(err.to_string());
            warn!(error = %err, "archive write failed for syslog batch");
        } else if !self.healthy {
            let detail = self
                .last_error
                .clone()
                .unwrap_or_else(|| "unknown archive writer error".to_string());
            warn!(error = %detail, "archive write failed for syslog batch");
        }
    }

    fn write_batch_impl(&mut self, batch: &[LogRecord]) -> Result<()> {
        let values = batch
            .iter()
            .map(serde_json::to_value)
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("failed to serialize log records for archive")?;
        self.writer.write_log_batch(&values);
        self.writer.flush();
        self.total_events = self.writer.total_records;
        self.total_bytes = self.writer.total_bytes;
        self.healthy = self.writer.healthy;
        self.last_error = self.writer.last_error.clone();
        Ok(())
    }

    #[cfg(test)]
    fn rotate_if_needed(&self, path: &str) -> Result<()> {
        let metadata = match stdfs::metadata(path) {
            Ok(md) => md,
            Err(_) => return Ok(()),
        };
        if metadata.len() < self.max_file_bytes {
            return Ok(());
        }

        for idx in (1..=self.retain_files).rev() {
            let src = format!("{}.{}", path, idx);
            let dst = format!("{}.{}", path, idx + 1);
            if Path::new(&src).exists() {
                let _ = stdfs::rename(&src, &dst);
            }
        }
        let _ = stdfs::rename(path, format!("{}.1", path));
        Ok(())
    }

    #[cfg(test)]
    fn prune_rotated_files(&self, prefix: &str) -> Result<()> {
        if self.retain_files == 0 {
            return Ok(());
        }
        let mut idx = self.retain_files + 2;
        loop {
            let candidate = format!("{}.{}", prefix, idx);
            if !Path::new(&candidate).exists() {
                break;
            }
            stdfs::remove_file(&candidate)
                .with_context(|| format!("failed to remove rotated archive '{}'", candidate))?;
            idx += 1;
        }
        Ok(())
    }
}

fn now_unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn bool_as_u64(value: bool) -> u64 {
    if value {
        1
    } else {
        0
    }
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

fn sanitize_ascii_line(value: &str, max_bytes: usize) -> String {
    let mut out = String::with_capacity(value.len().min(max_bytes));
    for ch in value.chars() {
        if ch.is_ascii() {
            if ch.is_ascii_control() && ch != '\n' && ch != '\t' {
                continue;
            }
            out.push(ch);
        } else {
            out.push('?');
        }
        if out.len() >= max_bytes {
            break;
        }
    }
    out
}

fn normalize_severity(value: &str) -> String {
    let upper = value.trim().to_ascii_uppercase();
    if upper.is_empty() {
        return "INFO".to_string();
    }
    match upper.as_str() {
        "TRACE" | "DEBUG" | "INFO" | "WARN" | "ERROR" | "FATAL" => upper,
        "WARNING" => "WARN".to_string(),
        "CRITICAL" => "FATAL".to_string(),
        _ => "INFO".to_string(),
    }
}

fn record_u64(instrument: &Gauge<u64>, filter: &PrefixFilter, name: &str, value: u64) {
    if filter.allows(name) {
        instrument.record(value, &[] as &[KeyValue]);
    }
}

fn record_snapshot(instruments: &Instruments, filter: &PrefixFilter, snapshot: &RuntimeSnapshot) {
    record_u64(
        &instruments.source_available,
        filter,
        "system.syslog.source.available",
        bool_as_u64(snapshot.available),
    );
    record_u64(
        &instruments.up,
        filter,
        "system.syslog.up",
        bool_as_u64(snapshot.available),
    );
    record_u64(
        &instruments.journald_available,
        filter,
        "system.syslog.journald.available",
        bool_as_u64(snapshot.journald_available),
    );
    record_u64(
        &instruments.etw_available,
        filter,
        "system.syslog.etw.available",
        bool_as_u64(snapshot.etw_available),
    );
    record_u64(
        &instruments.dmesg_available,
        filter,
        "system.syslog.kernel.dmesg.available",
        bool_as_u64(snapshot.dmesg_available),
    );
    record_u64(
        &instruments.process_logs_available,
        filter,
        "system.syslog.process.logs.available",
        bool_as_u64(snapshot.process_logs_available),
    );
    record_u64(
        &instruments.application_logs_available,
        filter,
        "system.syslog.application.logs.available",
        bool_as_u64(snapshot.application_logs_available),
    );
    record_u64(
        &instruments.file_watch_targets_configured,
        filter,
        "system.syslog.file.watch.targets.configured",
        snapshot.file_watch_targets_configured,
    );
    record_u64(
        &instruments.file_watch_targets_active,
        filter,
        "system.syslog.file.watch.targets.active",
        snapshot.file_watch_targets_active,
    );
    record_u64(
        &instruments.buffer_capacity_records,
        filter,
        "system.syslog.buffer.capacity.records",
        snapshot.buffer_capacity_records,
    );
    record_u64(
        &instruments.buffer_queued_records,
        filter,
        "system.syslog.buffer.queued.records",
        snapshot.buffer_queued_records,
    );
    record_u64(
        &instruments.exporter_available,
        filter,
        "system.syslog.exporter.available",
        bool_as_u64(snapshot.exporter_available),
    );
    record_u64(
        &instruments.exporter_reconnecting,
        filter,
        "system.syslog.exporter.reconnecting",
        bool_as_u64(snapshot.exporter_reconnecting),
    );
    record_u64(
        &instruments.last_batch_size,
        filter,
        "system.syslog.logs.batch.size",
        snapshot.last_batch_size,
    );
    record_u64(
        &instruments.last_payload_bytes,
        filter,
        "system.syslog.logs.payload.bytes",
        snapshot.last_payload_bytes,
    );
    record_u64(
        &instruments.collection_errors,
        filter,
        "system.syslog.collection.errors",
        snapshot.collection_errors,
    );
}

fn default_logs_endpoint(metrics_endpoint: &str) -> String {
    if metrics_endpoint.ends_with("/v1/metrics") {
        metrics_endpoint.replace("/v1/metrics", "/v1/logs")
    } else {
        format!("{}/v1/logs", metrics_endpoint.trim_end_matches('/'))
    }
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
            .or_else(|| env::var("OJO_SYSLOG_CONFIG").ok())
            .unwrap_or_else(|| {
                resolve_default_config_path("syslog.yaml", "services/syslog/syslog.yaml")
            });

        let file_cfg = load_yaml_config_file(&config_path)?;
        let service = file_cfg.service.unwrap_or_default();
        let collection = file_cfg.collection.unwrap_or_default();
        let watch = file_cfg.watch.unwrap_or_default();
        let export = file_cfg.export.unwrap_or_default();
        let otlp = export.otlp.unwrap_or_default();
        let logs = export.logs.unwrap_or_default();
        let batch = export.batch.unwrap_or_default();
        let pipeline = file_cfg.pipeline.unwrap_or_default();
        let storage = file_cfg.storage.unwrap_or_default();
        let metrics = file_cfg.metrics.unwrap_or_default();

        let otlp_endpoint = otlp
            .endpoint
            .or_else(|| env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok())
            .unwrap_or_else(|| "http://127.0.0.1:4318/v1/metrics".to_string());

        let otlp_protocol = otlp
            .protocol
            .or_else(|| env::var("OTEL_EXPORTER_OTLP_PROTOCOL").ok())
            .unwrap_or_else(|| default_protocol_for_endpoint(Some(&otlp_endpoint)));

        let logs_endpoint = logs
            .endpoint
            .or_else(|| env::var("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT").ok())
            .unwrap_or_else(|| default_logs_endpoint(&otlp_endpoint));

        Ok(Self {
            service_name: service.name.unwrap_or_else(|| "ojo-syslog".to_string()),
            instance_id: service
                .instance_id
                .unwrap_or_else(host_collectors::hostname),
            poll_interval: Duration::from_secs(collection.poll_interval_secs.unwrap_or(5).max(1)),
            otlp_endpoint,
            otlp_protocol,
            otlp_timeout: otlp.timeout_secs.map(Duration::from_secs),
            export_interval: batch.interval_secs.map(Duration::from_secs),
            export_timeout: batch.timeout_secs.map(Duration::from_secs),
            logs_endpoint,
            logs_timeout: Duration::from_secs(logs.timeout_secs.unwrap_or(10).max(1)),
            metrics_include: metrics
                .include
                .unwrap_or_else(|| vec!["system.syslog.".to_string()]),
            metrics_exclude: metrics.exclude.unwrap_or_default(),
            max_lines_per_source: collection.max_lines_per_source.unwrap_or(200).max(1) as usize,
            max_message_bytes: collection.max_message_bytes.unwrap_or(4096).max(128) as usize,
            watch_files: watch.files.unwrap_or_default(),
            buffer_capacity_records: pipeline.buffer_capacity_records.unwrap_or(10_000).max(256),
            export_batch_size: pipeline.export_batch_size.unwrap_or(250).max(1),
            retry_backoff: Duration::from_secs(pipeline.retry_backoff_secs.unwrap_or(3).max(1)),
            archive_enabled: storage.archive_enabled.unwrap_or(true),
            archive_dir: storage
                .archive_dir
                .unwrap_or_else(|| "services/syslog/data".to_string()),
            archive_max_file_bytes: storage.archive_max_file_bytes.unwrap_or(64 * 1024 * 1024),
            archive_retain_files: storage.archive_retain_files.unwrap_or(8),
            archive_format: ArchiveFormat::parse(storage.archive_format.as_deref()),
            archive_mode: ArchiveMode::parse(storage.archive_mode.as_deref()),
            archive_window_secs: storage.archive_window_secs.unwrap_or(60),
            archive_compression: ArchiveCompression::parse(storage.archive_compression.as_deref()),
            once,
        })
    }
}

fn advance_export_state(current: ExportState, export_succeeded: bool) -> (ExportState, FlushEvent) {
    if export_succeeded {
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

fn handle_flush_event(event: FlushEvent, export_error: Option<&dyn std::fmt::Display>) {
    if let Some(err) = export_error {
        match event {
            FlushEvent::Reconnecting => {
                warn!(error = %err, "Syslog exporter disconnected, reconnecting")
            }
            FlushEvent::StillUnavailable => {
                warn!(
                    error = %err,
                    "Syslog exporter disconnected; still unavailable"
                )
            }
            FlushEvent::None | FlushEvent::Connected | FlushEvent::Reconnected => {}
        }
    } else {
        match event {
            FlushEvent::Connected => info!("Syslog exporter connected successfully"),
            FlushEvent::Reconnected => info!("Syslog exporter reconnected successfully"),
            FlushEvent::None | FlushEvent::Reconnecting | FlushEvent::StillUnavailable => {}
        }
    }
}

fn record_buffer_drop_if_any(instruments: &Instruments, dropped_in_push: u64) {
    if dropped_in_push > 0 {
        instruments.buffer_dropped_total.add(dropped_in_push, &[]);
    }
}

fn next_export_state(
    current: ExportState,
    export_attempted: bool,
    export_succeeded: bool,
) -> (ExportState, FlushEvent) {
    if export_attempted {
        advance_export_state(current, export_succeeded)
    } else {
        (current, FlushEvent::None)
    }
}

fn build_otlp_logs_payload(service_name: &str, instance_id: &str, batch: &[LogRecord]) -> Value {
    let log_records = batch
        .iter()
        .map(|record| {
            json!({
                "timeUnixNano": record.observed_time_unix_nano.to_string(),
                "observedTimeUnixNano": record.observed_time_unix_nano.to_string(),
                "severityText": record.severity_text.clone(),
                "body": { "stringValue": record.body.clone() },
                "attributes": [
                    { "key": "log.source", "value": { "stringValue": record.source.clone() } },
                    { "key": "log.stream", "value": { "stringValue": record.stream.clone() } },
                    { "key": "log.watch_target", "value": { "stringValue": record.watch_target.clone() } }
                ]
            })
        })
        .collect::<Vec<_>>();

    json!({
        "resourceLogs": [
            {
                "resource": {
                    "attributes": [
                        { "key": "service.name", "value": { "stringValue": service_name } },
                        { "key": "service.instance.id", "value": { "stringValue": instance_id } }
                    ]
                },
                "scopeLogs": [
                    {
                        "scope": {
                            "name": "ojo-syslog"
                        },
                        "logRecords": log_records
                    }
                ]
            }
        ]
    })
}

fn export_buffered_logs(
    exporter: &OtlpLogExporter,
    buffer: &mut LogBuffer,
    export_batch_size: usize,
) -> (ExportTelemetry, Option<anyhow::Error>) {
    if buffer.len() == 0 {
        return (ExportTelemetry::none(), None);
    }

    let mut stats = ExportTelemetry::none();
    loop {
        if buffer.len() == 0 {
            return (stats, None);
        }

        let batch = buffer.pop_batch(export_batch_size);
        let batch_len = batch.len() as u64;
        match exporter.export_batch(&batch) {
            Ok(payload_bytes) => {
                stats.exported_records = stats.exported_records.saturating_add(batch_len);
                stats.last_batch_size = batch_len;
                stats.last_payload_bytes = payload_bytes;
            }
            Err(err) => {
                stats.retries = stats.retries.saturating_add(1);
                stats.errors = stats.errors.saturating_add(1);
                buffer.push_front_batch(batch);
                return (stats, Some(err));
            }
        }
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
        let platform_cfg = platform::PlatformConfig {
            max_lines_per_source: cfg.max_lines_per_source,
            max_message_bytes: cfg.max_message_bytes,
            watch_files: cfg.watch_files.clone(),
        };
        let snap = platform::collect(&platform_cfg);
        let snapshot_json = serde_json::to_string_pretty(&snap.snapshot)
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

    let meter = opentelemetry::global::meter("ojo-syslog");
    let instruments = Instruments::new(&meter);
    let metric_filter = PrefixFilter::new(cfg.metrics_include.clone(), cfg.metrics_exclude.clone());
    let exporter = OtlpLogExporter::new(&cfg)?;
    let platform_cfg = platform::PlatformConfig {
        max_lines_per_source: cfg.max_lines_per_source,
        max_message_bytes: cfg.max_message_bytes,
        watch_files: cfg.watch_files.clone(),
    };

    let running = Arc::new(AtomicBool::new(true));
    if !cfg.once {
        if let Err(err) = ctrlc::set_handler(make_stop_handler(Arc::clone(&running))) {
            warn!(error = %err, "failed to install signal handler");
        }
    }

    let mut export_state = ExportState::Pending;
    let mut archive = ArchivePipeline::from_config(&cfg);
    let mut buffer = LogBuffer::new(cfg.buffer_capacity_records);
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
        let collected = platform::collect(&platform_cfg);

        let dropped_in_push = buffer.push_many(collected.records.clone());
        record_buffer_drop_if_any(&instruments, dropped_in_push);
        if !collected.records.is_empty() {
            instruments
                .logs_collected_total
                .add(collected.records.len() as u64, &[]);
            archive.write_batch(&collected.records);
        }

        let (export_stats, export_error) =
            export_buffered_logs(&exporter, &mut buffer, cfg.export_batch_size);
        if export_stats.exported_records > 0 {
            instruments
                .logs_exported_total
                .add(export_stats.exported_records, &[]);
        }
        if export_stats.retries > 0 {
            instruments.logs_retry_total.add(export_stats.retries, &[]);
        }
        if export_stats.errors > 0 {
            instruments
                .logs_export_errors_total
                .add(export_stats.errors, &[]);
        }

        let export_attempted = export_stats.exported_records > 0 || export_error.is_some();
        let export_succeeded = export_error.is_none();
        let (next_state, event) =
            next_export_state(export_state, export_attempted, export_succeeded);
        handle_flush_event(
            event,
            export_error
                .as_ref()
                .map(|err| err as &dyn std::fmt::Display),
        );
        export_state = next_state;

        let runtime = RuntimeSnapshot {
            available: collected.snapshot.available,
            journald_available: collected.snapshot.journald_available,
            etw_available: collected.snapshot.etw_available,
            dmesg_available: collected.snapshot.dmesg_available,
            process_logs_available: collected.snapshot.process_logs_available,
            application_logs_available: collected.snapshot.application_logs_available,
            file_watch_targets_configured: cfg.watch_files.len() as u64,
            file_watch_targets_active: collected.snapshot.file_watch_targets_active,
            buffer_capacity_records: buffer.capacity() as u64,
            buffer_queued_records: buffer.len() as u64,
            exporter_available: matches!(export_state, ExportState::Connected),
            exporter_reconnecting: matches!(export_state, ExportState::Reconnecting),
            last_batch_size: export_stats.last_batch_size,
            last_payload_bytes: export_stats.last_payload_bytes,
            collection_errors: collected.snapshot.collection_errors,
        };

        record_snapshot(&instruments, &metric_filter, &runtime);
        let _ = metric_provider.force_flush();

        #[cfg(not(coverage))]
        debug!(
            queued = runtime.buffer_queued_records,
            exported = export_stats.exported_records,
            payload_bytes = export_stats.last_payload_bytes,
            elapsed_ms = started_at.elapsed().as_millis(),
            "syslog collection loop complete"
        );
        #[cfg(coverage)]
        let _ = started_at.elapsed().as_millis();

        if cfg.once {
            break;
        }

        if export_error.is_some() {
            let deadline = started_at + cfg.retry_backoff;
            while running.load(Ordering::SeqCst) && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(100));
            }
            #[cfg(test)]
            if iterations >= max_iterations {
                break;
            }
            continue;
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
    Ok(())
}

fn main() -> Result<()> {
    run()
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileConfig {
    service: Option<ServiceSection>,
    collection: Option<CollectionSection>,
    watch: Option<WatchSection>,
    pipeline: Option<PipelineSection>,
    storage: Option<StorageSection>,
    export: Option<ExportSection>,
    metrics: Option<MetricSection>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ServiceSection {
    name: Option<String>,
    instance_id: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct CollectionSection {
    poll_interval_secs: Option<u64>,
    max_lines_per_source: Option<u64>,
    max_message_bytes: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct WatchSection {
    files: Option<Vec<WatchedFileConfig>>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct PipelineSection {
    buffer_capacity_records: Option<usize>,
    export_batch_size: Option<usize>,
    retry_backoff_secs: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct StorageSection {
    archive_enabled: Option<bool>,
    archive_dir: Option<String>,
    archive_max_file_bytes: Option<u64>,
    archive_retain_files: Option<usize>,
    archive_format: Option<String>,
    archive_mode: Option<String>,
    archive_window_secs: Option<u64>,
    archive_compression: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ExportSection {
    otlp: Option<OtlpSection>,
    logs: Option<LogsSection>,
    batch: Option<BatchSection>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct OtlpSection {
    endpoint: Option<String>,
    protocol: Option<String>,
    timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct LogsSection {
    endpoint: Option<String>,
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

#[cfg(test)]
#[path = "tests/main_tests.rs"]
mod tests;
