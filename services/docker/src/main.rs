use anyhow::{anyhow, Context, Result};
use host_collectors::{
    default_protocol_for_endpoint, init_meter_provider, OtlpSettings, PrefixFilter,
    METRIC_PREFIX_SYSTEM,
};
use opentelemetry::metrics::{Gauge, Meter};
use opentelemetry::KeyValue;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::env;
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
    include_labels: bool,
    max_labeled_containers: usize,
    otlp_endpoint: String,
    otlp_protocol: String,
    otlp_headers: BTreeMap<String, String>,
    otlp_compression: Option<String>,
    otlp_timeout: Option<Duration>,
    export_interval: Option<Duration>,
    export_timeout: Option<Duration>,
    metrics_include: Vec<String>,
    metrics_exclude: Vec<String>,
    once: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileConfig {
    service: Option<ServiceSection>,
    collection: Option<CollectionSection>,
    export: Option<ExportSection>,
    metrics: Option<MetricSection>,
    docker: Option<DockerSection>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ServiceSection {
    name: Option<String>,
    instance_id: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct CollectionSection {
    poll_interval_secs: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct DockerSection {
    include_container_labels: Option<bool>,
    max_labeled_containers: Option<usize>,
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
    headers: Option<BTreeMap<String, String>>,
    compression: Option<String>,
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

#[derive(Clone, Debug, Default)]
pub(crate) struct DockerSample {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) image: String,
    pub(crate) state: String,
    pub(crate) cpu_ratio: f64,
    pub(crate) mem_usage_bytes: f64,
    pub(crate) mem_limit_bytes: f64,
    pub(crate) net_rx_bytes: f64,
    pub(crate) net_tx_bytes: f64,
    pub(crate) block_read_bytes: f64,
    pub(crate) block_write_bytes: f64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DockerSnapshot {
    pub(crate) available: bool,
    pub(crate) total: u64,
    pub(crate) running: u64,
    pub(crate) stopped: u64,
    pub(crate) samples: Vec<DockerSample>,
}

struct Instruments {
    total: Gauge<u64>,
    running: Gauge<u64>,
    stopped: Gauge<u64>,
    cpu_ratio: Gauge<f64>,
    mem_usage_bytes: Gauge<f64>,
    mem_limit_bytes: Gauge<f64>,
    net_rx_bytes: Gauge<f64>,
    net_tx_bytes: Gauge<f64>,
    block_read_bytes: Gauge<f64>,
    block_write_bytes: Gauge<f64>,
    source_available: Gauge<u64>,
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

impl Instruments {
    fn new(meter: &Meter) -> Self {
        Self {
            total: meter.u64_gauge("system.docker.containers.total").build(),
            running: meter.u64_gauge("system.docker.containers.running").build(),
            stopped: meter.u64_gauge("system.docker.containers.stopped").build(),
            cpu_ratio: meter
                .f64_gauge("system.docker.container.cpu.ratio")
                .with_unit("1")
                .build(),
            mem_usage_bytes: meter
                .f64_gauge("system.docker.container.memory.usage.bytes")
                .with_unit("By")
                .build(),
            mem_limit_bytes: meter
                .f64_gauge("system.docker.container.memory.limit.bytes")
                .with_unit("By")
                .build(),
            net_rx_bytes: meter
                .f64_gauge("system.docker.container.network.rx.bytes")
                .with_unit("By")
                .build(),
            net_tx_bytes: meter
                .f64_gauge("system.docker.container.network.tx.bytes")
                .with_unit("By")
                .build(),
            block_read_bytes: meter
                .f64_gauge("system.docker.container.block.read.bytes")
                .with_unit("By")
                .build(),
            block_write_bytes: meter
                .f64_gauge("system.docker.container.block.write.bytes")
                .with_unit("By")
                .build(),
            source_available: meter.u64_gauge("system.docker.source.available").build(),
        }
    }
}

fn make_stop_handler(signal: Arc<AtomicBool>) -> impl Fn() + Send + 'static {
    move || {
        signal.store(false, Ordering::SeqCst);
    }
}

fn install_signal_handler(running: &Arc<AtomicBool>) {
    if let Err(err) = ctrlc::set_handler(make_stop_handler(Arc::clone(running))) {
        warn!(error = %err, "failed to install signal handler");
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
            FlushEvent::Reconnecting => {
                warn!(error = %err, "Exporter flush failed; reconnecting")
            }
            FlushEvent::StillUnavailable => {
                warn!(error = %err, "Exporter still unavailable")
            }
            FlushEvent::None | FlushEvent::Connected | FlushEvent::Reconnected => {}
        }
    } else {
        match event {
            FlushEvent::Connected => info!("Connected Successfully"),
            FlushEvent::Reconnected => info!("Reconnected Successfully"),
            FlushEvent::None | FlushEvent::Reconnecting | FlushEvent::StillUnavailable => {}
        }
    }
}

fn sleep_until(deadline: Instant, running: &AtomicBool, sleep_interval: Duration) {
    while Instant::now() < deadline && running.load(Ordering::SeqCst) {
        thread::sleep(sleep_interval);
    }
}

fn maybe_sleep_until_next_poll(
    once: bool,
    started_at: Instant,
    poll_interval: Duration,
    running: &AtomicBool,
) {
    if once {
        return;
    }
    let deadline = started_at + poll_interval;
    sleep_until(deadline, running, Duration::from_millis(500));
}

fn run() -> Result<()> {
    let cfg = Config::load()?;
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init()
        .ok();

    let provider = init_meter_provider(&OtlpSettings {
        service_name: cfg.service_name.clone(),
        instance_id: cfg.instance_id.clone(),
        otlp_endpoint: cfg.otlp_endpoint.clone(),
        otlp_protocol: cfg.otlp_protocol.clone(),
        otlp_headers: cfg.otlp_headers.clone(),
        otlp_compression: cfg.otlp_compression.clone(),
        otlp_timeout: cfg.otlp_timeout,
        export_interval: cfg.export_interval,
        export_timeout: cfg.export_timeout,
    })?;
    let meter = opentelemetry::global::meter("ojo-docker");
    let instruments = Instruments::new(&meter);
    let filter = PrefixFilter::new(cfg.metrics_include.clone(), cfg.metrics_exclude.clone());

    let running = Arc::new(AtomicBool::new(true));
    install_signal_handler(&running);

    let mut export_state = ExportState::Pending;
    let mut continue_running = true;
    while continue_running && running.load(Ordering::SeqCst) {
        let started_at = Instant::now();
        let snapshot = platform::collect_snapshot();
        record_snapshot(&instruments, &filter, &cfg, &snapshot);
        let flush_result = provider.force_flush();
        log_flush_result(started_at, flush_result.is_ok());

        let (next_state, event) = advance_export_state(export_state, flush_result.is_ok());
        handle_flush_event(
            event,
            flush_result
                .as_ref()
                .err()
                .map(|err| err as &dyn std::fmt::Display),
        );
        export_state = next_state;
        maybe_sleep_until_next_poll(cfg.once, started_at, cfg.poll_interval, &running);
        continue_running = !cfg.once;
    }

    let _ = provider.shutdown();
    Ok(())
}

#[cfg(not(test))]
fn main() -> Result<()> {
    run()
}

fn record_snapshot(
    instruments: &Instruments,
    filter: &PrefixFilter,
    cfg: &Config,
    snap: &DockerSnapshot,
) {
    record_u64(
        &instruments.source_available,
        filter,
        "system.docker.source.available",
        if snap.available { 1 } else { 0 },
        &[],
    );
    if !snap.available {
        return;
    }

    record_u64(
        &instruments.total,
        filter,
        "system.docker.containers.total",
        snap.total,
        &[],
    );
    record_u64(
        &instruments.running,
        filter,
        "system.docker.containers.running",
        snap.running,
        &[],
    );
    record_u64(
        &instruments.stopped,
        filter,
        "system.docker.containers.stopped",
        snap.stopped,
        &[],
    );

    if snap.samples.is_empty() {
        return;
    }

    let count = snap.samples.len() as f64;
    let totals = snap
        .samples
        .iter()
        .fold(DockerSample::default(), |mut acc, sample| {
            acc.cpu_ratio += sample.cpu_ratio;
            acc.mem_usage_bytes += sample.mem_usage_bytes;
            acc.mem_limit_bytes += sample.mem_limit_bytes;
            acc.net_rx_bytes += sample.net_rx_bytes;
            acc.net_tx_bytes += sample.net_tx_bytes;
            acc.block_read_bytes += sample.block_read_bytes;
            acc.block_write_bytes += sample.block_write_bytes;
            acc
        });

    record_f64(
        &instruments.cpu_ratio,
        filter,
        "system.docker.container.cpu.ratio",
        totals.cpu_ratio / count,
        &[],
    );
    record_f64(
        &instruments.mem_usage_bytes,
        filter,
        "system.docker.container.memory.usage.bytes",
        totals.mem_usage_bytes,
        &[],
    );
    record_f64(
        &instruments.mem_limit_bytes,
        filter,
        "system.docker.container.memory.limit.bytes",
        totals.mem_limit_bytes,
        &[],
    );
    record_f64(
        &instruments.net_rx_bytes,
        filter,
        "system.docker.container.network.rx.bytes",
        totals.net_rx_bytes,
        &[],
    );
    record_f64(
        &instruments.net_tx_bytes,
        filter,
        "system.docker.container.network.tx.bytes",
        totals.net_tx_bytes,
        &[],
    );
    record_f64(
        &instruments.block_read_bytes,
        filter,
        "system.docker.container.block.read.bytes",
        totals.block_read_bytes,
        &[],
    );
    record_f64(
        &instruments.block_write_bytes,
        filter,
        "system.docker.container.block.write.bytes",
        totals.block_write_bytes,
        &[],
    );

    if !cfg.include_labels {
        return;
    }

    for sample in cap_samples_for_labels(&snap.samples, cfg.max_labeled_containers) {
        let attrs = [
            KeyValue::new("container.id", sample.id.clone()),
            KeyValue::new("container.name", container_name_label(&sample)),
            KeyValue::new(
                "container.image",
                non_empty_or(&sample.image, "unknown-image"),
            ),
            KeyValue::new("container.state", non_empty_or(&sample.state, "unknown")),
        ];
        record_f64(
            &instruments.cpu_ratio,
            filter,
            "system.docker.container.cpu.ratio",
            sample.cpu_ratio,
            &attrs,
        );
        record_f64(
            &instruments.mem_usage_bytes,
            filter,
            "system.docker.container.memory.usage.bytes",
            sample.mem_usage_bytes,
            &attrs,
        );
        record_f64(
            &instruments.mem_limit_bytes,
            filter,
            "system.docker.container.memory.limit.bytes",
            sample.mem_limit_bytes,
            &attrs,
        );
        record_f64(
            &instruments.net_rx_bytes,
            filter,
            "system.docker.container.network.rx.bytes",
            sample.net_rx_bytes,
            &attrs,
        );
        record_f64(
            &instruments.net_tx_bytes,
            filter,
            "system.docker.container.network.tx.bytes",
            sample.net_tx_bytes,
            &attrs,
        );
        record_f64(
            &instruments.block_read_bytes,
            filter,
            "system.docker.container.block.read.bytes",
            sample.block_read_bytes,
            &attrs,
        );
        record_f64(
            &instruments.block_write_bytes,
            filter,
            "system.docker.container.block.write.bytes",
            sample.block_write_bytes,
            &attrs,
        );
    }
}

fn container_name_label(sample: &DockerSample) -> String {
    let name = sample.name.trim();
    if !name.is_empty() {
        return name.to_string();
    }
    let id = sample.id.trim();
    if id.is_empty() {
        return "unknown-container".to_string();
    }
    id.chars().take(12).collect::<String>()
}

fn non_empty_or(value: &str, fallback: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return fallback.to_string();
    }
    trimmed.to_string()
}

fn cap_samples_for_labels(samples: &[DockerSample], limit: usize) -> Vec<DockerSample> {
    let mut ordered = samples.to_vec();
    ordered.sort_by(|a, b| a.name.cmp(&b.name));
    ordered.into_iter().take(limit).collect()
}

pub(crate) fn parse_percent(text: &str) -> f64 {
    text.trim_end_matches('%')
        .trim()
        .parse::<f64>()
        .unwrap_or(0.0)
        / 100.0
}

pub(crate) fn parse_pair_bytes(text: &str) -> (f64, f64) {
    let mut parts = text.split('/').map(str::trim);
    let first = parse_size_to_bytes(parts.next().unwrap_or_default());
    let second = parse_size_to_bytes(parts.next().unwrap_or_default());
    (first, second)
}

pub(crate) fn parse_size_to_bytes(text: &str) -> f64 {
    if text.is_empty() || text.eq_ignore_ascii_case("0B") || text.eq_ignore_ascii_case("--") {
        return 0.0;
    }
    let cleaned = text.replace(' ', "");
    let mut idx = 0usize;
    for (i, ch) in cleaned.char_indices() {
        if ch.is_ascii_digit() || ch == '.' {
            idx = i + ch.len_utf8();
        } else {
            break;
        }
    }
    let (value_part, unit_part) = cleaned.split_at(idx);
    let value = value_part.parse::<f64>().unwrap_or(0.0);
    let unit = unit_part.to_ascii_lowercase();
    let multiplier = match unit.as_str() {
        "b" | "" => 1.0,
        "kb" => 1000.0,
        "kib" => 1024.0,
        "mb" => 1000.0 * 1000.0,
        "mib" => 1024.0 * 1024.0,
        "gb" => 1000.0 * 1000.0 * 1000.0,
        "gib" => 1024.0 * 1024.0 * 1024.0,
        "tb" => 1000.0 * 1000.0 * 1000.0 * 1000.0,
        "tib" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => 1.0,
    };
    value * multiplier
}

impl Config {
    fn load() -> Result<Self> {
        let args = env::args().collect::<Vec<_>>();
        Self::load_from_args(&args)
    }

    fn load_from_args(args: &[String]) -> Result<Self> {
        let once = args.iter().any(|arg| arg == "--once")
            || env::var("OJO_RUN_ONCE")
                .ok()
                .map(|v| {
                    matches!(
                        v.trim().to_ascii_lowercase().as_str(),
                        "1" | "true" | "yes" | "on"
                    )
                })
                .unwrap_or(false);
        let config_path = args
            .windows(2)
            .find(|pair| pair[0] == "--config")
            .map(|pair| pair[1].clone())
            .or_else(|| env::var("OJO_DOCKER_CONFIG").ok())
            .unwrap_or_else(|| {
                resolve_default_config_path("docker.yaml", "services/docker/docker.yaml")
            });

        let file_cfg = load_yaml_config_file(&config_path)?;
        let service = file_cfg.service.unwrap_or_default();
        let collection = file_cfg.collection.unwrap_or_default();
        let export = file_cfg.export.unwrap_or_default();
        let otlp = export.otlp.unwrap_or_default();
        let batch = export.batch.unwrap_or_default();
        let metrics = file_cfg.metrics.unwrap_or_default();
        let docker = file_cfg.docker.unwrap_or_default();

        let otlp_endpoint = otlp
            .endpoint
            .or_else(|| env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok())
            .unwrap_or_else(|| "http://127.0.0.1:4318/v1/metrics".to_string());
        let otlp_protocol = otlp
            .protocol
            .or_else(|| env::var("OTEL_EXPORTER_OTLP_PROTOCOL").ok())
            .unwrap_or_else(|| default_protocol_for_endpoint(Some(&otlp_endpoint)));

        Ok(Self {
            service_name: service.name.unwrap_or_else(|| "ojo-docker".to_string()),
            instance_id: service
                .instance_id
                .unwrap_or_else(host_collectors::hostname),
            poll_interval: Duration::from_secs(collection.poll_interval_secs.unwrap_or(10).max(1)),
            include_labels: docker.include_container_labels.unwrap_or(false),
            max_labeled_containers: docker.max_labeled_containers.unwrap_or(25),
            otlp_endpoint,
            otlp_protocol,
            otlp_headers: otlp.headers.unwrap_or_default(),
            otlp_compression: otlp.compression,
            otlp_timeout: otlp.timeout_secs.map(Duration::from_secs),
            export_interval: batch.interval_secs.map(Duration::from_secs),
            export_timeout: batch.timeout_secs.map(Duration::from_secs),
            metrics_include: metrics
                .include
                .unwrap_or_else(|| vec![METRIC_PREFIX_SYSTEM.to_string()]),
            metrics_exclude: metrics.exclude.unwrap_or_default(),
            once,
        })
    }
}

fn resolve_default_config_path(local_name: &str, repo_relative: &str) -> String {
    if Path::new(local_name).exists() {
        return local_name.to_string();
    }
    repo_relative.to_string()
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

fn record_u64(
    instrument: &Gauge<u64>,
    filter: &PrefixFilter,
    name: &str,
    value: u64,
    attrs: &[KeyValue],
) {
    if filter.allows(name) {
        instrument.record(value, attrs);
    }
}

fn record_f64(
    instrument: &Gauge<f64>,
    filter: &PrefixFilter,
    name: &str,
    value: f64,
    attrs: &[KeyValue],
) {
    if filter.allows(name) {
        instrument.record(value, attrs);
    }
}

#[cfg(test)]
#[path = "tests/main_tests.rs"]
mod tests;
