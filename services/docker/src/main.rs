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

#[cfg(test)]
const SEMANTIC_GAUGE: &str = "gauge";
#[cfg(test)]
const SEMANTIC_GAUGE_RATIO: &str = "gauge_ratio";

#[cfg(test)]
#[derive(Clone, Debug)]
struct MetricDef {
    name: &'static str,
    semantic: &'static str,
}

#[cfg(test)]
const METRICS: &[MetricDef] = &[
    MetricDef {
        name: "system.docker.containers.total",
        semantic: SEMANTIC_GAUGE,
    },
    MetricDef {
        name: "system.docker.containers.running",
        semantic: SEMANTIC_GAUGE,
    },
    MetricDef {
        name: "system.docker.containers.stopped",
        semantic: SEMANTIC_GAUGE,
    },
    MetricDef {
        name: "system.docker.container.cpu.ratio",
        semantic: SEMANTIC_GAUGE_RATIO,
    },
    MetricDef {
        name: "system.docker.container.memory.usage.bytes",
        semantic: SEMANTIC_GAUGE,
    },
    MetricDef {
        name: "system.docker.container.memory.limit.bytes",
        semantic: SEMANTIC_GAUGE,
    },
    MetricDef {
        name: "system.docker.container.network.rx.bytes",
        semantic: SEMANTIC_GAUGE,
    },
    MetricDef {
        name: "system.docker.container.network.tx.bytes",
        semantic: SEMANTIC_GAUGE,
    },
    MetricDef {
        name: "system.docker.container.block.read.bytes",
        semantic: SEMANTIC_GAUGE,
    },
    MetricDef {
        name: "system.docker.container.block.write.bytes",
        semantic: SEMANTIC_GAUGE,
    },
    MetricDef {
        name: "system.docker.source.available",
        semantic: SEMANTIC_GAUGE,
    },
];

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

fn main() -> Result<()> {
    let cfg = Config::load()?;
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

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
    let signal = Arc::clone(&running);
    ctrlc::set_handler(move || {
        signal.store(false, Ordering::SeqCst);
    })?;

    let mut export_state = ExportState::Pending;
    while running.load(Ordering::SeqCst) {
        let started_at = Instant::now();
        let snapshot = platform::collect_snapshot();
        record_snapshot(&instruments, &filter, &cfg, &snapshot);
        match provider.force_flush() {
            Ok(()) => {
                debug!(
                    elapsed_ms = started_at.elapsed().as_millis(),
                    "force_flush ok"
                );
                match export_state {
                    ExportState::Pending => info!("Connected Successfully"),
                    ExportState::Reconnecting => info!("Reconnected Successfully"),
                    ExportState::Connected => {}
                }
                export_state = ExportState::Connected;
            }
            Err(err) => {
                debug!(
                    elapsed_ms = started_at.elapsed().as_millis(),
                    "force_flush err"
                );
                match export_state {
                    ExportState::Connected => {
                        warn!(error = %err, "Exporter flush failed; reconnecting")
                    }
                    ExportState::Pending | ExportState::Reconnecting => {
                        warn!(error = %err, "Exporter still unavailable")
                    }
                }
                export_state = ExportState::Reconnecting;
            }
        }
        if cfg.once {
            break;
        }
        let deadline = started_at + cfg.poll_interval;
        while Instant::now() < deadline && running.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(500));
        }
    }

    let _ = provider.shutdown();
    Ok(())
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
            KeyValue::new("container.image", non_empty_or(&sample.image, "unknown-image")),
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
        let once = args.iter().any(|arg| arg == "--once");
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
mod tests {
    use super::{
        cap_samples_for_labels, parse_pair_bytes, parse_percent, parse_size_to_bytes, DockerSample,
        METRICS,
    };

    #[test]
    fn metric_names_use_system_namespace() {
        for metric in METRICS {
            assert!(metric.name.starts_with("system."));
            assert!(metric.semantic == "gauge" || metric.semantic == "gauge_ratio");
        }
    }

    #[test]
    fn caps_container_labels_to_budget() {
        let samples = vec![
            DockerSample {
                name: "z".to_string(),
                ..DockerSample::default()
            },
            DockerSample {
                name: "a".to_string(),
                ..DockerSample::default()
            },
            DockerSample {
                name: "m".to_string(),
                ..DockerSample::default()
            },
        ];
        let capped = cap_samples_for_labels(&samples, 2);
        assert_eq!(capped.len(), 2);
        assert_eq!(capped[0].name, "a");
        assert_eq!(capped[1].name, "m");
    }

    #[test]
    fn parses_docker_units() {
        assert_eq!(parse_percent("50%"), 0.5);
        let (a, b) = parse_pair_bytes("1.5MiB / 2GiB");
        assert!(a > 1_000_000.0);
        assert!(b > a);
        assert!(parse_size_to_bytes("12kB") > 10_000.0);
    }
}
