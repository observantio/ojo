use anyhow::{anyhow, Context, Result};
use host_collectors::{
    default_protocol_for_endpoint, init_meter_provider, OtlpSettings, PrefixFilter,
};
use opentelemetry::metrics::Gauge;
use opentelemetry::KeyValue;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::env;
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

#[cfg(test)]
const METRICS: &[(&str, &str)] = &[
    ("system.sensor.temperature.celsius", "gauge"),
    ("system.sensor.temperature.max.celsius", "gauge"),
    ("system.sensor.fan.rpm", "gauge"),
    ("system.sensor.voltage.volts", "gauge"),
    ("system.sensor.count", "inventory"),
    ("system.sensor.source.available", "state"),
];

#[derive(Clone, Debug)]
struct Config {
    service_name: String,
    instance_id: String,
    poll_interval: Duration,
    include_sensor_labels: bool,
    max_labeled_sensors: usize,
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
    sensors: Option<SensorSection>,
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
struct SensorSection {
    include_sensor_labels: Option<bool>,
    max_labeled_sensors: Option<usize>,
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
pub(crate) struct SensorSample {
    pub(crate) chip: String,
    pub(crate) kind: String,
    pub(crate) label: String,
    pub(crate) value: f64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SensorSnapshot {
    pub(crate) available: bool,
    pub(crate) temperatures: Vec<SensorSample>,
    pub(crate) fans: Vec<SensorSample>,
    pub(crate) voltages: Vec<SensorSample>,
}

struct Instruments {
    temp_avg: Gauge<f64>,
    temp_max: Gauge<f64>,
    fan_rpm: Gauge<f64>,
    voltage: Gauge<f64>,
    sensor_count: Gauge<u64>,
    source_available: Gauge<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExportState {
    Pending,
    Connected,
    Reconnecting,
}

impl Instruments {
    fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self {
            temp_avg: meter
                .f64_gauge("system.sensor.temperature.celsius")
                .with_unit("Cel")
                .build(),
            temp_max: meter
                .f64_gauge("system.sensor.temperature.max.celsius")
                .with_unit("Cel")
                .build(),
            fan_rpm: meter.f64_gauge("system.sensor.fan.rpm").build(),
            voltage: meter
                .f64_gauge("system.sensor.voltage.volts")
                .with_unit("V")
                .build(),
            sensor_count: meter.u64_gauge("system.sensor.count").build(),
            source_available: meter.u64_gauge("system.sensor.source.available").build(),
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
    let meter = opentelemetry::global::meter("ojo-sensors");
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
        let elapsed = started_at.elapsed();
        if elapsed < cfg.poll_interval && running.load(Ordering::SeqCst) {
            thread::sleep(cfg.poll_interval - elapsed);
        }
    }

    let _ = provider.shutdown();
    Ok(())
}

fn record_snapshot(
    instruments: &Instruments,
    filter: &PrefixFilter,
    cfg: &Config,
    snap: &SensorSnapshot,
) {
    record_u64(
        &instruments.source_available,
        filter,
        "system.sensor.source.available",
        if snap.available { 1 } else { 0 },
        &[],
    );
    if !snap.available {
        return;
    }

    let total_count = snap.temperatures.len() + snap.fans.len() + snap.voltages.len();
    record_u64(
        &instruments.sensor_count,
        filter,
        "system.sensor.count",
        total_count as u64,
        &[],
    );

    if !snap.temperatures.is_empty() {
        let sum = snap.temperatures.iter().map(|s| s.value).sum::<f64>();
        let max = snap
            .temperatures
            .iter()
            .map(|s| s.value)
            .fold(f64::MIN, f64::max);
        record_f64(
            &instruments.temp_avg,
            filter,
            "system.sensor.temperature.celsius",
            sum / snap.temperatures.len() as f64,
            &[],
        );
        record_f64(
            &instruments.temp_max,
            filter,
            "system.sensor.temperature.max.celsius",
            max,
            &[],
        );
    }

    if !snap.fans.is_empty() {
        let sum = snap.fans.iter().map(|s| s.value).sum::<f64>();
        record_f64(
            &instruments.fan_rpm,
            filter,
            "system.sensor.fan.rpm",
            sum / snap.fans.len() as f64,
            &[],
        );
    }
    if !snap.voltages.is_empty() {
        let sum = snap.voltages.iter().map(|s| s.value).sum::<f64>();
        record_f64(
            &instruments.voltage,
            filter,
            "system.sensor.voltage.volts",
            sum / snap.voltages.len() as f64,
            &[],
        );
    }

    if !cfg.include_sensor_labels {
        return;
    }
    for sample in cap_samples_for_labels(&snap.temperatures, cfg.max_labeled_sensors) {
        let attrs = [
            KeyValue::new("sensor.chip", sample.chip.clone()),
            KeyValue::new("sensor.kind", sample.kind.clone()),
            KeyValue::new("sensor.label", sample.label.clone()),
        ];
        record_f64(
            &instruments.temp_avg,
            filter,
            "system.sensor.temperature.celsius",
            sample.value,
            &attrs,
        );
    }
}

fn cap_samples_for_labels(samples: &[SensorSample], limit: usize) -> Vec<SensorSample> {
    let mut out = samples.to_vec();
    out.sort_by(|a, b| a.label.cmp(&b.label));
    out.into_iter().take(limit).collect()
}

impl Config {
    fn load() -> Result<Self> {
        let args = env::args().collect::<Vec<_>>();
        let once = args.iter().any(|arg| arg == "--once");
        let config_path = args
            .windows(2)
            .find(|pair| pair[0] == "--config")
            .map(|pair| pair[1].clone())
            .or_else(|| env::var("OJO_SENSORS_CONFIG").ok())
            .unwrap_or_else(|| {
                resolve_default_config_path("sensors.yaml", "services/sensors/sensors.yaml")
            });

        let file_cfg = load_yaml_config_file(&config_path)?;
        let service = file_cfg.service.unwrap_or_default();
        let collection = file_cfg.collection.unwrap_or_default();
        let export = file_cfg.export.unwrap_or_default();
        let otlp = export.otlp.unwrap_or_default();
        let batch = export.batch.unwrap_or_default();
        let metrics = file_cfg.metrics.unwrap_or_default();
        let sensors = file_cfg.sensors.unwrap_or_default();

        let otlp_endpoint = otlp
            .endpoint
            .or_else(|| env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok())
            .unwrap_or_else(|| "http://127.0.0.1:4318/v1/metrics".to_string());
        let otlp_protocol = otlp
            .protocol
            .or_else(|| env::var("OTEL_EXPORTER_OTLP_PROTOCOL").ok())
            .unwrap_or_else(|| default_protocol_for_endpoint(Some(&otlp_endpoint)));

        Ok(Self {
            service_name: service.name.unwrap_or_else(|| "ojo-sensors".to_string()),
            instance_id: service
                .instance_id
                .unwrap_or_else(host_collectors::hostname),
            poll_interval: Duration::from_secs(collection.poll_interval_secs.unwrap_or(15)),
            include_sensor_labels: sensors.include_sensor_labels.unwrap_or(false),
            max_labeled_sensors: sensors.max_labeled_sensors.unwrap_or(32),
            otlp_endpoint,
            otlp_protocol,
            otlp_headers: otlp.headers.unwrap_or_default(),
            otlp_compression: otlp.compression,
            otlp_timeout: otlp.timeout_secs.map(Duration::from_secs),
            export_interval: batch.interval_secs.map(Duration::from_secs),
            export_timeout: batch.timeout_secs.map(Duration::from_secs),
            metrics_include: metrics
                .include
                .unwrap_or_else(|| vec!["system.sensor.".to_string()]),
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
    let contents = fs::read_to_string(path)
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
    use super::{cap_samples_for_labels, SensorSample, METRICS};

    #[test]
    fn metric_contract_uses_supported_namespaces() {
        for (name, semantic) in METRICS {
            assert!(name.starts_with("system."));
            assert!(matches!(*semantic, "gauge" | "inventory" | "state"));
        }
    }

    #[test]
    fn caps_sensor_labels() {
        let samples = vec![
            SensorSample {
                label: "z".to_string(),
                ..SensorSample::default()
            },
            SensorSample {
                label: "a".to_string(),
                ..SensorSample::default()
            },
            SensorSample {
                label: "m".to_string(),
                ..SensorSample::default()
            },
        ];
        let capped = cap_samples_for_labels(&samples, 2);
        assert_eq!(capped.len(), 2);
        assert_eq!(capped[0].label, "a");
        assert_eq!(capped[1].label, "m");
    }
}
