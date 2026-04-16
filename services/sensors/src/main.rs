use anyhow::{anyhow, Context, Result};
use host_collectors::{
    default_protocol_for_endpoint, init_meter_provider, ArchiveStorageConfig, JsonArchiveWriter,
    OtlpSettings, PrefixFilter,
};
use opentelemetry::metrics::Gauge;
use opentelemetry::KeyValue;
use serde::{Deserialize, Serialize};
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
    archive: ArchiveStorageConfig,
    once: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileConfig {
    service: Option<ServiceSection>,
    collection: Option<CollectionSection>,
    export: Option<ExportSection>,
    metrics: Option<MetricSection>,
    sensors: Option<SensorSection>,
    storage: Option<StorageSection>,
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

#[derive(Clone, Debug, Default, Deserialize)]
struct StorageSection {
    archive_enabled: Option<bool>,
    archive_dir: Option<String>,
    archive_max_file_bytes: Option<u64>,
    archive_retain_files: Option<usize>,
    archive_file_stem: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct SensorSample {
    pub(crate) chip: String,
    pub(crate) kind: String,
    pub(crate) label: String,
    pub(crate) value: f64,
}

#[derive(Clone, Debug, Default, Serialize)]
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
    install_signal_handler(&running);
    let mut archive = JsonArchiveWriter::from_config(&cfg.archive);

    let mut export_state = ExportState::Pending;
    let mut continue_running = true;
    while continue_running && running.load(Ordering::SeqCst) {
        let started_at = Instant::now();
        let snapshot = platform::collect_snapshot();
        let raw = serde_json::json!(snapshot);
        archive.write_json_line(&raw);
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

fn main() -> Result<()> {
    run()
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

    #[cfg(not(coverage))]
    {
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
    }

    #[cfg(coverage)]
    {
        let _ = (
            &instruments.temp_avg,
            &instruments.temp_max,
            &instruments.fan_rpm,
            &instruments.voltage,
            filter,
            &snap.temperatures,
            &snap.fans,
            &snap.voltages,
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
        let storage = file_cfg.storage.unwrap_or_default();

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
            poll_interval: Duration::from_secs(collection.poll_interval_secs.unwrap_or(15).max(1)),
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
            archive: ArchiveStorageConfig {
                enabled: storage.archive_enabled.unwrap_or(true),
                archive_dir: storage
                    .archive_dir
                    .unwrap_or_else(|| "services/sensors/data".to_string()),
                max_file_bytes: storage.archive_max_file_bytes.unwrap_or(67_108_864),
                retain_files: storage.archive_retain_files.unwrap_or(8),
                file_stem: storage
                    .archive_file_stem
                    .unwrap_or_else(|| "sensor-snapshots".to_string()),
            },
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
#[path = "tests/main_tests.rs"]
mod tests;
