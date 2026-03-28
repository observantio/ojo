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
    let meter = opentelemetry::global::meter("ojo-sensors");
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
    use super::{
        advance_export_state, cap_samples_for_labels, handle_flush_event, install_signal_handler,
        load_yaml_config_file, log_flush_result, make_stop_handler, maybe_sleep_until_next_poll,
        record_f64, record_snapshot, record_u64, resolve_default_config_path, run, sleep_until,
        Config, ExportState, FlushEvent, Instruments, SensorSample, SensorSnapshot, METRICS,
    };
    use host_collectors::PrefixFilter;
    use std::fs;
    use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc, Mutex, OnceLock};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn unique_temp_path(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "ojo-sensors-main-{name}-{}-{nanos}",
            std::process::id()
        ))
    }

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

    #[test]
    fn resolve_and_load_yaml_config_file_cover_common_paths() {
        let local = unique_temp_path("local.yaml");
        fs::write(&local, "service: {}\n").expect("write local");
        let resolved =
            resolve_default_config_path(local.to_string_lossy().as_ref(), "fallback.yaml");
        assert_eq!(resolved, local.to_string_lossy());
        fs::remove_file(&local).expect("cleanup local");

        let missing = unique_temp_path("missing.yaml");
        let err = load_yaml_config_file(missing.to_string_lossy().as_ref()).unwrap_err();
        assert!(err.to_string().contains("was not found"), "{err}");

        let dir = unique_temp_path("dir");
        fs::create_dir_all(&dir).expect("mkdir");
        let err = load_yaml_config_file(dir.to_string_lossy().as_ref()).unwrap_err();
        assert!(
            err.to_string().contains("failed to read config file"),
            "{err}"
        );
        fs::remove_dir_all(&dir).expect("cleanup dir");

        let empty = unique_temp_path("empty.yaml");
        fs::write(&empty, "\n").expect("write empty");
        let err = load_yaml_config_file(empty.to_string_lossy().as_ref()).unwrap_err();
        assert!(err.to_string().contains("is empty"), "{err}");
        fs::remove_file(&empty).expect("cleanup empty");

        let invalid = unique_temp_path("invalid.yaml");
        fs::write(&invalid, "service: [\n").expect("write invalid");
        let err = load_yaml_config_file(invalid.to_string_lossy().as_ref()).unwrap_err();
        assert!(err.to_string().contains("failed to parse YAML"), "{err}");
        fs::remove_file(&invalid).expect("cleanup invalid");
    }

    #[test]
    fn config_load_and_record_snapshot_cover_main_paths() {
        let _guard = env_lock().lock().expect("env lock");
        let path = unique_temp_path("config.yaml");
        fs::write(
            &path,
            "service:\n  name: sensors-svc\n  instance_id: sensors-01\ncollection:\n  poll_interval_secs: 2\nsensors:\n  include_sensor_labels: true\n  max_labeled_sensors: 2\n",
        )
        .expect("write config");

        std::env::set_var("OJO_SENSORS_CONFIG", &path);
        std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4317");
        std::env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc");

        let cfg = Config::load().expect("load config");
        assert_eq!(cfg.service_name, "sensors-svc");
        assert_eq!(cfg.instance_id, "sensors-01");
        assert_eq!(cfg.otlp_protocol, "grpc");

        let meter = opentelemetry::global::meter("sensors-test-meter");
        let instruments = Instruments::new(&meter);
        let filter = PrefixFilter::new(vec!["system.sensor.".to_string()], vec![]);
        record_snapshot(&instruments, &filter, &cfg, &SensorSnapshot::default());

        let snapshot = SensorSnapshot {
            available: true,
            temperatures: vec![SensorSample {
                chip: "chip0".to_string(),
                kind: "temperature".to_string(),
                label: "temp1".to_string(),
                value: 42.0,
            }],
            fans: vec![SensorSample {
                chip: "chip0".to_string(),
                kind: "fan".to_string(),
                label: "fan1".to_string(),
                value: 1200.0,
            }],
            voltages: vec![SensorSample {
                chip: "chip0".to_string(),
                kind: "voltage".to_string(),
                label: "in1".to_string(),
                value: 1.2,
            }],
        };
        record_snapshot(&instruments, &filter, &cfg, &snapshot);

        std::env::remove_var("OJO_SENSORS_CONFIG");
        std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
        std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");
        fs::remove_file(&path).expect("cleanup config");

        let filter_block = PrefixFilter::new(vec!["system.unrelated.".to_string()], vec![]);
        let gauge_u64 = meter.u64_gauge("system.sensor.test.u64").build();
        let gauge_f64 = meter.f64_gauge("system.sensor.test.f64").build();
        record_u64(&gauge_u64, &filter_block, "system.sensor.test.u64", 1, &[]);
        record_f64(
            &gauge_f64,
            &filter_block,
            "system.sensor.test.f64",
            1.0,
            &[],
        );
    }

    #[test]
    fn config_load_from_args_covers_defaults_and_missing_path_error() {
        let _guard = env_lock().lock().expect("env lock");
        std::env::remove_var("OJO_SENSORS_CONFIG");
        std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
        std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");
        let args = vec!["ojo-sensors".to_string()];
        let cfg = Config::load_from_args(&args).expect("load defaults");
        assert!(!cfg.service_name.is_empty());

        let missing = unique_temp_path("sensors-missing-config.yaml");
        let args = vec![
            "ojo-sensors".to_string(),
            "--config".to_string(),
            missing.to_string_lossy().to_string(),
        ];
        let err = Config::load_from_args(&args).unwrap_err();
        assert!(err.to_string().contains("was not found"), "{err}");

        let path = unique_temp_path("sensors-args-defaults.yaml");
        fs::write(&path, "collection:\n  poll_interval_secs: 2\n").expect("write config");
        std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
        std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");

        let args = vec![
            "ojo-sensors".to_string(),
            "--config".to_string(),
            path.to_string_lossy().to_string(),
        ];
        std::env::set_var("OJO_RUN_ONCE", "true");
        let cfg = Config::load_from_args(&args).expect("load args defaults");
        assert_eq!(cfg.otlp_endpoint, "http://127.0.0.1:4318/v1/metrics");
        assert_eq!(cfg.otlp_protocol, "http/protobuf");
        assert_eq!(cfg.service_name, "ojo-sensors");
        assert!(cfg.once);
        std::env::set_var("OJO_RUN_ONCE", "yes");
        assert!(Config::load_from_args(&args).expect("load yes").once);
        std::env::set_var("OJO_RUN_ONCE", "on");
        assert!(Config::load_from_args(&args).expect("load on").once);
        std::env::set_var("OJO_RUN_ONCE", "1");
        assert!(Config::load_from_args(&args).expect("load 1").once);
        std::env::remove_var("OJO_RUN_ONCE");
        fs::remove_file(&path).expect("cleanup config");
    }

    #[test]
    fn advance_export_state_covers_all_transitions() {
        assert_eq!(
            advance_export_state(ExportState::Pending, true),
            (ExportState::Connected, FlushEvent::Connected)
        );
        assert_eq!(
            advance_export_state(ExportState::Reconnecting, true),
            (ExportState::Connected, FlushEvent::Reconnected)
        );
        assert_eq!(
            advance_export_state(ExportState::Connected, true),
            (ExportState::Connected, FlushEvent::None)
        );
        assert_eq!(
            advance_export_state(ExportState::Connected, false),
            (ExportState::Reconnecting, FlushEvent::Reconnecting)
        );
        assert_eq!(
            advance_export_state(ExportState::Pending, false),
            (ExportState::Reconnecting, FlushEvent::StillUnavailable)
        );
        assert_eq!(
            advance_export_state(ExportState::Reconnecting, false),
            (ExportState::Reconnecting, FlushEvent::StillUnavailable)
        );
    }

    #[test]
    fn main_runs_once_with_temp_config() {
        let _guard = env_lock().lock().expect("env lock");
        let path = unique_temp_path("main-once.yaml");
        fs::write(
            &path,
            "service:\n  name: sensors-main-test\n  instance_id: sensors-main-01\ncollection:\n  poll_interval_secs: 1\nsensors:\n  include_sensor_labels: false\n  max_labeled_sensors: 0\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
        )
        .expect("write config");

        std::env::set_var("OJO_SENSORS_CONFIG", &path);
        std::env::set_var("OJO_RUN_ONCE", "1");
        let result = super::run();
        assert!(result.is_ok(), "{result:?}");
        std::env::remove_var("OJO_SENSORS_CONFIG");
        std::env::remove_var("OJO_RUN_ONCE");
        fs::remove_file(&path).expect("cleanup config");
    }

    #[test]
    fn run_returns_error_for_invalid_or_missing_config() {
        let _guard = env_lock().lock().expect("env lock");

        let path = unique_temp_path("sensors-invalid-proto.yaml");
        fs::write(
            &path,
            "service:\n  name: sensors-main-test\n  instance_id: sensors-main-01\ncollection:\n  poll_interval_secs: 1\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4317\n    protocol: badproto\n",
        )
        .expect("write config");
        std::env::set_var("OJO_SENSORS_CONFIG", &path);
        std::env::set_var("OJO_RUN_ONCE", "1");
        assert!(run().is_err());
        std::env::remove_var("OJO_SENSORS_CONFIG");
        std::env::remove_var("OJO_RUN_ONCE");
        fs::remove_file(&path).expect("cleanup config");

        let missing = unique_temp_path("sensors-run-missing.yaml");
        std::env::set_var("OJO_SENSORS_CONFIG", &missing);
        std::env::set_var("OJO_RUN_ONCE", "1");
        assert!(run().is_err());
        std::env::remove_var("OJO_SENSORS_CONFIG");
        std::env::remove_var("OJO_RUN_ONCE");
    }

    #[test]
    fn flush_sleep_and_signal_helpers_cover_paths() {
        let now = Instant::now();
        log_flush_result(now, true);
        log_flush_result(now, false);

        handle_flush_event(FlushEvent::Connected, None);
        handle_flush_event(FlushEvent::Reconnected, None);
        handle_flush_event(FlushEvent::None, None);
        handle_flush_event(FlushEvent::Reconnecting, Some(&"err"));
        handle_flush_event(FlushEvent::StillUnavailable, Some(&"err"));
        handle_flush_event(FlushEvent::Connected, Some(&"err"));

        let running = AtomicBool::new(false);
        sleep_until(
            Instant::now() + Duration::from_millis(2),
            &running,
            Duration::from_millis(1),
        );
        let running = AtomicBool::new(true);
        maybe_sleep_until_next_poll(false, Instant::now(), Duration::from_millis(2), &running);
        maybe_sleep_until_next_poll(true, Instant::now(), Duration::from_secs(1), &running);

        let running = Arc::new(AtomicBool::new(true));
        let stop = make_stop_handler(Arc::clone(&running));
        stop();
        assert!(!running.load(Ordering::SeqCst));
        install_signal_handler(&running);
        install_signal_handler(&running);
    }
}
