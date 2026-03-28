use super::{
    advance_export_state, cap_samples_for_labels, handle_flush_event, install_signal_handler,
    load_yaml_config_file, log_flush_result, make_stop_handler, maybe_sleep_until_next_poll,
    record_f64, record_snapshot, record_u64, resolve_default_config_path, run, sleep_until, Config,
    ExportState, FlushEvent, Instruments, SensorSample, SensorSnapshot,
};
use host_collectors::PrefixFilter;
use std::fs;
use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const METRICS: &[(&str, &str)] = &[
    ("system.sensor.temperature.celsius", "gauge"),
    ("system.sensor.temperature.max.celsius", "gauge"),
    ("system.sensor.fan.rpm", "gauge"),
    ("system.sensor.voltage.volts", "gauge"),
    ("system.sensor.count", "inventory"),
    ("system.sensor.source.available", "state"),
];

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
    let resolved = resolve_default_config_path(local.to_string_lossy().as_ref(), "fallback.yaml");
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
