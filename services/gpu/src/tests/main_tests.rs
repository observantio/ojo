use crate::{
    advance_export_state, cap_samples_for_labels, handle_flush_event, install_signal_handler,
    load_yaml_config_file, log_flush_result, make_stop_handler, maybe_sleep_until_next_poll,
    record_f64, record_snapshot, record_u64, resolve_default_config_path, run, sleep_until, Config,
    ExportState, FlushEvent, GpuSample, GpuSnapshot, Instruments,
};
use host_collectors::{ArchiveStorageConfig, PrefixFilter};
use std::collections::BTreeMap;
use std::fs;
use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const METRICS: &[(&str, &str)] = &[
    ("system.gpu.devices", "inventory"),
    ("system.gpu.utilization.ratio", "gauge_ratio"),
    ("system.gpu.memory.used.bytes", "gauge"),
    ("system.gpu.memory.total.bytes", "gauge"),
    ("system.gpu.temperature.celsius", "gauge"),
    ("system.gpu.power.watts", "gauge"),
    ("system.gpu.throttled", "state"),
    ("system.gpu.source.available", "state"),
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
        "ojo-gpu-main-{name}-{}-{nanos}",
        std::process::id()
    ))
}

#[test]
fn metric_contract_uses_supported_namespaces() {
    for (name, semantic) in METRICS {
        assert!(name.starts_with("system."));
        assert!(matches!(
            *semantic,
            "gauge" | "gauge_ratio" | "inventory" | "state"
        ));
    }
}

#[test]
fn caps_device_labels() {
    let samples = vec![
        GpuSample {
            index: 3,
            ..GpuSample::default()
        },
        GpuSample {
            index: 1,
            ..GpuSample::default()
        },
        GpuSample {
            index: 2,
            ..GpuSample::default()
        },
    ];
    let capped = cap_samples_for_labels(&samples, 2);
    assert_eq!(capped.len(), 2);
    assert_eq!(capped[0].index, 1);
    assert_eq!(capped[1].index, 2);
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
            "service:\n  name: gpu-svc\n  instance_id: gpu-01\ncollection:\n  poll_interval_secs: 2\ngpu:\n  include_device_labels: true\n  max_labeled_devices: 2\n",
        )
        .expect("write config");

    std::env::set_var("OJO_GPU_CONFIG", &path);
    std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4317");
    std::env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc");

    let cfg = Config::load().expect("load config");
    assert_eq!(cfg.service_name, "gpu-svc");
    assert_eq!(cfg.instance_id, "gpu-01");
    assert_eq!(cfg.otlp_protocol, "grpc");

    let meter = opentelemetry::global::meter("gpu-test-meter");
    let instruments = Instruments::new(&meter);
    let filter = PrefixFilter::new(vec!["system.gpu.".to_string()], vec![]);
    record_snapshot(&instruments, &filter, &cfg, &GpuSnapshot::default());

    let snap = GpuSnapshot {
        available: true,
        samples: vec![GpuSample {
            index: 0,
            name: "GPU 0".to_string(),
            util_ratio: 0.4,
            memory_used_bytes: 1024.0,
            memory_total_bytes: 4096.0,
            temperature_celsius: 60.0,
            power_watts: 120.0,
            throttled: false,
        }],
    };
    record_snapshot(&instruments, &filter, &cfg, &snap);

    std::env::remove_var("OJO_GPU_CONFIG");
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");
    fs::remove_file(&path).expect("cleanup config");

    let _cfg_shape = Config {
        service_name: "svc".to_string(),
        instance_id: "inst".to_string(),
        poll_interval: Duration::from_secs(1),
        include_device_labels: false,
        max_labeled_devices: 1,
        otlp_endpoint: "http://127.0.0.1:4318/v1/metrics".to_string(),
        otlp_protocol: "http/protobuf".to_string(),
        otlp_headers: BTreeMap::new(),
        otlp_compression: None,
        otlp_timeout: None,
        export_interval: None,
        export_timeout: None,
        metrics_include: vec![],
        metrics_exclude: vec![],
        archive: ArchiveStorageConfig {
            enabled: false,
            archive_dir: String::new(),
            max_file_bytes: 0,
            retain_files: 0,
            file_stem: "gpu-snapshots".to_string(),
        },
        once: true,
    };

    let filter_block = PrefixFilter::new(vec!["system.unrelated.".to_string()], vec![]);
    let gauge_u64 = meter.u64_gauge("system.gpu.test.u64").build();
    let gauge_f64 = meter.f64_gauge("system.gpu.test.f64").build();
    record_u64(&gauge_u64, &filter_block, "system.gpu.test.u64", 1, &[]);
    record_f64(&gauge_f64, &filter_block, "system.gpu.test.f64", 1.0, &[]);
}

#[test]
fn config_load_from_args_covers_defaults_and_missing_path_error() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::remove_var("OJO_GPU_CONFIG");
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");
    let args = vec!["ojo-gpu".to_string()];
    let cfg = Config::load_from_args(&args).expect("load defaults");
    assert!(!cfg.service_name.is_empty());

    let missing = unique_temp_path("gpu-missing-config.yaml");
    let args = vec![
        "ojo-gpu".to_string(),
        "--config".to_string(),
        missing.to_string_lossy().to_string(),
    ];
    let err = Config::load_from_args(&args).unwrap_err();
    assert!(err.to_string().contains("was not found"), "{err}");

    let path = unique_temp_path("gpu-args-defaults.yaml");
    fs::write(&path, "collection:\n  poll_interval_secs: 2\n").expect("write config");
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");

    let args = vec![
        "ojo-gpu".to_string(),
        "--config".to_string(),
        path.to_string_lossy().to_string(),
    ];
    std::env::set_var("OJO_RUN_ONCE", "true");
    let cfg = Config::load_from_args(&args).expect("load args defaults");
    assert_eq!(cfg.otlp_endpoint, "http://127.0.0.1:4318/v1/metrics");
    assert_eq!(cfg.otlp_protocol, "http/protobuf");
    assert_eq!(cfg.service_name, "ojo-gpu");
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
fn config_load_reads_from_environment_config_path() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("gpu-load-direct.yaml");
    fs::write(&path, "collection:\n  poll_interval_secs: 1\n").expect("write config");

    std::env::set_var("OJO_GPU_CONFIG", &path);
    let cfg = Config::load().expect("load config");
    assert_eq!(cfg.poll_interval, Duration::from_secs(1));

    std::env::remove_var("OJO_GPU_CONFIG");
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
            "service:\n  name: gpu-main-test\n  instance_id: gpu-main-01\ncollection:\n  poll_interval_secs: 1\ngpu:\n  include_device_labels: false\n  max_labeled_devices: 0\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
        )
        .expect("write config");

    std::env::set_var("OJO_GPU_CONFIG", &path);
    std::env::set_var("OJO_RUN_ONCE", "1");
    let result = run();
    assert!(result.is_ok(), "{result:?}");
    std::env::remove_var("OJO_GPU_CONFIG");
    std::env::remove_var("OJO_RUN_ONCE");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn run_returns_error_for_invalid_otlp_protocol() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("gpu-invalid-proto.yaml");
    fs::write(
            &path,
            "service:\n  name: gpu-main-test\n  instance_id: gpu-main-01\ncollection:\n  poll_interval_secs: 1\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4317\n    protocol: badproto\n",
        )
        .expect("write config");

    std::env::set_var("OJO_GPU_CONFIG", &path);
    std::env::set_var("OJO_RUN_ONCE", "1");
    let result = run();
    assert!(result.is_err());
    std::env::remove_var("OJO_GPU_CONFIG");
    std::env::remove_var("OJO_RUN_ONCE");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn run_returns_error_for_missing_config() {
    let _guard = env_lock().lock().expect("env lock");
    let missing = unique_temp_path("gpu-run-missing.yaml");
    std::env::set_var("OJO_GPU_CONFIG", &missing);
    std::env::set_var("OJO_RUN_ONCE", "1");
    let result = run();
    assert!(result.is_err());
    std::env::remove_var("OJO_GPU_CONFIG");
    std::env::remove_var("OJO_RUN_ONCE");
}

#[test]
fn flush_and_sleep_helpers_cover_paths() {
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
}

#[test]
fn stop_handler_and_install_signal_handler_are_safe() {
    let running = Arc::new(AtomicBool::new(true));
    let stop = make_stop_handler(Arc::clone(&running));
    stop();
    assert!(!running.load(Ordering::SeqCst));

    install_signal_handler(&running);
    install_signal_handler(&running);
}

#[test]
fn record_snapshot_handles_available_empty_samples() {
    let meter = opentelemetry::global::meter("gpu-test-empty");
    let instruments = Instruments::new(&meter);
    let filter = PrefixFilter::new(vec!["system.gpu.".to_string()], vec![]);
    let cfg = Config {
        service_name: "svc".to_string(),
        instance_id: "inst".to_string(),
        poll_interval: Duration::from_secs(1),
        include_device_labels: false,
        max_labeled_devices: 1,
        otlp_endpoint: "http://127.0.0.1:4318/v1/metrics".to_string(),
        otlp_protocol: "http/protobuf".to_string(),
        otlp_headers: BTreeMap::new(),
        otlp_compression: None,
        otlp_timeout: None,
        export_interval: None,
        export_timeout: None,
        metrics_include: vec!["system.gpu.".to_string()],
        metrics_exclude: vec![],
        archive: ArchiveStorageConfig {
            enabled: false,
            archive_dir: String::new(),
            max_file_bytes: 0,
            retain_files: 0,
            file_stem: "gpu-snapshots".to_string(),
        },
        once: true,
    };
    let snap = GpuSnapshot {
        available: true,
        samples: vec![],
    };
    record_snapshot(&instruments, &filter, &cfg, &snap);
}

#[test]
fn record_snapshot_returns_early_when_device_labels_disabled() {
    let meter = opentelemetry::global::meter("gpu-test-no-device-labels");
    let instruments = Instruments::new(&meter);
    let filter = PrefixFilter::new(vec!["system.gpu.".to_string()], vec![]);
    let cfg = Config {
        service_name: "svc".to_string(),
        instance_id: "inst".to_string(),
        poll_interval: Duration::from_secs(1),
        include_device_labels: false,
        max_labeled_devices: 1,
        otlp_endpoint: "http://127.0.0.1:4318/v1/metrics".to_string(),
        otlp_protocol: "http/protobuf".to_string(),
        otlp_headers: BTreeMap::new(),
        otlp_compression: None,
        otlp_timeout: None,
        export_interval: None,
        export_timeout: None,
        metrics_include: vec!["system.gpu.".to_string()],
        metrics_exclude: vec![],
        archive: ArchiveStorageConfig {
            enabled: false,
            archive_dir: String::new(),
            max_file_bytes: 0,
            retain_files: 0,
            file_stem: "gpu-snapshots".to_string(),
        },
        once: true,
    };
    let snap = GpuSnapshot {
        available: true,
        samples: vec![GpuSample {
            index: 0,
            name: "GPU 0".to_string(),
            util_ratio: 0.2,
            memory_used_bytes: 1.0,
            memory_total_bytes: 2.0,
            temperature_celsius: 40.0,
            power_watts: 90.0,
            throttled: false,
        }],
    };
    record_snapshot(&instruments, &filter, &cfg, &snap);
}

#[test]
fn record_snapshot_labeled_throttled_false_branch() {
    let meter = opentelemetry::global::meter("gpu-test-throttle");
    let instruments = Instruments::new(&meter);
    let filter = PrefixFilter::new(vec!["system.gpu.".to_string()], vec![]);
    let cfg = Config {
        service_name: "svc".to_string(),
        instance_id: "inst".to_string(),
        poll_interval: Duration::from_secs(1),
        include_device_labels: true,
        max_labeled_devices: 1,
        otlp_endpoint: "http://127.0.0.1:4318/v1/metrics".to_string(),
        otlp_protocol: "http/protobuf".to_string(),
        otlp_headers: BTreeMap::new(),
        otlp_compression: None,
        otlp_timeout: None,
        export_interval: None,
        export_timeout: None,
        metrics_include: vec!["system.gpu.".to_string()],
        metrics_exclude: vec![],
        archive: ArchiveStorageConfig {
            enabled: false,
            archive_dir: String::new(),
            max_file_bytes: 0,
            retain_files: 0,
            file_stem: "gpu-snapshots".to_string(),
        },
        once: true,
    };
    let snap = GpuSnapshot {
        available: true,
        samples: vec![GpuSample {
            index: 0,
            name: "GPU 0".to_string(),
            util_ratio: 0.2,
            memory_used_bytes: 1.0,
            memory_total_bytes: 2.0,
            temperature_celsius: 40.0,
            power_watts: 90.0,
            throttled: false,
        }],
    };
    record_snapshot(&instruments, &filter, &cfg, &snap);

    let snap = GpuSnapshot {
        available: true,
        samples: vec![GpuSample {
            index: 1,
            name: "GPU 1".to_string(),
            util_ratio: 0.8,
            memory_used_bytes: 3.0,
            memory_total_bytes: 4.0,
            temperature_celsius: 55.0,
            power_watts: 130.0,
            throttled: true,
        }],
    };
    record_snapshot(&instruments, &filter, &cfg, &snap);
}

#[test]
fn resolve_default_config_path_returns_repo_relative_when_missing() {
    let selected =
        resolve_default_config_path("/definitely/missing/gpu.yaml", "services/gpu/gpu.yaml");
    assert_eq!(selected, "services/gpu/gpu.yaml");
}
