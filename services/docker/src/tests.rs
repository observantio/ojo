use super::{
    advance_export_state, cap_samples_for_labels, container_name_label, handle_flush_event,
    install_signal_handler, load_yaml_config_file, log_flush_result, make_stop_handler,
    maybe_sleep_until_next_poll, non_empty_or, parse_pair_bytes, parse_percent,
    parse_size_to_bytes, record_f64, record_snapshot, record_u64, resolve_default_config_path, run,
    sleep_until, Config, DockerSample, DockerSnapshot, ExportState, FlushEvent, Instruments,
};
use host_collectors::PrefixFilter;
use std::collections::BTreeMap;
use std::fs;
use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
struct MetricDef {
    name: &'static str,
    semantic: &'static str,
}

const METRICS: &[MetricDef] = &[
    MetricDef {
        name: "system.docker.containers.total",
        semantic: "gauge",
    },
    MetricDef {
        name: "system.docker.containers.running",
        semantic: "gauge",
    },
    MetricDef {
        name: "system.docker.containers.stopped",
        semantic: "gauge",
    },
    MetricDef {
        name: "system.docker.container.cpu.ratio",
        semantic: "gauge_ratio",
    },
    MetricDef {
        name: "system.docker.container.memory.usage.bytes",
        semantic: "gauge",
    },
    MetricDef {
        name: "system.docker.container.memory.limit.bytes",
        semantic: "gauge",
    },
    MetricDef {
        name: "system.docker.container.network.rx.bytes",
        semantic: "gauge",
    },
    MetricDef {
        name: "system.docker.container.network.tx.bytes",
        semantic: "gauge",
    },
    MetricDef {
        name: "system.docker.container.block.read.bytes",
        semantic: "gauge",
    },
    MetricDef {
        name: "system.docker.container.block.write.bytes",
        semantic: "gauge",
    },
    MetricDef {
        name: "system.docker.source.available",
        semantic: "gauge",
    },
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
        "ojo-docker-main-{name}-{}-{nanos}",
        std::process::id()
    ))
}

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
    assert_eq!(parse_percent("n/a"), 0.0);
    assert_eq!(parse_size_to_bytes(""), 0.0);
    assert_eq!(parse_size_to_bytes("1B"), 1.0);
    assert_eq!(parse_size_to_bytes("1KiB"), 1024.0);
    assert_eq!(parse_size_to_bytes("1MB"), 1_000_000.0);
    assert_eq!(parse_size_to_bytes("1GB"), 1_000_000_000.0);
    assert!(parse_size_to_bytes("1TB") > parse_size_to_bytes("1GiB"));
    assert!(parse_size_to_bytes("1TiB") > parse_size_to_bytes("1GiB"));
    assert_eq!(parse_size_to_bytes("3widgets"), 3.0);
}

#[test]
fn container_name_and_fallback_helpers_handle_empty_values() {
    let sample = DockerSample {
        id: "abcdef1234567890".to_string(),
        name: " ".to_string(),
        ..DockerSample::default()
    };
    assert_eq!(container_name_label(&sample), "abcdef123456");

    let unknown = DockerSample {
        id: "   ".to_string(),
        name: "".to_string(),
        ..DockerSample::default()
    };
    assert_eq!(container_name_label(&unknown), "unknown-container");
    assert_eq!(non_empty_or("  ", "fallback"), "fallback");
    assert_eq!(non_empty_or(" value ", "fallback"), "value");
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

    let empty = unique_temp_path("empty.yaml");
    fs::write(&empty, "\n").expect("write empty");
    let err = load_yaml_config_file(empty.to_string_lossy().as_ref()).unwrap_err();
    assert!(err.to_string().contains("is empty"), "{err}");
    fs::remove_file(&empty).expect("cleanup empty");
}

#[test]
fn config_load_uses_file_and_env_fallbacks() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("config.yaml");
    fs::write(
            &path,
            "service:\n  name: docker-svc\n  instance_id: docker-01\ncollection:\n  poll_interval_secs: 2\ndocker:\n  include_container_labels: true\n  max_labeled_containers: 3\n",
        )
        .expect("write config");

    std::env::set_var("OJO_DOCKER_CONFIG", &path);
    std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4317");
    std::env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc");

    let cfg = Config::load().expect("load config");
    assert_eq!(cfg.service_name, "docker-svc");
    assert_eq!(cfg.instance_id, "docker-01");
    assert_eq!(cfg.poll_interval, Duration::from_secs(2));
    assert!(cfg.include_labels);
    assert_eq!(cfg.max_labeled_containers, 3);
    assert_eq!(cfg.otlp_endpoint, "http://127.0.0.1:4317");
    assert_eq!(cfg.otlp_protocol, "grpc");

    std::env::remove_var("OJO_DOCKER_CONFIG");
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn config_load_from_args_covers_flags_and_missing_path_error() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("docker-args.yaml");
    fs::write(&path, "collection:\n  poll_interval_secs: 2\n").expect("write config");

    std::env::remove_var("OJO_DOCKER_CONFIG");
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");

    let args = vec![
        "ojo-docker".to_string(),
        "--config".to_string(),
        path.to_string_lossy().to_string(),
        "--once".to_string(),
    ];
    let cfg = Config::load_from_args(&args).expect("load args config");
    assert!(cfg.once);

    std::env::set_var("OJO_RUN_ONCE", "on");
    let cfg = Config::load_from_args(&args[..3]).expect("load env once");
    assert!(cfg.once);
    std::env::remove_var("OJO_RUN_ONCE");

    let missing = unique_temp_path("docker-missing.yaml");
    let missing_args = vec![
        "ojo-docker".to_string(),
        "--config".to_string(),
        missing.to_string_lossy().to_string(),
    ];
    let err = Config::load_from_args(&missing_args).unwrap_err();
    assert!(err.to_string().contains("was not found"), "{err}");

    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn config_load_reads_from_environment_config_path() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("docker-load-direct.yaml");
    fs::write(&path, "collection:\n  poll_interval_secs: 1\n").expect("write config");

    std::env::set_var("OJO_DOCKER_CONFIG", &path);
    let cfg = Config::load().expect("load config");
    assert_eq!(cfg.poll_interval, Duration::from_secs(1));

    std::env::remove_var("OJO_DOCKER_CONFIG");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn config_load_from_args_uses_repo_relative_default() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::remove_var("OJO_DOCKER_CONFIG");
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");
    let args = vec!["ojo-docker".to_string()];
    let cfg = Config::load_from_args(&args).expect("load default config path");
    assert!(!cfg.service_name.is_empty());
}

#[test]
fn resolve_default_config_path_returns_repo_relative_when_local_missing() {
    let selected = resolve_default_config_path(
        "/definitely/missing/docker.yaml",
        "services/docker/docker.yaml",
    );
    assert_eq!(selected, "services/docker/docker.yaml");
}

#[test]
fn load_yaml_config_file_covers_directory_and_invalid_yaml_errors() {
    let dir = unique_temp_path("docker-config-dir");
    fs::create_dir_all(&dir).expect("mkdir");
    let dir_err = load_yaml_config_file(dir.to_string_lossy().as_ref()).unwrap_err();
    assert!(
        dir_err.to_string().contains("failed to read config file"),
        "{dir_err}"
    );
    fs::remove_dir_all(&dir).expect("cleanup dir");

    let invalid = unique_temp_path("docker-invalid.yaml");
    fs::write(&invalid, "service: [\n").expect("write invalid");
    let parse_err = load_yaml_config_file(invalid.to_string_lossy().as_ref()).unwrap_err();
    assert!(
        parse_err.to_string().contains("failed to parse YAML"),
        "{parse_err}"
    );
    fs::remove_file(&invalid).expect("cleanup invalid");
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
fn stop_handler_sets_running_false() {
    let running = Arc::new(AtomicBool::new(true));
    let stop = make_stop_handler(Arc::clone(&running));
    stop();
    assert!(!running.load(Ordering::SeqCst));
}

#[test]
fn install_signal_handler_handles_already_registered_case() {
    let running = Arc::new(AtomicBool::new(true));
    install_signal_handler(&running);
    install_signal_handler(&running);
}

#[test]
fn run_returns_error_for_missing_config() {
    let _guard = env_lock().lock().expect("env lock");
    let missing = unique_temp_path("docker-run-missing.yaml");
    std::env::set_var("OJO_DOCKER_CONFIG", &missing);
    std::env::set_var("OJO_RUN_ONCE", "1");
    let result = run();
    assert!(result.is_err());
    std::env::remove_var("OJO_DOCKER_CONFIG");
    std::env::remove_var("OJO_RUN_ONCE");
}

#[test]
fn run_returns_error_for_invalid_otlp_protocol() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("docker-run-invalid-proto.yaml");
    fs::write(
            &path,
            "service:\n  name: docker-main-test\n  instance_id: docker-main-01\ncollection:\n  poll_interval_secs: 1\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4317\n    protocol: badproto\n",
        )
        .expect("write config");
    std::env::set_var("OJO_DOCKER_CONFIG", &path);
    std::env::set_var("OJO_RUN_ONCE", "1");
    let result = run();
    assert!(result.is_err());
    std::env::remove_var("OJO_DOCKER_CONFIG");
    std::env::remove_var("OJO_RUN_ONCE");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn record_helpers_skip_when_filter_blocks_metric() {
    let meter = opentelemetry::global::meter("docker-filter-test");
    let gauge_u64 = meter.u64_gauge("system.docker.test.u64").build();
    let gauge_f64 = meter.f64_gauge("system.docker.test.f64").build();
    let deny_all = PrefixFilter::new(vec!["system.unrelated.".to_string()], vec![]);
    record_u64(&gauge_u64, &deny_all, "system.docker.test.u64", 1, &[]);
    record_f64(&gauge_f64, &deny_all, "system.docker.test.f64", 1.0, &[]);
}

#[test]
fn record_snapshot_covers_available_unavailable_and_labeled_paths() {
    let meter = opentelemetry::global::meter("docker-test-meter");
    let instruments = Instruments::new(&meter);
    let filter = PrefixFilter::new(vec!["system.docker.".to_string()], vec![]);

    let cfg = Config {
        service_name: "svc".to_string(),
        instance_id: "inst".to_string(),
        poll_interval: Duration::from_secs(1),
        include_labels: true,
        max_labeled_containers: 2,
        otlp_endpoint: "http://127.0.0.1:4318/v1/metrics".to_string(),
        otlp_protocol: "http/protobuf".to_string(),
        otlp_headers: BTreeMap::new(),
        otlp_compression: None,
        otlp_timeout: None,
        export_interval: None,
        export_timeout: None,
        metrics_include: vec![],
        metrics_exclude: vec![],
        once: true,
    };

    let unavailable = DockerSnapshot::default();
    record_snapshot(&instruments, &filter, &cfg, &unavailable);

    let available_empty = DockerSnapshot {
        available: true,
        total: 1,
        running: 1,
        stopped: 0,
        samples: vec![],
    };
    record_snapshot(&instruments, &filter, &cfg, &available_empty);

    let sample = DockerSample {
        id: "abcdef123456".to_string(),
        name: "web".to_string(),
        image: "nginx".to_string(),
        state: "running".to_string(),
        cpu_ratio: 0.5,
        mem_usage_bytes: 1024.0,
        mem_limit_bytes: 2048.0,
        net_rx_bytes: 100.0,
        net_tx_bytes: 200.0,
        block_read_bytes: 50.0,
        block_write_bytes: 60.0,
    };
    let available = DockerSnapshot {
        available: true,
        total: 1,
        running: 1,
        stopped: 0,
        samples: vec![sample],
    };
    record_snapshot(&instruments, &filter, &cfg, &available);
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
            "service:\n  name: docker-main-test\n  instance_id: docker-main-01\ncollection:\n  poll_interval_secs: 1\ndocker:\n  include_container_labels: false\n  max_labeled_containers: 0\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
        )
        .expect("write config");

    std::env::set_var("OJO_DOCKER_CONFIG", &path);
    std::env::set_var("OJO_RUN_ONCE", "1");
    let result = super::run();
    assert!(result.is_ok(), "{result:?}");
    std::env::remove_var("OJO_DOCKER_CONFIG");
    std::env::remove_var("OJO_RUN_ONCE");
    fs::remove_file(&path).expect("cleanup config");
}
