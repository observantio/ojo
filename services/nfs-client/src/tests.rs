use super::{
    advance_export_state, derive_rates_or_reset, handle_flush_event, install_signal_handler,
    load_yaml_config_file, log_flush_result, make_stop_handler, maybe_sleep_until_next_poll,
    record_f64, record_snapshot, record_u64, resolve_default_config_path, run, saturating_rate,
    sleep_until, Config, ExportState, FlushEvent, Instruments, NfsClientSnapshot, NfsRates,
    PrevState,
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
    std::env::temp_dir().join(format!("ojo-nfs-{name}-{}-{nanos}", std::process::id()))
}

#[test]
fn saturating_rate_handles_counter_reset_and_normal_rate() {
    assert_eq!(saturating_rate(100, 90, 1.0), 0.0);
    assert_eq!(saturating_rate(10, 40, 2.0), 15.0);
}

#[test]
fn prev_state_derive_initializes_and_then_computes_rates() {
    let mut state = PrevState::default();
    let first = NfsClientSnapshot {
        rpc_calls_total: 100,
        rpc_retransmissions_total: 10,
        ..NfsClientSnapshot::default()
    };
    let rates = state.derive(&first);
    assert_eq!(rates.rpc_calls_rate, 0.0);
    assert_eq!(rates.rpc_retransmissions_rate, 0.0);

    state.last = Some((first, Instant::now() - Duration::from_secs(2)));
    let second = NfsClientSnapshot {
        rpc_calls_total: 180,
        rpc_retransmissions_total: 16,
        ..NfsClientSnapshot::default()
    };
    let rates = state.derive(&second);
    assert!(rates.rpc_calls_rate > 30.0);
    assert!(rates.rpc_retransmissions_rate > 2.0);
}

#[test]
fn prev_state_derive_resets_on_non_progressing_time() {
    let mut state = PrevState {
        last: Some((
            NfsClientSnapshot::default(),
            Instant::now() + Duration::from_secs(1),
        )),
    };
    let rates = state.derive(&NfsClientSnapshot::default());
    assert_eq!(rates.rpc_calls_rate, 0.0);
    assert_eq!(rates.rpc_retransmissions_rate, 0.0);
}

#[test]
fn derive_rates_or_reset_resets_state_when_snapshot_unavailable() {
    let mut state = PrevState::default();
    let available = NfsClientSnapshot {
        available: true,
        rpc_calls_total: 10,
        rpc_retransmissions_total: 1,
        ..NfsClientSnapshot::default()
    };
    let _ = derive_rates_or_reset(&mut state, &available);
    assert!(state.last.is_some());

    let unavailable = NfsClientSnapshot::default();
    let rates = derive_rates_or_reset(&mut state, &unavailable);
    assert_eq!(rates.rpc_calls_rate, 0.0);
    assert_eq!(rates.rpc_retransmissions_rate, 0.0);
    assert!(state.last.is_none());
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
fn resolve_default_config_path_prefers_local_file_when_present() {
    let local = unique_temp_path("nfs-local.yaml");
    fs::write(&local, "service: {}\n").expect("write local");
    let chosen = resolve_default_config_path(local.to_string_lossy().as_ref(), "fallback.yaml");
    assert_eq!(chosen, local.to_string_lossy());
    fs::remove_file(&local).expect("cleanup local");

    let missing = unique_temp_path("nfs-missing.yaml");
    let chosen = resolve_default_config_path(missing.to_string_lossy().as_ref(), "fallback.yaml");
    assert_eq!(chosen, "fallback.yaml");
}

#[test]
fn load_yaml_config_file_handles_missing_empty_and_valid_yaml() {
    let missing = unique_temp_path("nfs-missing-config.yaml");
    let missing_err = load_yaml_config_file(missing.to_string_lossy().as_ref()).unwrap_err();
    assert!(
        missing_err.to_string().contains("was not found"),
        "{missing_err}"
    );

    let empty = unique_temp_path("nfs-empty-config.yaml");
    fs::write(&empty, " \n").expect("write empty");
    let empty_err = load_yaml_config_file(empty.to_string_lossy().as_ref()).unwrap_err();
    assert!(empty_err.to_string().contains("is empty"), "{empty_err}");
    fs::remove_file(&empty).expect("cleanup empty");

    let valid = unique_temp_path("nfs-valid-config.yaml");
    fs::write(
            &valid,
            "service:\n  name: ojo-nfs-client\n  instance_id: nfs-1\ncollection:\n  poll_interval_secs: 2\nnfs_client:\n  executable: nfsstat\n",
        )
        .expect("write valid");
    let parsed = load_yaml_config_file(valid.to_string_lossy().as_ref());
    assert!(parsed.is_ok(), "{parsed:?}");
    fs::remove_file(&valid).expect("cleanup valid");

    let dir = unique_temp_path("nfs-config-dir");
    fs::create_dir_all(&dir).expect("mkdir");
    let dir_err = load_yaml_config_file(dir.to_string_lossy().as_ref()).unwrap_err();
    assert!(
        dir_err.to_string().contains("failed to read config file"),
        "{dir_err}"
    );
    fs::remove_dir_all(&dir).expect("cleanup dir");

    let invalid = unique_temp_path("nfs-invalid-config.yaml");
    fs::write(&invalid, "service: [\n").expect("write invalid");
    let parse_err = load_yaml_config_file(invalid.to_string_lossy().as_ref()).unwrap_err();
    assert!(
        parse_err.to_string().contains("failed to parse YAML"),
        "{parse_err}"
    );
    fs::remove_file(&invalid).expect("cleanup invalid");
}

#[test]
fn config_load_reads_yaml_and_applies_defaults() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("nfs-load.yaml");
    fs::write(
            &path,
            "service:\n  name: nfs-svc\n  instance_id: nfs-01\ncollection:\n  poll_interval_secs: 1\nnfs_client:\n  executable: nfsstat\n",
        )
        .expect("write config");

    std::env::set_var("OJO_NFS_CLIENT_CONFIG", &path);
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");

    let cfg = Config::load().expect("load config");
    assert_eq!(cfg.service_name, "nfs-svc");
    assert_eq!(cfg.instance_id, "nfs-01");
    assert_eq!(cfg.poll_interval, Duration::from_secs(1));
    assert_eq!(cfg.otlp_endpoint, "http://127.0.0.1:4318/v1/metrics");
    assert_eq!(cfg.otlp_protocol, "http/protobuf");
    assert_eq!(cfg.metrics_include, vec!["system.nfs_client.".to_string()]);
    assert_eq!(cfg.nfs_client.executable.as_deref(), Some("nfsstat"));

    std::env::remove_var("OJO_NFS_CLIENT_CONFIG");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn config_load_uses_otlp_env_fallback_when_export_section_missing() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("nfs-env-load.yaml");
    fs::write(
            &path,
            "service:\n  name: nfs-svc\n  instance_id: nfs-01\ncollection:\n  poll_interval_secs: 2\nnfs_client:\n  executable: nfsstat\n",
        )
        .expect("write config");

    std::env::set_var("OJO_NFS_CLIENT_CONFIG", &path);
    std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4317");
    std::env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc");

    let cfg = Config::load().expect("load config");
    assert_eq!(cfg.otlp_endpoint, "http://127.0.0.1:4317");
    assert_eq!(cfg.otlp_protocol, "grpc");

    std::env::remove_var("OJO_NFS_CLIENT_CONFIG");
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn config_load_from_args_covers_default_and_missing_config_error() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::remove_var("OJO_NFS_CLIENT_CONFIG");
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");
    let args = vec!["ojo-nfs-client".to_string()];
    let cfg = Config::load_from_args(&args).expect("load default config");
    assert!(!cfg.service_name.is_empty());

    let missing = unique_temp_path("nfs-missing-from-args.yaml");
    let args = vec![
        "ojo-nfs-client".to_string(),
        "--config".to_string(),
        missing.to_string_lossy().to_string(),
    ];
    let err = Config::load_from_args(&args).unwrap_err();
    assert!(err.to_string().contains("was not found"), "{err}");

    let default_name_cfg = unique_temp_path("nfs-default-name.yaml");
    fs::write(
        &default_name_cfg,
        "service:\n  instance_id: nfs-from-test\ncollection:\n  poll_interval_secs: 1\n",
    )
    .expect("write default-name config");
    let args = vec![
        "ojo-nfs-client".to_string(),
        "--config".to_string(),
        default_name_cfg.to_string_lossy().to_string(),
    ];
    let cfg = Config::load_from_args(&args).expect("load default-name config");
    assert_eq!(cfg.service_name, "ojo-nfs-client");
    fs::remove_file(&default_name_cfg).expect("cleanup default-name config");

    let run_once_cfg = unique_temp_path("nfs-run-once-values.yaml");
    fs::write(
            &run_once_cfg,
            "service:\n  name: nfs-once\n  instance_id: nfs-once-01\ncollection:\n  poll_interval_secs: 1\n",
        )
        .expect("write run-once config");
    for value in ["true", "yes", "on"] {
        std::env::set_var("OJO_RUN_ONCE", value);
        let args = vec![
            "ojo-nfs-client".to_string(),
            "--config".to_string(),
            run_once_cfg.to_string_lossy().to_string(),
        ];
        let cfg = Config::load_from_args(&args).expect("load run-once value config");
        assert!(cfg.once, "expected once=true for value={value}");
    }
    std::env::remove_var("OJO_RUN_ONCE");
    fs::remove_file(&run_once_cfg).expect("cleanup run-once config");
}

#[test]
fn config_load_reads_from_environment_config_path() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("nfs-load-direct.yaml");
    fs::write(&path, "collection:\n  poll_interval_secs: 1\n").expect("write config");

    std::env::set_var("OJO_NFS_CLIENT_CONFIG", &path);
    let cfg = Config::load().expect("load config");
    assert_eq!(cfg.poll_interval, Duration::from_secs(1));

    std::env::remove_var("OJO_NFS_CLIENT_CONFIG");
    fs::remove_file(&path).expect("cleanup config");
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
fn stop_handler_and_signal_install_are_safe() {
    let running = Arc::new(AtomicBool::new(true));
    let stop = make_stop_handler(Arc::clone(&running));
    stop();
    assert!(!running.load(Ordering::SeqCst));

    install_signal_handler(&running);
    install_signal_handler(&running);
}

#[test]
fn record_helpers_skip_when_filter_blocks_metric() {
    let meter = opentelemetry::global::meter("nfs-filter-test");
    let gauge_u64 = meter.u64_gauge("system.nfs_client.test.u64").build();
    let gauge_f64 = meter.f64_gauge("system.nfs_client.test.f64").build();
    let deny_all = PrefixFilter::new(vec!["system.unrelated.".to_string()], vec![]);
    record_u64(&gauge_u64, &deny_all, "system.nfs_client.test.u64", 1);
    record_f64(&gauge_f64, &deny_all, "system.nfs_client.test.f64", 1.0);
}

#[test]
fn run_returns_error_for_missing_or_invalid_config() {
    let _guard = env_lock().lock().expect("env lock");

    let missing = unique_temp_path("nfs-run-missing.yaml");
    std::env::set_var("OJO_NFS_CLIENT_CONFIG", &missing);
    std::env::set_var("OJO_RUN_ONCE", "1");
    let result = run();
    assert!(result.is_err());

    let invalid = unique_temp_path("nfs-run-invalid.yaml");
    fs::write(
            &invalid,
            "service:\n  name: nfs-main-test\n  instance_id: nfs-main-01\ncollection:\n  poll_interval_secs: 1\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4317\n    protocol: badproto\n",
        )
        .expect("write config");
    std::env::set_var("OJO_NFS_CLIENT_CONFIG", &invalid);
    let result = run();
    assert!(result.is_err());

    std::env::remove_var("OJO_NFS_CLIENT_CONFIG");
    std::env::remove_var("OJO_RUN_ONCE");
    fs::remove_file(&invalid).expect("cleanup config");
}

#[test]
fn record_snapshot_handles_unavailable_and_available_samples() {
    let meter = opentelemetry::global::meter("nfs-test-meter");
    let instruments = Instruments::new(&meter);
    let filter = PrefixFilter::new(vec!["system.nfs_client.".to_string()], vec![]);

    let unavailable = NfsClientSnapshot::default();
    record_snapshot(&instruments, &filter, &unavailable, &NfsRates::default());

    let available = NfsClientSnapshot {
        available: true,
        mounts: 2,
        rpc_calls_total: 100,
        rpc_retransmissions_total: 5,
        rpc_auth_refreshes_total: 1,
    };
    let rates = NfsRates {
        rpc_calls_rate: 10.0,
        rpc_retransmissions_rate: 0.5,
    };
    record_snapshot(&instruments, &filter, &available, &rates);
}

#[test]
fn main_runs_once_with_temp_config() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("nfs-main-once.yaml");
    fs::write(
            &path,
            "service:\n  name: nfs-main-test\n  instance_id: nfs-main-01\ncollection:\n  poll_interval_secs: 1\nnfs_client:\n  executable: /definitely/missing/nfsstat\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
        )
        .expect("write config");

    std::env::set_var("OJO_NFS_CLIENT_CONFIG", &path);
    std::env::set_var("OJO_RUN_ONCE", "1");
    let result = super::run();
    assert!(result.is_ok(), "{result:?}");
    std::env::remove_var("OJO_NFS_CLIENT_CONFIG");
    std::env::remove_var("OJO_RUN_ONCE");
    fs::remove_file(&path).expect("cleanup config");
}
