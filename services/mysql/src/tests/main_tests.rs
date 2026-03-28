use crate::{
    advance_export_state, derive_rates_or_reset, handle_flush_event, load_yaml_config_file,
    log_flush_result, make_stop_handler, maybe_sleep_until_next_poll, record_f64, record_snapshot,
    record_u64, resolve_default_config_path, saturating_rate, sleep_until, Config, ExportState,
    FlushEvent, Instruments, MysqlRates, MysqlSnapshot, PrevState,
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
    std::env::temp_dir().join(format!("ojo-mysql-{name}-{}-{nanos}", std::process::id()))
}

#[test]
fn saturating_rate_handles_counter_reset_and_normal_rate() {
    assert_eq!(saturating_rate(100, 90, 1.0), 0.0);
    assert_eq!(saturating_rate(10, 40, 2.0), 15.0);
}

#[test]
fn prev_state_derive_initializes_and_then_computes_rates() {
    let mut state = PrevState::default();
    let first = MysqlSnapshot {
        queries_total: 100,
        bytes_received_total: 1_000,
        bytes_sent_total: 2_000,
        ..MysqlSnapshot::default()
    };
    let rates = state.derive(&first);
    assert_eq!(rates.queries_per_second, 0.0);
    assert!(state.last.is_some());

    state.last = Some((first, Instant::now() - Duration::from_secs(2)));
    let second = MysqlSnapshot {
        queries_total: 160,
        bytes_received_total: 1_600,
        bytes_sent_total: 2_900,
        ..MysqlSnapshot::default()
    };
    let rates = state.derive(&second);
    assert!(rates.queries_per_second > 25.0);
    assert!(rates.bytes_received_per_second > 250.0);
    assert!(rates.bytes_sent_per_second > 400.0);
}

#[test]
fn prev_state_derive_resets_on_non_progressing_time() {
    let mut state = PrevState {
        last: Some((
            MysqlSnapshot::default(),
            Instant::now() + Duration::from_secs(1),
        )),
    };
    let rates = state.derive(&MysqlSnapshot::default());
    assert_eq!(rates.queries_per_second, 0.0);
    assert_eq!(rates.bytes_received_per_second, 0.0);
    assert_eq!(rates.bytes_sent_per_second, 0.0);
}

#[test]
fn derive_rates_or_reset_resets_state_when_snapshot_unavailable() {
    let mut state = PrevState::default();
    let available = MysqlSnapshot {
        available: true,
        queries_total: 10,
        bytes_received_total: 20,
        bytes_sent_total: 30,
        ..MysqlSnapshot::default()
    };
    let _ = derive_rates_or_reset(&mut state, &available);
    assert!(state.last.is_some());

    let unavailable = MysqlSnapshot::default();
    let rates = derive_rates_or_reset(&mut state, &unavailable);
    assert_eq!(rates.queries_per_second, 0.0);
    assert_eq!(rates.bytes_received_per_second, 0.0);
    assert_eq!(rates.bytes_sent_per_second, 0.0);
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
fn flush_helpers_cover_success_failure_and_sleep_paths() {
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
        Instant::now() + Duration::from_millis(5),
        &running,
        Duration::from_millis(1),
    );

    let running = AtomicBool::new(true);
    sleep_until(
        Instant::now() + Duration::from_millis(2),
        &running,
        Duration::from_millis(1),
    );

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
fn should_break_after_iteration_covers_true_and_false() {
    assert!(!super::should_break_after_iteration(false));
    assert!(super::should_break_after_iteration(true));
}

#[test]
fn resolve_default_config_path_prefers_local_file_when_present() {
    let local = unique_temp_path("mysql-local.yaml");
    fs::write(&local, "service: {}\n").expect("write local");
    let chosen = resolve_default_config_path(local.to_string_lossy().as_ref(), "fallback.yaml");
    assert_eq!(chosen, local.to_string_lossy());
    fs::remove_file(&local).expect("cleanup local");

    let missing = unique_temp_path("mysql-missing.yaml");
    let chosen = resolve_default_config_path(missing.to_string_lossy().as_ref(), "fallback.yaml");
    assert_eq!(chosen, "fallback.yaml");
}

#[test]
fn load_yaml_config_file_handles_missing_empty_and_valid_yaml() {
    let missing = unique_temp_path("mysql-missing-config.yaml");
    let missing_err = load_yaml_config_file(missing.to_string_lossy().as_ref()).unwrap_err();
    assert!(
        missing_err.to_string().contains("was not found"),
        "{missing_err}"
    );

    let empty = unique_temp_path("mysql-empty-config.yaml");
    fs::write(&empty, " \n").expect("write empty");
    let empty_err = load_yaml_config_file(empty.to_string_lossy().as_ref()).unwrap_err();
    assert!(empty_err.to_string().contains("is empty"), "{empty_err}");
    fs::remove_file(&empty).expect("cleanup empty");

    let valid = unique_temp_path("mysql-valid-config.yaml");
    fs::write(
            &valid,
            "service:\n  name: ojo-mysql\n  instance_id: mysql-1\ncollection:\n  poll_interval_secs: 2\nmysql:\n  executable: mysql\n",
        )
        .expect("write valid");
    let parsed = load_yaml_config_file(valid.to_string_lossy().as_ref());
    assert!(parsed.is_ok(), "{parsed:?}");
    fs::remove_file(&valid).expect("cleanup valid");
}

#[test]
fn config_load_reads_yaml_and_applies_defaults() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("mysql-load.yaml");
    fs::write(
            &path,
            "service:\n  name: mysql-svc\n  instance_id: mysql-01\ncollection:\n  poll_interval_secs: 1\nmysql:\n  executable: mysql\n  host: '  '\n  user: root\n  password: secret\n  database: app\n",
        )
        .expect("write config");

    std::env::set_var("OJO_MYSQL_CONFIG", &path);
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");

    let cfg = Config::load().expect("load config");
    assert_eq!(cfg.service_name, "mysql-svc");
    assert_eq!(cfg.instance_id, "mysql-01");
    assert_eq!(cfg.poll_interval, Duration::from_secs(1));
    assert_eq!(cfg.otlp_endpoint, "http://127.0.0.1:4318/v1/metrics");
    assert_eq!(cfg.otlp_protocol, "http/protobuf");
    assert_eq!(cfg.metrics_include, vec!["system.mysql.".to_string()]);
    assert!(cfg.mysql.host.is_none());
    assert_eq!(cfg.mysql.user.as_deref(), Some("root"));
    assert_eq!(cfg.mysql.password.as_deref(), Some("secret"));
    assert_eq!(cfg.mysql.database.as_deref(), Some("app"));

    std::env::remove_var("OJO_MYSQL_CONFIG");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn config_load_uses_otlp_env_fallback_when_export_section_missing() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("mysql-env-load.yaml");
    fs::write(
            &path,
            "service:\n  name: mysql-svc\n  instance_id: mysql-01\ncollection:\n  poll_interval_secs: 2\nmysql:\n  executable: mysql\n",
        )
        .expect("write config");

    std::env::set_var("OJO_MYSQL_CONFIG", &path);
    std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4317");
    std::env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc");

    let cfg = Config::load().expect("load config");
    assert_eq!(cfg.otlp_endpoint, "http://127.0.0.1:4317");
    assert_eq!(cfg.otlp_protocol, "grpc");

    std::env::remove_var("OJO_MYSQL_CONFIG");
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn config_load_uses_repo_default_config_path_when_env_not_set() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::remove_var("OJO_MYSQL_CONFIG");
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");

    let cfg = Config::load().expect("load default config");
    assert!(!cfg.service_name.is_empty());
}

#[test]
fn config_load_from_args_supports_config_flag_and_once_aliases() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("mysql-args-load.yaml");
    fs::write(
        &path,
        "collection:\n  poll_interval_secs: 2\nmysql:\n  host: db\n",
    )
    .expect("write config");

    std::env::remove_var("OJO_MYSQL_CONFIG");
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");

    let args = vec![
        "ojo-mysql".to_string(),
        "--config".to_string(),
        path.to_string_lossy().to_string(),
        "--once".to_string(),
    ];
    let cfg = Config::load_from_args(&args).expect("load args config");
    assert_eq!(cfg.service_name, "ojo-mysql");
    assert_eq!(cfg.mysql.executable, "mysql");
    assert_eq!(cfg.mysql.host.as_deref(), Some("db"));
    assert!(cfg.once);

    std::env::set_var("OJO_RUN_ONCE", "yes");
    let cfg = Config::load_from_args(&args[..3]).expect("load with env yes");
    assert!(cfg.once);

    std::env::set_var("OJO_RUN_ONCE", "on");
    let cfg = Config::load_from_args(&args[..3]).expect("load with env on");
    assert!(cfg.once);

    std::env::remove_var("OJO_RUN_ONCE");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn config_load_from_args_errors_for_missing_config_path() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::remove_var("OJO_MYSQL_CONFIG");
    let missing = unique_temp_path("mysql-missing-from-args.yaml");
    let args = vec![
        "ojo-mysql".to_string(),
        "--config".to_string(),
        missing.to_string_lossy().to_string(),
    ];
    let err = Config::load_from_args(&args).unwrap_err();
    assert!(err.to_string().contains("was not found"), "{err}");
}

#[test]
fn config_load_reads_from_environment_config_path() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("mysql-load-direct.yaml");
    fs::write(&path, "collection:\n  poll_interval_secs: 1\n").expect("write config");

    std::env::set_var("OJO_MYSQL_CONFIG", &path);
    let cfg = Config::load().expect("load config");
    assert_eq!(cfg.poll_interval, Duration::from_secs(1));

    std::env::remove_var("OJO_MYSQL_CONFIG");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn load_yaml_config_file_errors_for_directory_and_invalid_yaml() {
    let dir = unique_temp_path("mysql-config-dir");
    fs::create_dir_all(&dir).expect("mkdir");
    let dir_err = load_yaml_config_file(dir.to_string_lossy().as_ref()).unwrap_err();
    assert!(
        dir_err.to_string().contains("failed to read config file"),
        "{dir_err}"
    );
    fs::remove_dir_all(&dir).expect("cleanup dir");

    let invalid = unique_temp_path("mysql-invalid-config.yaml");
    fs::write(&invalid, "service: [\n").expect("write invalid");
    let parse_err = load_yaml_config_file(invalid.to_string_lossy().as_ref()).unwrap_err();
    assert!(
        parse_err.to_string().contains("failed to parse YAML"),
        "{parse_err}"
    );
    fs::remove_file(&invalid).expect("cleanup invalid");
}

#[test]
fn record_snapshot_handles_unavailable_and_available_samples() {
    let meter = opentelemetry::global::meter("mysql-test-meter");
    let instruments = Instruments::new(&meter);
    let filter = PrefixFilter::new(vec!["system.mysql.".to_string()], vec![]);

    let unavailable = MysqlSnapshot::default();
    record_snapshot(&instruments, &filter, &unavailable, &MysqlRates::default());

    let available = MysqlSnapshot {
        available: true,
        up: true,
        connections: 3,
        threads_running: 2,
        queries_total: 100,
        slow_queries_total: 4,
        bytes_received_total: 500,
        bytes_sent_total: 800,
    };
    let rates = MysqlRates {
        queries_per_second: 10.0,
        bytes_received_per_second: 50.0,
        bytes_sent_per_second: 80.0,
    };
    record_snapshot(&instruments, &filter, &available, &rates);
}

#[test]
fn record_helpers_cover_allow_and_block_paths() {
    let meter = opentelemetry::global::meter("mysql-record-helpers");
    let gauge_u64 = meter.u64_gauge("mysql.test.u64").build();
    let gauge_f64 = meter.f64_gauge("mysql.test.f64").build();

    let filter_allow = PrefixFilter::new(vec!["system.mysql.".to_string()], vec![]);
    record_u64(&gauge_u64, &filter_allow, "system.mysql.test.u64", 1);
    record_f64(&gauge_f64, &filter_allow, "system.mysql.test.f64", 1.0);

    let filter_block = PrefixFilter::new(vec!["system.unrelated.".to_string()], vec![]);
    record_u64(&gauge_u64, &filter_block, "system.mysql.test.u64", 2);
    record_f64(&gauge_f64, &filter_block, "system.mysql.test.f64", 2.0);
}

#[cfg(target_os = "linux")]
#[test]
fn linux_platform_collect_snapshot_wrapper_is_callable() {
    let cfg = super::MysqlConfig {
        executable: "/definitely/missing/mysql".to_string(),
        ..super::MysqlConfig::default()
    };
    let snap = super::platform::collect_snapshot(&cfg);
    assert!(!snap.available);
}

#[test]
fn main_runs_once_with_temp_config() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("mysql-main-once.yaml");
    fs::write(
            &path,
            "service:\n  name: mysql-main-test\n  instance_id: mysql-main-01\ncollection:\n  poll_interval_secs: 1\nmysql:\n  executable: /definitely/missing/mysql\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
        )
        .expect("write config");

    std::env::set_var("OJO_MYSQL_CONFIG", &path);
    std::env::set_var("OJO_RUN_ONCE", "1");
    let result = super::run();
    assert!(result.is_ok(), "{result:?}");
    std::env::remove_var("OJO_MYSQL_CONFIG");
    std::env::remove_var("OJO_RUN_ONCE");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn run_supports_test_iteration_cap_when_once_is_false() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("mysql-main-iter-cap.yaml");
    fs::write(
        &path,
        "service:\n  name: mysql-main-test\n  instance_id: mysql-main-01\ncollection:\n  poll_interval_secs: 1\nmysql:\n  executable: /definitely/missing/mysql\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
    )
    .expect("write config");

    std::env::set_var("OJO_MYSQL_CONFIG", &path);
    std::env::remove_var("OJO_RUN_ONCE");
    std::env::set_var("OJO_TEST_MAX_ITERATIONS", "1");
    std::env::set_var("OJO_TEST_SKIP_SIGNAL_HANDLER", "1");

    let result = super::run();
    assert!(result.is_ok(), "{result:?}");

    std::env::remove_var("OJO_MYSQL_CONFIG");
    std::env::remove_var("OJO_TEST_MAX_ITERATIONS");
    std::env::remove_var("OJO_TEST_SKIP_SIGNAL_HANDLER");
    fs::remove_file(&path).expect("cleanup config");
}
