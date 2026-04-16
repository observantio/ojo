use crate::{
    advance_export_state, derive_rates_or_reset, load_yaml_config_file, record_f64,
    record_snapshot, record_u64, resolve_default_config_path, saturating_rate, sleep_until,
    update_postgres_connection_state, Config, ExportState, FlushEvent, Instruments,
    PostgresConnectionState, PostgresRates, PostgresSnapshot, PrevState,
};
use host_collectors::PrefixFilter;
use std::fs;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::{Mutex, OnceLock};
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
        "ojo-postgres-{name}-{}-{nanos}",
        std::process::id()
    ))
}

#[test]
fn saturating_rate_handles_counter_reset_and_normal_rate() {
    assert_eq!(saturating_rate(20, 10, 1.0), 0.0);
    assert_eq!(saturating_rate(10, 30, 2.0), 10.0);
}

#[test]
fn prev_state_derive_initializes_and_then_computes_rates() {
    let mut state = PrevState::default();
    let first = PostgresSnapshot {
        xact_commit_total: 100,
        xact_rollback_total: 20,
        ..PostgresSnapshot::default()
    };
    let rates = state.derive(&first);
    assert_eq!(rates.commits_per_second, 0.0);
    assert_eq!(rates.rollbacks_per_second, 0.0);

    state.last = Some((first, Instant::now() - Duration::from_secs(2)));
    let second = PostgresSnapshot {
        xact_commit_total: 180,
        xact_rollback_total: 30,
        ..PostgresSnapshot::default()
    };
    let rates = state.derive(&second);
    assert!(rates.commits_per_second > 30.0);
    assert!(rates.rollbacks_per_second > 4.0);
}

#[test]
fn prev_state_derive_resets_on_non_progressing_time() {
    let mut state = PrevState {
        last: Some((
            PostgresSnapshot {
                xact_commit_total: 100,
                xact_rollback_total: 10,
                ..PostgresSnapshot::default()
            },
            Instant::now() + Duration::from_secs(1),
        )),
    };
    let rates = state.derive(&PostgresSnapshot {
        xact_commit_total: 120,
        xact_rollback_total: 12,
        ..PostgresSnapshot::default()
    });
    assert_eq!(rates.commits_per_second, 0.0);
    assert_eq!(rates.rollbacks_per_second, 0.0);
}

#[test]
fn derive_rates_or_reset_resets_state_when_snapshot_unavailable() {
    let mut state = PrevState::default();
    let available = PostgresSnapshot {
        available: true,
        xact_commit_total: 10,
        xact_rollback_total: 2,
        ..PostgresSnapshot::default()
    };
    let _ = derive_rates_or_reset(&mut state, &available);
    assert!(state.last.is_some());

    let unavailable = PostgresSnapshot::default();
    let rates = derive_rates_or_reset(&mut state, &unavailable);
    assert_eq!(rates.commits_per_second, 0.0);
    assert_eq!(rates.rollbacks_per_second, 0.0);
    assert!(state.last.is_none());
}

#[test]
fn postgres_connection_state_transitions_cover_all_paths() {
    let up = PostgresSnapshot {
        available: true,
        up: true,
        ..PostgresSnapshot::default()
    };
    let down = PostgresSnapshot::default();

    assert_eq!(
        update_postgres_connection_state(PostgresConnectionState::Unknown, &up),
        PostgresConnectionState::Connected
    );
    assert_eq!(
        update_postgres_connection_state(PostgresConnectionState::Unknown, &down),
        PostgresConnectionState::Disconnected
    );
    assert_eq!(
        update_postgres_connection_state(PostgresConnectionState::Disconnected, &up),
        PostgresConnectionState::Connected
    );
    assert_eq!(
        update_postgres_connection_state(PostgresConnectionState::Disconnected, &down),
        PostgresConnectionState::Disconnected
    );
    assert_eq!(
        update_postgres_connection_state(PostgresConnectionState::Connected, &up),
        PostgresConnectionState::Connected
    );
    assert_eq!(
        update_postgres_connection_state(PostgresConnectionState::Connected, &down),
        PostgresConnectionState::Disconnected
    );
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
    let local = unique_temp_path("postgres-local.yaml");
    fs::write(&local, "service: {}\n").expect("write local");
    let chosen = resolve_default_config_path(local.to_string_lossy().as_ref(), "fallback.yaml");
    assert_eq!(chosen, local.to_string_lossy());
    fs::remove_file(&local).expect("cleanup local");

    let missing = unique_temp_path("postgres-missing.yaml");
    let chosen = resolve_default_config_path(missing.to_string_lossy().as_ref(), "fallback.yaml");
    assert_eq!(chosen, "fallback.yaml");
}

#[test]
fn load_yaml_config_file_handles_missing_empty_and_valid_yaml() {
    let missing = unique_temp_path("postgres-missing-config.yaml");
    let missing_err = load_yaml_config_file(missing.to_string_lossy().as_ref()).unwrap_err();
    assert!(
        missing_err.to_string().contains("was not found"),
        "{missing_err}"
    );

    let empty = unique_temp_path("postgres-empty-config.yaml");
    fs::write(&empty, " \n").expect("write empty");
    let empty_err = load_yaml_config_file(empty.to_string_lossy().as_ref()).unwrap_err();
    assert!(empty_err.to_string().contains("is empty"), "{empty_err}");
    fs::remove_file(&empty).expect("cleanup empty");

    let valid = unique_temp_path("postgres-valid-config.yaml");
    fs::write(
            &valid,
            "service:\n  name: ojo-postgres\n  instance_id: pg-1\ncollection:\n  poll_interval_secs: 2\npostgres:\n  executable: psql\n",
        )
        .expect("write valid");
    let parsed = load_yaml_config_file(valid.to_string_lossy().as_ref());
    assert!(parsed.is_ok(), "{parsed:?}");
    fs::remove_file(&valid).expect("cleanup valid");
}

#[test]
fn load_yaml_config_file_covers_read_and_parse_errors() {
    let dir = unique_temp_path("postgres-config-dir");
    fs::create_dir_all(&dir).expect("mkdir");
    let dir_err = load_yaml_config_file(dir.to_string_lossy().as_ref()).unwrap_err();
    assert!(
        dir_err.to_string().contains("failed to read config file"),
        "{dir_err}"
    );
    fs::remove_dir_all(&dir).expect("cleanup dir");

    let invalid = unique_temp_path("postgres-invalid-config.yaml");
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
    let path = unique_temp_path("postgres-load.yaml");
    fs::write(
            &path,
            "service:\n  name: pg-svc\n  instance_id: pg-01\ncollection:\n  poll_interval_secs: 1\npostgres:\n  executable: psql\n  uri: '  '\n",
        )
        .expect("write config");

    std::env::set_var("OJO_POSTGRES_CONFIG", &path);
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");

    let cfg = Config::load().expect("load config");
    assert_eq!(cfg.service_name, "pg-svc");
    assert_eq!(cfg.instance_id, "pg-01");
    assert_eq!(cfg.poll_interval, Duration::from_secs(1));
    assert_eq!(cfg.otlp_endpoint, "http://127.0.0.1:4318/v1/metrics");
    assert_eq!(cfg.otlp_protocol, "http/protobuf");
    assert_eq!(cfg.metrics_include, vec!["system.postgres.".to_string()]);
    assert!(cfg.postgres.uri.is_none());
    assert_eq!(cfg.postgres.executable, "psql");

    std::env::remove_var("OJO_POSTGRES_CONFIG");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn config_load_uses_otlp_env_fallback_when_export_section_missing() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("postgres-env-load.yaml");
    fs::write(
            &path,
            "service:\n  name: pg-svc\n  instance_id: pg-01\ncollection:\n  poll_interval_secs: 2\npostgres:\n  executable: psql\n",
        )
        .expect("write config");

    std::env::set_var("OJO_POSTGRES_CONFIG", &path);
    std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4317");
    std::env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc");

    let cfg = Config::load().expect("load config");
    assert_eq!(cfg.otlp_endpoint, "http://127.0.0.1:4317");
    assert_eq!(cfg.otlp_protocol, "grpc");

    std::env::remove_var("OJO_POSTGRES_CONFIG");
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn config_load_from_args_covers_default_and_missing_config_error() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::remove_var("OJO_POSTGRES_CONFIG");
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");

    let default_path = unique_temp_path("postgres-default-from-args.yaml");
    fs::write(
            &default_path,
            "service:\n  name: pg-default\n  instance_id: pg-default-01\ncollection:\n  poll_interval_secs: 1\n",
        )
        .expect("write default config");
    std::env::set_var("OJO_POSTGRES_CONFIG", &default_path);

    let args = vec!["ojo-postgres".to_string()];
    let cfg = Config::load_from_args(&args).expect("load default config");
    assert_eq!(cfg.service_name, "pg-default");

    std::env::remove_var("OJO_POSTGRES_CONFIG");
    fs::remove_file(&default_path).expect("cleanup default config");

    let missing = unique_temp_path("postgres-missing-from-args.yaml");
    let args = vec![
        "ojo-postgres".to_string(),
        "--config".to_string(),
        missing.to_string_lossy().to_string(),
    ];
    let err = Config::load_from_args(&args).unwrap_err();
    assert!(err.to_string().contains("was not found"), "{err}");

    let path = unique_temp_path("postgres-run-once-values.yaml");
    fs::write(
            &path,
            "service:\n  name: pg-once\n  instance_id: pg-once-01\ncollection:\n  poll_interval_secs: 1\n",
        )
        .expect("write run-once config");
    let args = vec![
        "ojo-postgres".to_string(),
        "--config".to_string(),
        path.to_string_lossy().to_string(),
    ];
    for value in ["true", "yes", "on"] {
        std::env::set_var("OJO_RUN_ONCE", value);
        let cfg = Config::load_from_args(&args).expect("load run-once config");
        assert!(cfg.once, "expected once=true for value={value}");
    }
    std::env::remove_var("OJO_RUN_ONCE");
    fs::remove_file(&path).expect("cleanup run-once config");
}

#[test]
fn config_load_reads_from_environment_config_path() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("postgres-load-direct.yaml");
    fs::write(&path, "collection:\n  poll_interval_secs: 1\n").expect("write config");

    std::env::set_var("OJO_POSTGRES_CONFIG", &path);
    let cfg = Config::load().expect("load config");
    assert_eq!(cfg.poll_interval, Duration::from_secs(1));

    std::env::remove_var("OJO_POSTGRES_CONFIG");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn config_load_from_args_uses_repo_default_when_env_not_set() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::remove_var("OJO_POSTGRES_CONFIG");
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");

    let args = vec!["ojo-postgres".to_string()];
    let cfg = Config::load_from_args(&args).expect("load default config path");
    assert!(!cfg.service_name.is_empty());
}

#[test]
fn flush_and_sleep_helpers_cover_paths() {
    let now = Instant::now();
    super::log_flush_result(now, true);
    super::log_flush_result(now, false);

    super::handle_flush_event(FlushEvent::Connected, None);
    super::handle_flush_event(FlushEvent::Reconnected, None);
    super::handle_flush_event(FlushEvent::None, None);
    super::handle_flush_event(FlushEvent::Reconnecting, Some(&"err"));
    super::handle_flush_event(FlushEvent::StillUnavailable, Some(&"err"));
    super::handle_flush_event(FlushEvent::Connected, Some(&"err"));

    let running = AtomicBool::new(false);
    sleep_until(
        Instant::now() + Duration::from_millis(2),
        &running,
        Duration::from_millis(1),
    );

    let running = AtomicBool::new(true);
    super::maybe_sleep_until_next_poll(false, Instant::now(), Duration::from_millis(2), &running);
    super::maybe_sleep_until_next_poll(true, Instant::now(), Duration::from_secs(1), &running);
}

#[test]
fn stop_handler_and_signal_install_are_safe() {
    let running = Arc::new(AtomicBool::new(true));
    let stop = super::make_stop_handler(Arc::clone(&running));
    stop();
    assert!(!running.load(Ordering::SeqCst));

    super::install_signal_handler(&running);
    super::install_signal_handler(&running);
}

#[test]
fn record_snapshot_handles_unavailable_and_available_samples() {
    let meter = opentelemetry::global::meter("postgres-test-meter");
    let instruments = Instruments::new(&meter);
    let filter = PrefixFilter::new(vec!["system.postgres.".to_string()], vec![]);

    let unavailable = PostgresSnapshot::default();
    record_snapshot(
        &instruments,
        &filter,
        &unavailable,
        &PostgresRates::default(),
    );

    let available = PostgresSnapshot {
        available: true,
        up: true,
        connections: 5,
        xact_commit_total: 100,
        xact_rollback_total: 3,
        deadlocks_total: 1,
        blks_read_total: 20,
        blks_hit_total: 90,
    };
    let rates = PostgresRates {
        commits_per_second: 8.0,
        rollbacks_per_second: 0.2,
    };
    record_snapshot(&instruments, &filter, &available, &rates);
}

#[test]
fn record_helpers_cover_allow_and_block_paths() {
    let meter = opentelemetry::global::meter("postgres-record-helpers");
    let gauge_u64 = meter.u64_gauge("postgres.test.u64").build();
    let gauge_f64 = meter.f64_gauge("postgres.test.f64").build();

    let filter_allow = PrefixFilter::new(vec!["system.postgres.".to_string()], vec![]);
    record_u64(&gauge_u64, &filter_allow, "system.postgres.test.u64", 1);
    record_f64(&gauge_f64, &filter_allow, "system.postgres.test.f64", 1.0);

    let filter_block = PrefixFilter::new(vec!["system.unrelated.".to_string()], vec![]);
    record_u64(&gauge_u64, &filter_block, "system.postgres.test.u64", 2);
    record_f64(&gauge_f64, &filter_block, "system.postgres.test.f64", 2.0);
}

#[cfg(target_os = "linux")]
#[test]
fn linux_platform_collect_snapshot_wrapper_is_callable() {
    let cfg = super::PostgresConfig {
        executable: "/definitely/missing/psql".to_string(),
        ..super::PostgresConfig::default()
    };
    let snap = super::platform::collect_snapshot(&cfg);
    assert!(!snap.available);
}

#[test]
fn main_runs_once_with_temp_config() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("postgres-main-once.yaml");
    fs::write(
            &path,
            "service:\n  name: postgres-main-test\n  instance_id: postgres-main-01\ncollection:\n  poll_interval_secs: 1\npostgres:\n  executable: /definitely/missing/psql\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
        )
        .expect("write config");

    std::env::set_var("OJO_POSTGRES_CONFIG", &path);
    std::env::set_var("OJO_RUN_ONCE", "1");
    let result = super::run();
    assert!(result.is_ok(), "{result:?}");
    std::env::remove_var("OJO_POSTGRES_CONFIG");
    std::env::remove_var("OJO_RUN_ONCE");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn run_returns_error_for_missing_config() {
    let _guard = env_lock().lock().expect("env lock");
    let missing = unique_temp_path("postgres-run-missing.yaml");
    std::env::set_var("OJO_POSTGRES_CONFIG", &missing);
    std::env::set_var("OJO_RUN_ONCE", "1");
    let result = super::run();
    assert!(result.is_err(), "{result:?}");
    std::env::remove_var("OJO_POSTGRES_CONFIG");
    std::env::remove_var("OJO_RUN_ONCE");
}

#[test]
fn run_returns_error_for_invalid_otlp_protocol() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("postgres-main-invalid-protocol.yaml");
    fs::write(
            &path,
            "service:\n  name: postgres-main-test\n  instance_id: postgres-main-01\ncollection:\n  poll_interval_secs: 1\npostgres:\n  executable: /definitely/missing/psql\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: unsupported-protocol\n",
        )
        .expect("write config");

    std::env::set_var("OJO_POSTGRES_CONFIG", &path);
    std::env::set_var("OJO_RUN_ONCE", "1");
    let result = super::run();
    assert!(result.is_err(), "{result:?}");
    std::env::remove_var("OJO_POSTGRES_CONFIG");
    std::env::remove_var("OJO_RUN_ONCE");
    fs::remove_file(&path).expect("cleanup config");
}
