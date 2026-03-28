use crate::{
    collect_snapshot, load_yaml_config_file, parse_bool_env, parse_u64_env, ratio, record_snapshot,
    record_u64, resolve_default_config_path, run, simulated_snapshot_from_env, snapshot_up, Config,
    Instruments, SystemdSnapshot,
};
use host_collectors::PrefixFilter;
use std::fs;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn unique_temp_path(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("ojo-systemd-{name}-{}-{nanos}", std::process::id()))
}

#[test]
fn parse_bool_env_covers_variants() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::set_var("OJO_SYSTEMD_BOOL", "yes");
    assert_eq!(parse_bool_env("OJO_SYSTEMD_BOOL"), Some(true));
    std::env::set_var("OJO_SYSTEMD_BOOL", "off");
    assert_eq!(parse_bool_env("OJO_SYSTEMD_BOOL"), Some(false));
    std::env::set_var("OJO_SYSTEMD_BOOL", "maybe");
    assert_eq!(parse_bool_env("OJO_SYSTEMD_BOOL"), None);
    std::env::remove_var("OJO_SYSTEMD_BOOL");
}

#[test]
fn parse_u64_env_handles_valid_and_invalid_values() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::set_var("OJO_SYSTEMD_NUM", "42");
    assert_eq!(parse_u64_env("OJO_SYSTEMD_NUM"), Some(42));
    std::env::set_var("OJO_SYSTEMD_NUM", "x");
    assert_eq!(parse_u64_env("OJO_SYSTEMD_NUM"), None);
    std::env::remove_var("OJO_SYSTEMD_NUM");
}

#[test]
fn simulated_snapshot_from_env_covers_enabled_and_disabled_paths() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::remove_var("OJO_SYSTEMD_SIMULATE_UP");
    assert!(simulated_snapshot_from_env().is_none());

    std::env::set_var("OJO_SYSTEMD_SIMULATE_UP", "1");
    std::env::set_var("OJO_SYSTEMD_SIMULATE_UNITS_TOTAL", "20");
    std::env::set_var("OJO_SYSTEMD_SIMULATE_UNITS_ACTIVE", "15");
    std::env::set_var("OJO_SYSTEMD_SIMULATE_UNITS_FAILED", "2");
    let snapshot = simulated_snapshot_from_env().expect("snapshot from env");
    assert!(snapshot.available);
    assert_eq!(snapshot.units_total, 20);
    assert_eq!(snapshot.units_active, 15);
    assert_eq!(snapshot.units_failed, 2);

    std::env::remove_var("OJO_SYSTEMD_SIMULATE_UP");
    std::env::remove_var("OJO_SYSTEMD_SIMULATE_UNITS_TOTAL");
    std::env::remove_var("OJO_SYSTEMD_SIMULATE_UNITS_ACTIVE");
    std::env::remove_var("OJO_SYSTEMD_SIMULATE_UNITS_FAILED");
}

#[test]
fn ratio_and_snapshot_up_cover_core_derived_values() {
    assert_eq!(ratio(1, 0), 0.0);
    assert_eq!(ratio(3, 2), 1.5);
    assert_eq!(snapshot_up(&SystemdSnapshot::default()), 0);
    assert_eq!(
        snapshot_up(&SystemdSnapshot {
            available: true,
            ..SystemdSnapshot::default()
        }),
        1
    );
}

#[test]
fn collect_snapshot_uses_simulated_values_when_present() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::set_var("OJO_SYSTEMD_SIMULATE_UP", "1");
    std::env::set_var("OJO_SYSTEMD_SIMULATE_UNITS_TOTAL", "12");
    std::env::set_var("OJO_SYSTEMD_SIMULATE_UNITS_ACTIVE", "9");
    let snapshot = collect_snapshot();
    assert_eq!(snapshot.units_total, 12);
    assert_eq!(snapshot.units_active, 9);
    assert!(snapshot.available);
    std::env::remove_var("OJO_SYSTEMD_SIMULATE_UP");
    std::env::remove_var("OJO_SYSTEMD_SIMULATE_UNITS_TOTAL");
    std::env::remove_var("OJO_SYSTEMD_SIMULATE_UNITS_ACTIVE");
}

#[test]
fn collect_snapshot_can_use_platform_path_when_simulation_is_unset() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::remove_var("OJO_SYSTEMD_SIMULATE_UP");
    let snapshot = collect_snapshot();
    if snapshot.available {
        assert!(snapshot.units_total >= snapshot.units_active);
    } else {
        assert_eq!(snapshot.units_total, 0);
    }
}

#[test]
fn resolve_default_config_path_prefers_existing_local() {
    let local = unique_temp_path("systemd-local.yaml");
    fs::write(&local, "service: {}\n").expect("write local");
    let selected = resolve_default_config_path(local.to_string_lossy().as_ref(), "fallback.yaml");
    assert_eq!(selected, local.to_string_lossy());
    fs::remove_file(&local).expect("cleanup local");

    let selected = resolve_default_config_path("/definitely/missing/systemd.yaml", "fallback.yaml");
    assert_eq!(selected, "fallback.yaml");
}

#[test]
fn load_yaml_config_file_covers_missing_empty_invalid_and_valid() {
    let missing = unique_temp_path("systemd-missing.yaml");
    let err = load_yaml_config_file(missing.to_string_lossy().as_ref()).unwrap_err();
    assert!(err.to_string().contains("was not found"), "{err}");

    let empty = unique_temp_path("systemd-empty.yaml");
    fs::write(&empty, " \n").expect("write empty");
    let err = load_yaml_config_file(empty.to_string_lossy().as_ref()).unwrap_err();
    assert!(err.to_string().contains("is empty"), "{err}");
    fs::remove_file(&empty).expect("cleanup empty");

    let invalid = unique_temp_path("systemd-invalid.yaml");
    fs::write(&invalid, "service: [\n").expect("write invalid");
    let err = load_yaml_config_file(invalid.to_string_lossy().as_ref()).unwrap_err();
    assert!(err.to_string().contains("failed to parse YAML"), "{err}");
    fs::remove_file(&invalid).expect("cleanup invalid");

    let valid = unique_temp_path("systemd-valid.yaml");
    fs::write(
        &valid,
        "service:\n  name: ojo-systemd\ncollection:\n  poll_interval_secs: 1\n",
    )
    .expect("write valid");
    assert!(load_yaml_config_file(valid.to_string_lossy().as_ref()).is_ok());
    fs::remove_file(&valid).expect("cleanup valid");
}

#[test]
fn config_load_from_args_reads_env_config() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("systemd-config.yaml");
    fs::write(
        &path,
        "service:\n  name: ojo-systemd-test\ncollection:\n  poll_interval_secs: 1\n",
    )
    .expect("write config");
    std::env::set_var("OJO_SYSTEMD_CONFIG", &path);

    let args = vec!["ojo-systemd".to_string()];
    let cfg = Config::load_from_args(&args).expect("load config");
    assert_eq!(cfg.service_name, "ojo-systemd-test");
    assert_eq!(cfg.poll_interval, Duration::from_secs(1));

    std::env::remove_var("OJO_SYSTEMD_CONFIG");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn config_load_reads_current_process_args_with_env_config() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("systemd-config-load.yaml");
    fs::write(
        &path,
        "service:\n  name: ojo-systemd-load\ncollection:\n  poll_interval_secs: 1\n",
    )
    .expect("write config");
    std::env::set_var("OJO_SYSTEMD_CONFIG", &path);

    let cfg = Config::load().expect("load config");
    assert_eq!(cfg.service_name, "ojo-systemd-load");

    std::env::remove_var("OJO_SYSTEMD_CONFIG");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn config_load_from_args_uses_repo_default_when_env_not_set() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::remove_var("OJO_SYSTEMD_CONFIG");
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");

    let args = vec!["ojo-systemd".to_string()];
    let cfg = Config::load_from_args(&args).expect("load default config");
    assert!(!cfg.service_name.is_empty());
}

#[test]
fn record_u64_covers_allow_and_block_paths() {
    let meter = opentelemetry::global::meter("systemd-tests");
    let gauge = meter.u64_gauge("systemd.test.value").build();
    let allow = PrefixFilter::new(vec!["system.systemd.".to_string()], vec![]);
    record_u64(&gauge, &allow, "system.systemd.up", 1);
    let block = PrefixFilter::new(vec!["system.other.".to_string()], vec![]);
    record_u64(&gauge, &block, "system.systemd.up", 2);
}

#[test]
fn record_snapshot_emits_all_paths_without_panicking() {
    let meter = opentelemetry::global::meter("systemd-tests");
    let instruments = Instruments::new(&meter);
    let filter = PrefixFilter::new(vec!["system.systemd.".to_string()], vec![]);
    let snapshot = SystemdSnapshot {
        available: true,
        units_total: 10,
        units_active: 8,
        units_inactive: 1,
        units_failed: 1,
        units_activating: 0,
        units_deactivating: 0,
        units_reloading: 0,
        units_not_found: 0,
        units_maintenance: 0,
        jobs_queued: 2,
        jobs_running: 2,
        failed_units_reported: 1,
    };
    record_snapshot(&instruments, &filter, &snapshot);
}

#[test]
fn run_once_with_temp_config() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("systemd-run.yaml");
    fs::write(
        &path,
        "service:\n  name: ojo-systemd-test\ncollection:\n  poll_interval_secs: 1\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
    )
    .expect("write config");

    std::env::set_var("OJO_SYSTEMD_CONFIG", &path);
    std::env::set_var("OJO_RUN_ONCE", "1");
    std::env::set_var("OJO_TEST_MAX_ITERATIONS", "2");
    std::env::set_var("OJO_SYSTEMD_SIMULATE_UP", "1");
    let result = run();
    assert!(result.is_ok(), "{result:?}");

    std::env::remove_var("OJO_SYSTEMD_CONFIG");
    std::env::remove_var("OJO_RUN_ONCE");
    std::env::remove_var("OJO_TEST_MAX_ITERATIONS");
    std::env::remove_var("OJO_SYSTEMD_SIMULATE_UP");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn run_supports_test_iteration_cap_when_once_is_false() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("systemd-loop.yaml");
    fs::write(
        &path,
        "service:\n  name: ojo-systemd-test\ncollection:\n  poll_interval_secs: 1\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
    )
    .expect("write config");

    std::env::set_var("OJO_SYSTEMD_CONFIG", &path);
    std::env::remove_var("OJO_RUN_ONCE");
    std::env::set_var("OJO_TEST_MAX_ITERATIONS", "2");
    std::env::set_var("OJO_SYSTEMD_SIMULATE_UP", "1");

    let result = run();
    assert!(result.is_ok(), "{result:?}");

    std::env::remove_var("OJO_SYSTEMD_CONFIG");
    std::env::remove_var("OJO_TEST_MAX_ITERATIONS");
    std::env::remove_var("OJO_SYSTEMD_SIMULATE_UP");
    fs::remove_file(&path).expect("cleanup config");
}
