use super::{
    collect_nginx_up, load_yaml_config_file, parse_bool_env, record_u64,
    resolve_default_config_path, run, Config,
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
    std::env::temp_dir().join(format!("ojo-nginx-{name}-{}-{nanos}", std::process::id()))
}

#[test]
fn parse_bool_env_covers_variants() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::set_var("OJO_NGINX_BOOL", "yes");
    assert_eq!(parse_bool_env("OJO_NGINX_BOOL"), Some(true));
    std::env::set_var("OJO_NGINX_BOOL", "off");
    assert_eq!(parse_bool_env("OJO_NGINX_BOOL"), Some(false));
    std::env::set_var("OJO_NGINX_BOOL", "maybe");
    assert_eq!(parse_bool_env("OJO_NGINX_BOOL"), None);
    std::env::remove_var("OJO_NGINX_BOOL");
}

#[test]
fn collect_nginx_up_respects_env() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::set_var("OJO_NGINX_SIMULATE_UP", "1");
    assert_eq!(collect_nginx_up(), 1);
    std::env::set_var("OJO_NGINX_SIMULATE_UP", "0");
    assert_eq!(collect_nginx_up(), 0);
    std::env::remove_var("OJO_NGINX_SIMULATE_UP");
}

#[test]
fn resolve_default_config_path_prefers_existing_local() {
    let local = unique_temp_path("nginx-local.yaml");
    fs::write(&local, "service: {}\n").expect("write local");
    let selected = resolve_default_config_path(local.to_string_lossy().as_ref(), "fallback.yaml");
    assert_eq!(selected, local.to_string_lossy());
    fs::remove_file(&local).expect("cleanup local");

    let selected = resolve_default_config_path("/definitely/missing/nginx.yaml", "fallback.yaml");
    assert_eq!(selected, "fallback.yaml");
}

#[test]
fn load_yaml_config_file_covers_missing_empty_invalid_and_valid() {
    let missing = unique_temp_path("nginx-missing.yaml");
    let err = load_yaml_config_file(missing.to_string_lossy().as_ref()).unwrap_err();
    assert!(err.to_string().contains("was not found"), "{err}");

    let empty = unique_temp_path("nginx-empty.yaml");
    fs::write(&empty, " \n").expect("write empty");
    let err = load_yaml_config_file(empty.to_string_lossy().as_ref()).unwrap_err();
    assert!(err.to_string().contains("is empty"), "{err}");
    fs::remove_file(&empty).expect("cleanup empty");

    let invalid = unique_temp_path("nginx-invalid.yaml");
    fs::write(&invalid, "service: [\n").expect("write invalid");
    let err = load_yaml_config_file(invalid.to_string_lossy().as_ref()).unwrap_err();
    assert!(err.to_string().contains("failed to parse YAML"), "{err}");
    fs::remove_file(&invalid).expect("cleanup invalid");

    let valid = unique_temp_path("nginx-valid.yaml");
    fs::write(
        &valid,
        "service:\n  name: ojo-nginx\ncollection:\n  poll_interval_secs: 1\n",
    )
    .expect("write valid");
    assert!(load_yaml_config_file(valid.to_string_lossy().as_ref()).is_ok());
    fs::remove_file(&valid).expect("cleanup valid");
}

#[test]
fn config_load_from_args_reads_env_config() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("nginx-config.yaml");
    fs::write(
        &path,
        "service:\n  name: ojo-nginx-test\ncollection:\n  poll_interval_secs: 1\n",
    )
    .expect("write config");
    std::env::set_var("OJO_NGINX_CONFIG", &path);

    let args = vec!["ojo-nginx".to_string()];
    let cfg = Config::load_from_args(&args).expect("load config");
    assert_eq!(cfg.service_name, "ojo-nginx-test");
    assert_eq!(cfg.poll_interval, Duration::from_secs(1));

    std::env::remove_var("OJO_NGINX_CONFIG");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn config_load_from_args_uses_repo_default_when_env_not_set() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::remove_var("OJO_NGINX_CONFIG");
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");

    let args = vec!["ojo-nginx".to_string()];
    let cfg = Config::load_from_args(&args).expect("load default config");
    assert!(!cfg.service_name.is_empty());
}

#[test]
fn record_u64_covers_allow_and_block_paths() {
    let meter = opentelemetry::global::meter("nginx-tests");
    let gauge = meter.u64_gauge("nginx.test.value").build();
    let allow = PrefixFilter::new(vec!["system.nginx.".to_string()], vec![]);
    record_u64(&gauge, &allow, "system.nginx.up", 1);
    let block = PrefixFilter::new(vec!["system.other.".to_string()], vec![]);
    record_u64(&gauge, &block, "system.nginx.up", 2);
}

#[test]
fn run_once_with_temp_config() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("nginx-run.yaml");
    fs::write(
        &path,
        "service:\n  name: ojo-nginx-test\ncollection:\n  poll_interval_secs: 1\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
    )
    .expect("write config");

    std::env::set_var("OJO_NGINX_CONFIG", &path);
    std::env::set_var("OJO_RUN_ONCE", "1");
    std::env::set_var("OJO_TEST_MAX_ITERATIONS", "2");
    std::env::set_var("OJO_NGINX_SIMULATE_UP", "1");
    let result = run();
    assert!(result.is_ok(), "{result:?}");

    std::env::remove_var("OJO_NGINX_CONFIG");
    std::env::remove_var("OJO_RUN_ONCE");
    std::env::remove_var("OJO_TEST_MAX_ITERATIONS");
    std::env::remove_var("OJO_NGINX_SIMULATE_UP");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn run_supports_test_iteration_cap_when_once_is_false() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("nginx-loop.yaml");
    fs::write(
        &path,
        "service:\n  name: ojo-nginx-test\ncollection:\n  poll_interval_secs: 1\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
    )
    .expect("write config");

    std::env::set_var("OJO_NGINX_CONFIG", &path);
    std::env::remove_var("OJO_RUN_ONCE");
    std::env::set_var("OJO_TEST_MAX_ITERATIONS", "2");
    std::env::set_var("OJO_NGINX_SIMULATE_UP", "1");

    let result = run();
    assert!(result.is_ok(), "{result:?}");

    std::env::remove_var("OJO_NGINX_CONFIG");
    std::env::remove_var("OJO_TEST_MAX_ITERATIONS");
    std::env::remove_var("OJO_NGINX_SIMULATE_UP");
    fs::remove_file(&path).expect("cleanup config");
}
