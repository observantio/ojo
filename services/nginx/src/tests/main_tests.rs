use crate::{
    derive_rates_or_reset, load_yaml_config_file, parse_bool_env, record_f64, record_snapshot,
    record_u64, resolve_default_config_path, run, saturating_rate, Config, Instruments,
    NginxConfig, NginxRates, NginxSnapshot, PrevState,
};
use host_collectors::PrefixFilter;
use std::fs;
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
fn saturating_rate_and_prev_state_cover_resets() {
    assert_eq!(saturating_rate(10, 5, 1.0), 0.0);
    assert_eq!(saturating_rate(10, 20, 2.0), 5.0);

    let mut prev = PrevState::default();
    let first = NginxSnapshot {
        available: true,
        accepts_total: 10,
        requests_total: 20,
        ..NginxSnapshot::default()
    };
    let rates = prev.derive(&first);
    assert_eq!(rates.accepts_per_second, 0.0);
    assert_eq!(rates.requests_per_second, 0.0);

    prev.last = Some((first, Instant::now() + Duration::from_secs(1)));
    let zero_elapsed = prev.derive(&NginxSnapshot {
        available: true,
        accepts_total: 15,
        requests_total: 40,
        ..NginxSnapshot::default()
    });
    assert_eq!(zero_elapsed.accepts_per_second, 0.0);
    assert_eq!(zero_elapsed.requests_per_second, 0.0);

    let unavailable = NginxSnapshot::default();
    let reset = derive_rates_or_reset(&mut prev, &unavailable);
    assert_eq!(reset.accepts_per_second, 0.0);
    assert_eq!(reset.requests_per_second, 0.0);
    assert!(prev.last.is_none());

    prev.last = Some((
        NginxSnapshot {
            available: true,
            accepts_total: 10,
            requests_total: 20,
            ..NginxSnapshot::default()
        },
        Instant::now() - Duration::from_secs(1),
    ));
    let available = NginxSnapshot {
        available: true,
        accepts_total: 20,
        requests_total: 40,
        ..NginxSnapshot::default()
    };
    let rates = derive_rates_or_reset(&mut prev, &available);
    assert!(rates.accepts_per_second > 0.0);
    assert!(rates.requests_per_second > 0.0);
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
        "service:\n  name: ojo-nginx\ncollection:\n  poll_interval_secs: 1\nnginx:\n  executable: curl\n  status_url: http://127.0.0.1/nginx_status\n",
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
        "service:\n  name: ojo-nginx-test\ncollection:\n  poll_interval_secs: 1\nnginx:\n  executable: curl\n  status_url: http://127.0.0.1/nginx_status\n",
    )
    .expect("write config");
    std::env::set_var("OJO_NGINX_CONFIG", &path);

    let args = vec!["ojo-nginx".to_string()];
    let cfg = Config::load_from_args(&args).expect("load config");
    assert_eq!(cfg.service_name, "ojo-nginx-test");
    assert_eq!(cfg.poll_interval, Duration::from_secs(1));
    assert_eq!(cfg.nginx.executable, "curl");

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
fn record_snapshot_covers_available_and_unavailable_paths() {
    let meter = opentelemetry::global::meter("nginx-record-tests");
    let instruments = Instruments::new(&meter);
    let filter = PrefixFilter::new(vec!["system.nginx.".to_string()], vec![]);

    record_snapshot(
        &instruments,
        &filter,
        &NginxSnapshot::default(),
        &NginxRates::default(),
    );

    let snapshot = NginxSnapshot {
        available: true,
        up: true,
        connections_active: 10,
        connections_reading: 1,
        connections_writing: 2,
        connections_waiting: 7,
        accepts_total: 100,
        handled_total: 100,
        requests_total: 1000,
    };
    let rates = NginxRates {
        accepts_per_second: 10.0,
        requests_per_second: 25.0,
    };
    record_snapshot(&instruments, &filter, &snapshot, &rates);
}

#[test]
fn record_f64_covers_allow_and_block_paths() {
    let meter = opentelemetry::global::meter("nginx-tests-f64");
    let gauge = meter.f64_gauge("nginx.test.value.f64").build();
    let allow = PrefixFilter::new(vec!["system.nginx.".to_string()], vec![]);
    record_f64(&gauge, &allow, "system.nginx.requests.rate_per_second", 1.0);
    let block = PrefixFilter::new(vec!["system.other.".to_string()], vec![]);
    record_f64(&gauge, &block, "system.nginx.requests.rate_per_second", 2.0);
}

#[test]
fn run_once_with_temp_config() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("nginx-run.yaml");
    fs::write(
        &path,
        "service:\n  name: ojo-nginx-test\ncollection:\n  poll_interval_secs: 1\nnginx:\n  executable: curl\n  status_url: http://127.0.0.1/nginx_status\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
    )
    .expect("write config");

    std::env::set_var("OJO_NGINX_CONFIG", &path);
    std::env::set_var("OJO_RUN_ONCE", "1");
    std::env::set_var("OJO_TEST_MAX_ITERATIONS", "2");
    let fake_status = "Active connections: 2\nserver accepts handled requests\n 10 10 20\nReading: 1 Writing: 0 Waiting: 1\n";
    let old = std::env::var("OJO_NGINX_STUB_STATUS").ok();
    std::env::set_var("OJO_NGINX_STUB_STATUS", fake_status);
    let result = run();
    assert!(result.is_ok(), "{result:?}");

    std::env::remove_var("OJO_NGINX_CONFIG");
    std::env::remove_var("OJO_RUN_ONCE");
    std::env::remove_var("OJO_TEST_MAX_ITERATIONS");
    if let Some(previous) = old {
        std::env::set_var("OJO_NGINX_STUB_STATUS", previous);
    } else {
        std::env::remove_var("OJO_NGINX_STUB_STATUS");
    }
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn run_supports_test_iteration_cap_when_once_is_false() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("nginx-loop.yaml");
    fs::write(
        &path,
        "service:\n  name: ojo-nginx-test\ncollection:\n  poll_interval_secs: 1\nnginx:\n  executable: curl\n  status_url: http://127.0.0.1/nginx_status\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
    )
    .expect("write config");

    std::env::set_var("OJO_NGINX_CONFIG", &path);
    std::env::remove_var("OJO_RUN_ONCE");
    std::env::set_var("OJO_TEST_MAX_ITERATIONS", "2");
    let fake_status = "Active connections: 3\nserver accepts handled requests\n 11 11 22\nReading: 1 Writing: 1 Waiting: 1\n";
    let old = std::env::var("OJO_NGINX_STUB_STATUS").ok();
    std::env::set_var("OJO_NGINX_STUB_STATUS", fake_status);

    let result = run();
    assert!(result.is_ok(), "{result:?}");

    std::env::remove_var("OJO_NGINX_CONFIG");
    std::env::remove_var("OJO_TEST_MAX_ITERATIONS");
    if let Some(previous) = old {
        std::env::set_var("OJO_NGINX_STUB_STATUS", previous);
    } else {
        std::env::remove_var("OJO_NGINX_STUB_STATUS");
    }
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn config_shape_covers_all_fields() {
    let _cfg_shape = Config {
        service_name: "svc".to_string(),
        instance_id: "inst".to_string(),
        poll_interval: Duration::from_secs(1),
        otlp_endpoint: "http://127.0.0.1:4318/v1/metrics".to_string(),
        otlp_protocol: "http/protobuf".to_string(),
        otlp_headers: std::collections::BTreeMap::new(),
        otlp_compression: None,
        otlp_timeout: None,
        export_interval: None,
        export_timeout: None,
        metrics_include: vec![],
        metrics_exclude: vec![],
        nginx: NginxConfig {
            executable: "curl".to_string(),
            status_url: "http://127.0.0.1/nginx_status".to_string(),
        },
        once: true,
    };
}
