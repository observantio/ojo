use crate::{
    bool_as_u64, default_traces_endpoint, derive_trace_line_delta_us, infer_platform_component,
    infer_trace_line_component,
    load_yaml_config_file, parse_bool_env, resolve_default_config_path, Config, SystraceSnapshot,
};
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
    std::env::temp_dir().join(format!(
        "ojo-systrace-{name}-{}-{nanos}",
        std::process::id()
    ))
}

#[test]
fn bool_as_u64_maps_cleanly() {
    assert_eq!(bool_as_u64(true), 1);
    assert_eq!(bool_as_u64(false), 0);
}

#[test]
fn parse_bool_env_handles_supported_values() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::set_var("OJO_SYSTRACE_BOOL", "yes");
    assert_eq!(parse_bool_env("OJO_SYSTRACE_BOOL"), Some(true));
    std::env::set_var("OJO_SYSTRACE_BOOL", "off");
    assert_eq!(parse_bool_env("OJO_SYSTRACE_BOOL"), Some(false));
    std::env::set_var("OJO_SYSTRACE_BOOL", "invalid");
    assert_eq!(parse_bool_env("OJO_SYSTRACE_BOOL"), None);
    std::env::remove_var("OJO_SYSTRACE_BOOL");
}

#[test]
fn default_traces_endpoint_translates_metrics_suffix() {
    assert_eq!(
        default_traces_endpoint("http://127.0.0.1:4318/v1/metrics"),
        "http://127.0.0.1:4318/v1/traces"
    );
    assert_eq!(
        default_traces_endpoint("http://127.0.0.1:4317"),
        "http://127.0.0.1:4317"
    );
}

#[test]
fn resolve_default_config_path_prefers_existing_local() {
    let local = unique_temp_path("systrace-local.yaml");
    fs::write(&local, "service: {}\n").expect("write local");
    let selected = resolve_default_config_path(local.to_string_lossy().as_ref(), "fallback.yaml");
    assert_eq!(selected, local.to_string_lossy());
    fs::remove_file(&local).expect("cleanup local");

    let selected =
        resolve_default_config_path("/definitely/missing/systrace.yaml", "fallback.yaml");
    assert_eq!(selected, "fallback.yaml");
}

#[test]
fn load_yaml_config_file_covers_missing_empty_invalid_and_valid() {
    let missing = unique_temp_path("systrace-missing.yaml");
    let err = load_yaml_config_file(missing.to_string_lossy().as_ref()).unwrap_err();
    assert!(err.to_string().contains("was not found"), "{err}");

    let empty = unique_temp_path("systrace-empty.yaml");
    fs::write(&empty, " \n").expect("write empty");
    let err = load_yaml_config_file(empty.to_string_lossy().as_ref()).unwrap_err();
    assert!(err.to_string().contains("is empty"), "{err}");
    fs::remove_file(&empty).expect("cleanup empty");

    let invalid = unique_temp_path("systrace-invalid.yaml");
    fs::write(&invalid, "service: [\n").expect("write invalid");
    let err = load_yaml_config_file(invalid.to_string_lossy().as_ref()).unwrap_err();
    assert!(err.to_string().contains("failed to parse YAML"), "{err}");
    fs::remove_file(&invalid).expect("cleanup invalid");

    let valid = unique_temp_path("systrace-valid.yaml");
    fs::write(
        &valid,
        "service:\n  name: ojo-systrace\ncollection:\n  poll_interval_secs: 2\n",
    )
    .expect("write valid");
    assert!(load_yaml_config_file(valid.to_string_lossy().as_ref()).is_ok());
    fs::remove_file(&valid).expect("cleanup valid");
}

#[test]
fn config_load_from_args_supports_trace_and_metrics_sections() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("systrace-config.yaml");
    fs::write(
        &path,
        "service:\n  name: ojo-systrace-test\n  instance_id: systrace-test-1\ncollection:\n  poll_interval_secs: 3\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\nmetrics:\n  include: [system.systrace.]\ntraces:\n  enabled: true\n  include: [systrace.]\n",
    )
    .expect("write config");

    std::env::remove_var("OJO_SYSTRACE_CONFIG");
    let args = vec![
        "ojo-systrace".to_string(),
        "--config".to_string(),
        path.to_string_lossy().to_string(),
        "--once".to_string(),
    ];

    let cfg = Config::load_from_args(&args).expect("load config");
    assert_eq!(cfg.service_name, "ojo-systrace-test");
    assert_eq!(cfg.instance_id, "systrace-test-1");
    assert_eq!(cfg.poll_interval, Duration::from_secs(3));
    assert!(cfg.trace_enabled);
    assert_eq!(cfg.metrics_include, vec!["system.systrace."]);
    assert_eq!(cfg.trace_include, vec!["systrace."]);
    assert!(cfg.once);

    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn default_snapshot_is_zeroed() {
    let snapshot = SystraceSnapshot::default();
    assert!(!snapshot.available);
    assert_eq!(snapshot.events_total, 0);
    assert_eq!(snapshot.context_switches_per_sec, 0.0);
    assert_eq!(snapshot.interrupts_per_sec, 0.0);
    assert_eq!(snapshot.system_calls_per_sec, 0.0);
    assert_eq!(snapshot.system_calls_source, "");
    assert_eq!(snapshot.system_calls_source_code, 0);
    assert_eq!(snapshot.system_calls_coverage_ratio, 0.0);
    assert_eq!(snapshot.dpcs_per_sec, 0.0);
    assert_eq!(snapshot.process_forks_per_sec, 0.0);
    assert_eq!(snapshot.run_queue_depth, 0.0);
    assert_eq!(snapshot.processes_total, 0);
    assert_eq!(snapshot.threads_total, 0);
    assert_eq!(snapshot.kernel_stack_samples_total, 0);
    assert_eq!(snapshot.user_stack_samples_total, 0);
}

#[test]
fn infer_platform_component_covers_linux_windows_unknown() {
    let linux = SystraceSnapshot {
        tracefs_available: true,
        etw_available: false,
        ..SystraceSnapshot::default()
    };
    assert_eq!(infer_platform_component(&linux), "kernel.linux");

    let windows = SystraceSnapshot {
        tracefs_available: false,
        etw_available: true,
        ..SystraceSnapshot::default()
    };
    assert_eq!(infer_platform_component(&windows), "kernel.windows");

    let unknown = SystraceSnapshot::default();
    assert_eq!(infer_platform_component(&unknown), "kernel.unknown");
}

#[test]
fn infer_trace_line_component_normalizes_kernel_task_names() {
    assert_eq!(
        infer_trace_line_component("<idle>-0     [002] d..2  12.34: func"),
        Some("kernel.idle".to_string())
    );
    assert_eq!(
        infer_trace_line_component("kworker/0:1-123 [000] .... 1.0: foo"),
        Some("kernel.kworker.0".to_string())
    );
    assert_eq!(
        infer_trace_line_component("my-app_worker-42 [001] .... 2.0: bar"),
        Some("kernel.my-app_worker".to_string())
    );
    assert_eq!(
        infer_trace_line_component("=> exc_page_fault"),
        Some("kernel.exc_page_fault".to_string())
    );
    assert_eq!(
        infer_trace_line_component("=>  <000077aab70ac772>"),
        Some("kernel.userstack".to_string())
    );
    assert_eq!(infer_trace_line_component(""), None);
}

#[test]
fn parse_trace_line_timestamp_yields_boot_relative_time() {
    let line = "<idle>-0     [002] d..2  12.34: func";
    let timestamp = super::parse_trace_line_timestamp(line).expect("should parse timestamp");
    let now = SystemTime::now();
    assert!(timestamp <= now, "parsed timestamp should not be in the future");
}

#[test]
fn parse_trace_line_timestamp_handles_colons_in_task_name() {
    let line = "kworker/0:1-123 [000] .... 1024.000123: sched_switch: prev=foo next=bar";
    let timestamp = super::parse_trace_line_timestamp(line).expect("should parse timestamp");
    let now = SystemTime::now();
    assert!(timestamp <= now, "parsed timestamp should not be in the future");
}

#[test]
fn derive_trace_line_delta_us_distributes_sparse_timestamps() {
    let lines = vec![
        "task-1 [000] .... 10.000000: <stack trace>",
        "=> symbol_a",
        "=> symbol_b",
        "task-1 [000] .... 10.001000: sys_enter",
    ];
    let deltas = derive_trace_line_delta_us(&lines);
    assert_eq!(deltas.len(), 4);
    assert!(deltas[0] >= 300);
    assert!(deltas[1] >= 300);
    assert!(deltas[2] >= 300);
}
