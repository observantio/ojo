use crate::{
    advance_export_state, build_otlp_logs_payload, default_logs_endpoint, load_yaml_config_file,
    normalize_severity, resolve_default_config_path, sanitize_ascii_line, Config, ExportState,
    FlushEvent, LogBuffer, LogRecord,
};
use std::fs;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn unique_temp_path(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("ojo-syslog-{name}-{}-{nanos}", std::process::id()))
}

fn sample_record(body: &str) -> LogRecord {
    LogRecord {
        observed_time_unix_nano: 123,
        severity_text: "INFO".to_string(),
        body: body.to_string(),
        source: "application".to_string(),
        stream: "file".to_string(),
        watch_target: "app".to_string(),
    }
}

#[test]
fn default_logs_endpoint_translates_metrics_endpoint() {
    assert_eq!(
        default_logs_endpoint("http://127.0.0.1:4318/v1/metrics"),
        "http://127.0.0.1:4318/v1/logs"
    );
    assert_eq!(
        default_logs_endpoint("http://127.0.0.1:4318"),
        "http://127.0.0.1:4318/v1/logs"
    );
}

#[test]
fn sanitize_ascii_line_enforces_ascii_and_truncation() {
    let value = sanitize_ascii_line("hello\u{2603}\nworld", 8);
    assert_eq!(value, "hello?\nw");
}

#[test]
fn normalize_severity_maps_aliases_and_unknown() {
    assert_eq!(normalize_severity("warning"), "WARN");
    assert_eq!(normalize_severity("critical"), "FATAL");
    assert_eq!(normalize_severity("whatever"), "INFO");
}

#[test]
fn buffer_drops_oldest_when_capacity_reached() {
    let mut buffer = LogBuffer::new(2);
    let dropped = buffer.push_many(vec![
        sample_record("a"),
        sample_record("b"),
        sample_record("c"),
    ]);
    assert_eq!(dropped, 1);
    assert_eq!(buffer.len(), 2);
    let batch = buffer.pop_batch(10);
    assert_eq!(batch.len(), 2);
    assert_eq!(batch[0].body, "b");
    assert_eq!(batch[1].body, "c");
}

#[test]
fn payload_shape_contains_resource_and_scope_logs() {
    let payload = build_otlp_logs_payload("ojo-syslog", "node-1", &[sample_record("line")]);
    let resource_logs = payload
        .get("resourceLogs")
        .and_then(|v| v.as_array())
        .expect("resourceLogs array");
    assert_eq!(resource_logs.len(), 1);

    let scope_logs = resource_logs[0]
        .get("scopeLogs")
        .and_then(|v| v.as_array())
        .expect("scopeLogs array");
    assert_eq!(scope_logs.len(), 1);

    let log_records = scope_logs[0]
        .get("logRecords")
        .and_then(|v| v.as_array())
        .expect("logRecords array");
    assert_eq!(log_records.len(), 1);
}

#[test]
fn resolve_default_config_path_prefers_existing_local() {
    let local = unique_temp_path("syslog-local.yaml");
    fs::write(&local, "service: {}\n").expect("write local");
    let selected = resolve_default_config_path(local.to_string_lossy().as_ref(), "fallback.yaml");
    assert_eq!(selected, local.to_string_lossy());
    fs::remove_file(&local).expect("cleanup local");

    let selected = resolve_default_config_path("/definitely/missing/syslog.yaml", "fallback.yaml");
    assert_eq!(selected, "fallback.yaml");
}

#[test]
fn load_yaml_config_file_handles_missing_empty_invalid() {
    let missing = unique_temp_path("syslog-missing.yaml");
    let err = load_yaml_config_file(missing.to_string_lossy().as_ref()).unwrap_err();
    assert!(err.to_string().contains("was not found"));

    let empty = unique_temp_path("syslog-empty.yaml");
    fs::write(&empty, " \n").expect("write empty");
    let err = load_yaml_config_file(empty.to_string_lossy().as_ref()).unwrap_err();
    assert!(err.to_string().contains("is empty"));
    fs::remove_file(&empty).expect("cleanup empty");

    let invalid = unique_temp_path("syslog-invalid.yaml");
    fs::write(&invalid, "service: [\n").expect("write invalid");
    let err = load_yaml_config_file(invalid.to_string_lossy().as_ref()).unwrap_err();
    assert!(err.to_string().contains("failed to parse YAML"));
    fs::remove_file(&invalid).expect("cleanup invalid");
}

#[test]
fn config_load_from_args_reads_watch_and_pipeline_sections() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("syslog-config.yaml");
    fs::write(
        &path,
        "service:\n  name: ojo-syslog-test\ncollection:\n  poll_interval_secs: 2\n  max_lines_per_source: 11\nwatch:\n  files:\n    - name: app\n      path: /tmp/app.log\n      source: application\npipeline:\n  buffer_capacity_records: 321\n  export_batch_size: 7\n  retry_backoff_secs: 2\n",
    )
    .expect("write config");

    std::env::remove_var("OJO_SYSLOG_CONFIG");
    let args = vec![
        "ojo-syslog".to_string(),
        "--config".to_string(),
        path.to_string_lossy().to_string(),
        "--once".to_string(),
    ];

    let cfg = Config::load_from_args(&args).expect("load config");
    assert_eq!(cfg.service_name, "ojo-syslog-test");
    assert_eq!(cfg.max_lines_per_source, 11);
    assert_eq!(cfg.watch_files.len(), 1);
    assert_eq!(cfg.buffer_capacity_records, 321);
    assert_eq!(cfg.export_batch_size, 7);
    assert!(cfg.once);

    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn export_state_transitions_follow_connect_reconnect_contract() {
    let (state, event) = advance_export_state(ExportState::Pending, true);
    assert_eq!(state, ExportState::Connected);
    assert_eq!(event, FlushEvent::Connected);

    let (state, event) = advance_export_state(ExportState::Connected, false);
    assert_eq!(state, ExportState::Reconnecting);
    assert_eq!(event, FlushEvent::Reconnecting);

    let (state, event) = advance_export_state(ExportState::Reconnecting, true);
    assert_eq!(state, ExportState::Connected);
    assert_eq!(event, FlushEvent::Reconnected);

    let (state, event) = advance_export_state(ExportState::Pending, false);
    assert_eq!(state, ExportState::Reconnecting);
    assert_eq!(event, FlushEvent::StillUnavailable);
}
