use crate::{
    advance_export_state, bool_as_u64, build_otlp_logs_payload, default_logs_endpoint,
    export_buffered_logs, handle_flush_event, load_yaml_config_file, make_stop_handler,
    next_export_state, normalize_severity, parse_bool_env, record_buffer_drop_if_any,
    record_snapshot, resolve_default_config_path, sanitize_ascii_line, ArchivePipeline, Config,
    ExportState, FlushEvent, Instruments, LogBuffer, LogRecord, OtlpLogExporter, RuntimeSnapshot,
};
use host_collectors::{ArchiveCompression, ArchiveFormat, ArchiveMode, PrefixFilter};
use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::Duration;
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
    let ctrl = sanitize_ascii_line("a\u{0007}b\t\n", 8);
    assert_eq!(ctrl, "ab\t\n");
}

#[test]
fn normalize_severity_maps_aliases_and_unknown() {
    assert_eq!(normalize_severity("warning"), "WARN");
    assert_eq!(normalize_severity("critical"), "FATAL");
    assert_eq!(normalize_severity("  \t  "), "INFO");
    assert_eq!(normalize_severity("whatever"), "INFO");
}

#[test]
fn normalize_severity_preserves_known_levels() {
    assert_eq!(normalize_severity("error"), "ERROR");
    assert_eq!(normalize_severity("debug"), "DEBUG");
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
fn push_front_batch_trims_when_capacity_exceeded() {
    let mut buffer = LogBuffer::new(2);
    let _ = buffer.push_many(vec![sample_record("a"), sample_record("b")]);
    buffer.push_front_batch(vec![sample_record("c"), sample_record("d")]);
    assert_eq!(buffer.len(), 2);
    let batch = buffer.pop_batch(10);
    assert_eq!(batch.len(), 2);
    assert_eq!(batch[0].body, "c");
    assert_eq!(batch[1].body, "d");
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

#[cfg(target_os = "windows")]
#[test]
fn resolve_default_config_path_prefers_windows_syslog_default() {
    let selected = resolve_default_config_path(
        "syslog.windows.yaml",
        "services/syslog/syslog.windows.yaml",
    );
    assert_eq!(selected, "syslog.windows.yaml");
}

#[cfg(target_os = "windows")]
#[test]
fn load_windows_companion_config_parses() {
    let config_path = concat!(env!("CARGO_MANIFEST_DIR"), "/syslog.windows.yaml");
    let file_cfg = load_yaml_config_file(config_path).expect("windows config");

    let service = file_cfg.service.expect("service section");
    assert_eq!(service.name.as_deref(), Some("ojo-syslog"));
    assert_eq!(service.instance_id.as_deref(), Some("syslog-windows"));

    let collection = file_cfg.collection.expect("collection section");
    assert_eq!(collection.poll_interval_secs, Some(5));
    assert_eq!(collection.max_lines_per_source, Some(200));
    assert_eq!(collection.max_message_bytes, Some(4096));

    let watch = file_cfg.watch.expect("watch section");
    assert!(watch.files.unwrap_or_default().is_empty());

    let export = file_cfg.export.expect("export section");
    assert_eq!(export.otlp.expect("otlp section").endpoint.as_deref(), Some("http://192.168.0.214:4355/v1/metrics"));
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

    let (state, event) = advance_export_state(ExportState::Connected, true);
    assert_eq!(state, ExportState::Connected);
    assert_eq!(event, FlushEvent::None);
}

#[test]
fn next_export_state_covers_attempted_and_not_attempted_paths() {
    assert_eq!(
        next_export_state(ExportState::Pending, true, true),
        (ExportState::Connected, FlushEvent::Connected)
    );
    assert_eq!(
        next_export_state(ExportState::Connected, false, true),
        (ExportState::Connected, FlushEvent::None)
    );
}

fn test_config_with_logs_endpoint(logs_endpoint: String) -> Config {
    Config {
        service_name: "ojo-syslog-test".to_string(),
        instance_id: "instance-1".to_string(),
        poll_interval: Duration::from_secs(1),
        otlp_endpoint: "http://127.0.0.1:4318/v1/metrics".to_string(),
        otlp_protocol: "http/protobuf".to_string(),
        otlp_timeout: Some(Duration::from_secs(1)),
        export_interval: Some(Duration::from_secs(1)),
        export_timeout: Some(Duration::from_secs(1)),
        logs_endpoint,
        logs_timeout: Duration::from_secs(2),
        metrics_include: vec!["system.syslog.".to_string()],
        metrics_exclude: vec![],
        max_lines_per_source: 10,
        max_message_bytes: 1024,
        watch_files: vec![],
        buffer_capacity_records: 64,
        export_batch_size: 10,
        retry_backoff: Duration::from_secs(1),
        archive_enabled: true,
        archive_dir: "".to_string(),
        archive_max_file_bytes: 1024,
        archive_retain_files: 2,
        archive_format: ArchiveFormat::Parquet,
        archive_mode: ArchiveMode::Trend,
        archive_window_secs: 60,
        archive_compression: ArchiveCompression::Zstd,
        once: true,
    }
}

#[test]
fn exporter_empty_batch_and_archive_prune_edges() {
    let cfg = test_config_with_logs_endpoint("http://127.0.0.1:4318/v1/logs".to_string());
    let exporter = OtlpLogExporter::new(&cfg).expect("create exporter");
    assert_eq!(exporter.export_batch(&[]).expect("empty batch"), 0);

    let dir = unique_temp_path("syslog-archive-prune");
    fs::create_dir_all(&dir).expect("create dir");
    let path = dir.join("syslog.ndjson");
    fs::write(&path, "line\n").expect("seed archive");
    fs::write(dir.join("syslog.ndjson.3"), "old\n").expect("seed rotated");

    let mut archive_cfg =
        test_config_with_logs_endpoint("http://127.0.0.1:4318/v1/logs".to_string());
    archive_cfg.archive_enabled = true;
    archive_cfg.archive_dir = dir.to_string_lossy().to_string();
    archive_cfg.archive_max_file_bytes = 1024;
    archive_cfg.archive_retain_files = 1;
    let archive = ArchivePipeline::from_config(&archive_cfg);
    archive
        .rotate_if_needed(path.to_string_lossy().as_ref())
        .expect("no rotate needed");
    archive
        .prune_rotated_files(path.to_string_lossy().as_ref())
        .expect("prune works");
    assert!(!dir.join("syslog.ndjson.3").exists());

    archive_cfg.archive_retain_files = 0;
    let archive_no_retain = ArchivePipeline::from_config(&archive_cfg);
    archive_no_retain
        .prune_rotated_files(path.to_string_lossy().as_ref())
        .expect("retain zero noop");

    fs::remove_dir_all(&dir).expect("cleanup");
}

#[test]
fn export_buffered_logs_returns_early_for_empty_buffer_and_zero_batch() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
    let addr = listener.local_addr().expect("local addr");

    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut req = [0u8; 4096];
        let _ = stream.read(&mut req).expect("read request");
        let response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
        stream
            .write_all(response.as_bytes())
            .expect("write response");
    });

    let cfg = test_config_with_logs_endpoint(format!("http://{addr}/logs"));
    let exporter = OtlpLogExporter::new(&cfg).expect("create exporter");

    let mut empty_buffer = LogBuffer::new(8);
    let (stats, err) = export_buffered_logs(&exporter, &mut empty_buffer, 4);
    assert!(err.is_none());
    assert_eq!(stats.exported_records, 0);
    assert_eq!(empty_buffer.len(), 0);

    let mut zero_batch_buffer = LogBuffer::new(8);
    zero_batch_buffer.push_many(vec![sample_record("one")]);
    let (stats, err) = export_buffered_logs(&exporter, &mut zero_batch_buffer, 0);
    assert!(err.is_none());
    assert_eq!(stats.exported_records, 1);
    assert_eq!(zero_batch_buffer.len(), 0);

    handle.join().expect("server thread");
}

#[test]
fn parse_bool_and_bool_as_u64_cover_paths() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::set_var("OJO_SYSLOG_BOOL_TEST", "yes");
    assert_eq!(parse_bool_env("OJO_SYSLOG_BOOL_TEST"), Some(true));
    std::env::set_var("OJO_SYSLOG_BOOL_TEST", "off");
    assert_eq!(parse_bool_env("OJO_SYSLOG_BOOL_TEST"), Some(false));
    std::env::set_var("OJO_SYSLOG_BOOL_TEST", "garbage");
    assert_eq!(parse_bool_env("OJO_SYSLOG_BOOL_TEST"), None);
    std::env::remove_var("OJO_SYSLOG_BOOL_TEST");
    assert_eq!(parse_bool_env("OJO_SYSLOG_BOOL_TEST"), None);
    assert_eq!(bool_as_u64(true), 1);
    assert_eq!(bool_as_u64(false), 0);
}

#[test]
fn config_load_from_args_applies_env_fallbacks_and_minimums() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("syslog-config-env.yaml");
    fs::write(
        &path,
        "service:\n  name: env-fallback\ncollection:\n  poll_interval_secs: 0\n  max_lines_per_source: 0\n  max_message_bytes: 1\nexport:\n  logs:\n    timeout_secs: 0\npipeline:\n  buffer_capacity_records: 0\n  export_batch_size: 0\n  retry_backoff_secs: 0\nstorage:\n  archive_enabled: true\n",
    )
    .expect("write config");

    std::env::set_var(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "http://127.0.0.1:4320/v1/metrics",
    );
    std::env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc");
    std::env::set_var(
        "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
        "http://127.0.0.1:4320/v1/logs",
    );

    let args = vec![
        "ojo-syslog".to_string(),
        "--config".to_string(),
        path.to_string_lossy().to_string(),
    ];
    let cfg = Config::load_from_args(&args).expect("load config");

    assert_eq!(cfg.otlp_endpoint, "http://127.0.0.1:4320/v1/metrics");
    assert_eq!(cfg.otlp_protocol, "grpc");
    assert_eq!(cfg.logs_endpoint, "http://127.0.0.1:4320/v1/logs");
    assert_eq!(cfg.poll_interval, Duration::from_secs(1));
    assert_eq!(cfg.max_lines_per_source, 1);
    assert_eq!(cfg.max_message_bytes, 128);
    assert_eq!(cfg.logs_timeout, Duration::from_secs(1));
    assert_eq!(cfg.buffer_capacity_records, 256);
    assert_eq!(cfg.export_batch_size, 1);
    assert_eq!(cfg.retry_backoff, Duration::from_secs(1));

    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");
    std::env::remove_var("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn config_load_from_args_uses_repo_default_when_env_not_set() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::remove_var("OJO_SYSLOG_CONFIG");
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");

    let args = vec!["ojo-syslog".to_string()];
    let cfg = Config::load_from_args(&args).expect("load default config");
    assert!(!cfg.service_name.is_empty());
}

#[test]
fn config_load_from_args_uses_builtin_service_name_when_service_section_missing() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("syslog-config-no-service.yaml");
    fs::write(
        &path,
        "collection:\n  poll_interval_secs: 1\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
    )
    .expect("write config");

    let args = vec![
        "ojo-syslog".to_string(),
        "--config".to_string(),
        path.to_string_lossy().to_string(),
    ];
    let cfg = Config::load_from_args(&args).expect("load config");
    assert_eq!(cfg.service_name, "ojo-syslog");

    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn archive_pipeline_writes_and_rotates_records() {
    let dir = unique_temp_path("syslog-archive-ok");
    fs::create_dir_all(&dir).expect("create archive dir");
    let mut cfg = test_config_with_logs_endpoint("http://127.0.0.1:1/v1/logs".to_string());
    cfg.archive_dir = dir.to_string_lossy().to_string();
    cfg.archive_max_file_bytes = 40;
    cfg.archive_retain_files = 2;

    let mut archive = ArchivePipeline::from_config(&cfg);
    let path = dir.join("syslog-trend.parquet");

    archive.write_batch(&[sample_record("first")]);
    archive.write_batch(&[sample_record("second")]);
    archive.write_batch(&[sample_record("third")]);

    assert!(archive.healthy);
    assert!(archive.last_error.is_none());
    assert!(path.exists());
    assert!(std::path::Path::new(&format!("{}.1", path.to_string_lossy())).exists());

    fs::remove_dir_all(&dir).expect("cleanup archive dir");
}

#[test]
fn archive_pipeline_marks_unhealthy_on_invalid_dir() {
    let parent = unique_temp_path("syslog-archive-err");
    fs::create_dir_all(&parent).expect("create parent");
    let blocker = parent.join("not-a-dir");
    fs::write(&blocker, b"blocker").expect("create blocker file");

    let mut cfg = test_config_with_logs_endpoint("http://127.0.0.1:1/v1/logs".to_string());
    cfg.archive_dir = blocker.to_string_lossy().to_string();
    let mut archive = ArchivePipeline::from_config(&cfg);
    archive.write_batch(&[sample_record("bad")]);

    assert!(!archive.healthy);
    assert!(archive.last_error.is_some());

    fs::remove_dir_all(&parent).expect("cleanup");
}

#[test]
fn archive_pipeline_rotate_if_needed_covers_missing_and_oversized_paths() {
    let dir = unique_temp_path("syslog-archive-rotate-branches");
    fs::create_dir_all(&dir).expect("create archive dir");
    let mut cfg = test_config_with_logs_endpoint("http://127.0.0.1:1/v1/logs".to_string());
    cfg.archive_dir = dir.to_string_lossy().to_string();
    cfg.archive_max_file_bytes = 8;
    cfg.archive_retain_files = 2;
    let archive = ArchivePipeline::from_config(&cfg);

    let missing_path = dir.join("missing.parquet");
    archive
        .rotate_if_needed(missing_path.to_string_lossy().as_ref())
        .expect("missing file should be a no-op");

    let path = dir.join("syslog-trend.parquet");
    fs::write(&path, b"this file is oversized").expect("seed oversized archive");
    fs::write(format!("{}.1", path.to_string_lossy()), b"old1").expect("seed .1");
    fs::write(format!("{}.2", path.to_string_lossy()), b"old2").expect("seed .2");

    archive
        .rotate_if_needed(path.to_string_lossy().as_ref())
        .expect("oversized file should rotate");

    assert!(std::path::Path::new(&format!("{}.1", path.to_string_lossy())).exists());
    assert!(std::path::Path::new(&format!("{}.2", path.to_string_lossy())).exists());

    fs::remove_dir_all(&dir).expect("cleanup archive dir");
}

#[test]
fn export_buffered_logs_handles_success_and_error() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
    let addr = listener.local_addr().expect("local addr");

    let handle = thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut req = [0u8; 4096];
            let _ = stream.read(&mut req).expect("read request");
            let status = if req.starts_with(b"POST") && req.windows(6).any(|w| w == b"/error") {
                "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n"
            } else {
                "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
            };
            stream.write_all(status.as_bytes()).expect("write response");
        }
    });

    let mut ok_cfg = test_config_with_logs_endpoint(format!("http://{addr}/ok"));
    ok_cfg.archive_enabled = false;
    let exporter_ok = OtlpLogExporter::new(&ok_cfg).expect("exporter ok");
    let mut ok_buffer = LogBuffer::new(8);
    ok_buffer.push_many(vec![sample_record("one"), sample_record("two")]);

    let (ok_stats, ok_err) = export_buffered_logs(&exporter_ok, &mut ok_buffer, 10);
    assert!(ok_err.is_none());
    assert_eq!(ok_stats.exported_records, 2);
    assert_eq!(ok_buffer.len(), 0);

    let mut err_cfg = test_config_with_logs_endpoint(format!("http://{addr}/error"));
    err_cfg.archive_enabled = false;
    let exporter_err = OtlpLogExporter::new(&err_cfg).expect("exporter err");
    let mut err_buffer = LogBuffer::new(8);
    err_buffer.push_many(vec![sample_record("fail")]);

    let (err_stats, err) = export_buffered_logs(&exporter_err, &mut err_buffer, 10);
    assert!(err.is_some());
    assert_eq!(err_stats.errors, 1);
    assert_eq!(err_stats.retries, 1);
    assert_eq!(err_buffer.len(), 1);

    handle.join().expect("server thread");
}

#[test]
fn make_stop_handler_sets_signal_false() {
    let running = Arc::new(AtomicBool::new(true));
    let stop = make_stop_handler(Arc::clone(&running));
    stop();
    assert!(!running.load(Ordering::SeqCst));
}

#[test]
fn instruments_and_record_snapshot_cover_metric_paths() {
    let meter = opentelemetry::global::meter("syslog-metrics-test");
    let instruments = Instruments::new(&meter);
    let filter = PrefixFilter::new(vec!["system.syslog.".to_string()], vec![]);
    let snapshot = RuntimeSnapshot {
        available: true,
        journald_available: true,
        etw_available: true,
        dmesg_available: true,
        process_logs_available: true,
        application_logs_available: true,
        file_watch_targets_configured: 2,
        file_watch_targets_active: 1,
        buffer_capacity_records: 10,
        buffer_queued_records: 3,
        exporter_available: true,
        exporter_reconnecting: false,
        last_batch_size: 2,
        last_payload_bytes: 123,
        collection_errors: 0,
    };

    record_snapshot(&instruments, &filter, &snapshot);

    record_buffer_drop_if_any(&instruments, 0);
    record_buffer_drop_if_any(&instruments, 2);
}

#[test]
fn record_snapshot_respects_filter_exclusions() {
    let meter = opentelemetry::global::meter("syslog-metrics-filter-test");
    let instruments = Instruments::new(&meter);
    let filter = PrefixFilter::new(vec!["unrelated.prefix.".to_string()], vec![]);
    let snapshot = RuntimeSnapshot {
        available: true,
        journald_available: true,
        etw_available: false,
        dmesg_available: false,
        process_logs_available: false,
        application_logs_available: true,
        file_watch_targets_configured: 1,
        file_watch_targets_active: 1,
        buffer_capacity_records: 4,
        buffer_queued_records: 2,
        exporter_available: true,
        exporter_reconnecting: false,
        last_batch_size: 1,
        last_payload_bytes: 10,
        collection_errors: 0,
    };

    // Ensure the false branch of record_u64 executes when metrics are filtered out.
    record_snapshot(&instruments, &filter, &snapshot);
}

#[test]
fn handle_flush_event_covers_error_and_success_paths() {
    let err = "mock exporter error";
    handle_flush_event(FlushEvent::Reconnecting, Some(&err));
    handle_flush_event(FlushEvent::StillUnavailable, Some(&err));
    handle_flush_event(FlushEvent::None, Some(&err));
    handle_flush_event(FlushEvent::Connected, Some(&err));
    handle_flush_event(FlushEvent::Reconnected, Some(&err));
    handle_flush_event(FlushEvent::Connected, None);
    handle_flush_event(FlushEvent::Reconnected, None);
    handle_flush_event(FlushEvent::None, None);
}

#[test]
fn run_smoke_once_with_temp_config() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("syslog-run-once.yaml");
    fs::write(
        &path,
        "service:\n  name: ojo-syslog-test\ncollection:\n  poll_interval_secs: 1\n  max_lines_per_source: 1\n  max_message_bytes: 256\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n  logs:\n    endpoint: http://127.0.0.1:4318/v1/logs\n    timeout_secs: 1\npipeline:\n  buffer_capacity_records: 256\n  export_batch_size: 1\n  retry_backoff_secs: 1\nstorage:\n  archive_enabled: false\n",
    )
    .expect("write config");

    std::env::set_var("OJO_SYSLOG_CONFIG", path.to_string_lossy().to_string());
    std::env::set_var("OJO_RUN_ONCE", "1");

    let result = crate::run();
    assert!(
        result.is_ok()
            || result
                .as_ref()
                .err()
                .map(|e| e
                    .to_string()
                    .contains("Ctrl-C signal handler already registered"))
                .unwrap_or(false),
        "run result: {result:?}"
    );

    std::env::remove_var("OJO_SYSLOG_CONFIG");
    std::env::remove_var("OJO_RUN_ONCE");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn run_smoke_once_with_watch_file_and_export_failure() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("syslog-run-fail.yaml");
    let watched = unique_temp_path("syslog-watch.log");
    fs::write(&watched, "watch-line\n").expect("write watched file");
    fs::write(
        &path,
        format!(
            "service:\n  name: ojo-syslog-fail\ncollection:\n  poll_interval_secs: 1\n  max_lines_per_source: 5\n  max_message_bytes: 256\nwatch:\n  files:\n    - name: test-watch\n      path: {}\n      source: application\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n  logs:\n    endpoint: http://127.0.0.1:1/v1/logs\n    timeout_secs: 1\npipeline:\n  buffer_capacity_records: 256\n  export_batch_size: 1\n  retry_backoff_secs: 1\nstorage:\n  archive_enabled: false\n",
            watched.to_string_lossy()
        ),
    )
    .expect("write config");

    std::env::set_var("OJO_SYSLOG_CONFIG", path.to_string_lossy().to_string());
    std::env::set_var("OJO_RUN_ONCE", "1");

    let result = crate::run();
    assert!(
        result.is_ok()
            || result
                .as_ref()
                .err()
                .map(|e| e
                    .to_string()
                    .contains("Ctrl-C signal handler already registered"))
                .unwrap_or(false),
        "run result: {result:?}"
    );

    std::env::remove_var("OJO_SYSLOG_CONFIG");
    std::env::remove_var("OJO_RUN_ONCE");
    fs::remove_file(&path).expect("cleanup config");
    fs::remove_file(&watched).expect("cleanup watched");
}

#[test]
fn run_non_once_with_export_failure_covers_retry_backoff_path() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("syslog-run-loop-error.yaml");
    fs::write(
        &path,
        "service:\n  name: ojo-syslog-loop-error\ncollection:\n  poll_interval_secs: 1\n  max_lines_per_source: 1\n  max_message_bytes: 256\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n  logs:\n    endpoint: http://127.0.0.1:1/v1/logs\n    timeout_secs: 1\npipeline:\n  buffer_capacity_records: 256\n  export_batch_size: 1\n  retry_backoff_secs: 1\nstorage:\n  archive_enabled: false\n",
    )
    .expect("write config");

    std::env::set_var("OJO_SYSLOG_CONFIG", path.to_string_lossy().to_string());
    std::env::remove_var("OJO_RUN_ONCE");
    std::env::set_var("OJO_TEST_MAX_ITERATIONS", "2");

    let result = crate::run();
    assert!(result.is_ok(), "run result: {result:?}");

    std::env::remove_var("OJO_SYSLOG_CONFIG");
    std::env::remove_var("OJO_TEST_MAX_ITERATIONS");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn run_non_once_with_successful_export_covers_poll_sleep_path() {
    let _guard = env_lock().lock().expect("env lock");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
    let addr = listener.local_addr().expect("local addr");
    let server = thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut req = [0u8; 4096];
            let _ = stream.read(&mut req).expect("read request");
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .expect("write response");
        }
    });

    let path = unique_temp_path("syslog-run-loop-success.yaml");
    fs::write(
        &path,
        format!(
            "service:\n  name: ojo-syslog-loop-success\ncollection:\n  poll_interval_secs: 1\n  max_lines_per_source: 1\n  max_message_bytes: 256\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n  logs:\n    endpoint: http://{addr}/v1/logs\n    timeout_secs: 1\npipeline:\n  buffer_capacity_records: 256\n  export_batch_size: 1\n  retry_backoff_secs: 1\nstorage:\n  archive_enabled: false\n"
        ),
    )
    .expect("write config");

    std::env::set_var("OJO_SYSLOG_CONFIG", path.to_string_lossy().to_string());
    std::env::remove_var("OJO_RUN_ONCE");
    std::env::set_var("OJO_TEST_MAX_ITERATIONS", "2");

    let result = crate::run();
    assert!(result.is_ok(), "run result: {result:?}");

    std::env::remove_var("OJO_SYSLOG_CONFIG");
    std::env::remove_var("OJO_TEST_MAX_ITERATIONS");
    fs::remove_file(&path).expect("cleanup config");
    server.join().expect("server thread");
}
