use super::{
    build_meter_provider, build_tracer_provider, default_protocol_for_endpoint, hostname,
    init_meter_provider, init_tracer_provider, ArchiveStorageConfig, JsonArchiveWriter,
    OtlpSettings, PrefixFilter, StdoutSpanExporter, METRIC_PREFIX_SYSTEM,
};
use opentelemetry::trace::{
    SpanContext, SpanId, SpanKind, Status, TraceFlags, TraceId, TraceState,
};
use opentelemetry::InstrumentationScope;
use opentelemetry::KeyValue;
use opentelemetry_sdk::error::OTelSdkError;
use opentelemetry_sdk::resource::Resource;
use opentelemetry_sdk::trace::{SpanData, SpanEvents, SpanExporter, SpanLinks};
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    path.push(format!("{prefix}_{}_{}", std::process::id(), nanos));
    path
}

fn make_span_data(
    name: &'static str,
    parent_span_id: SpanId,
    attributes: Vec<KeyValue>,
) -> SpanData {
    SpanData {
        span_context: SpanContext::new(
            TraceId::from_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 66]),
            SpanId::from_bytes([0, 0, 0, 0, 0, 0, 0, 36]),
            TraceFlags::default(),
            false,
            TraceState::default(),
        ),
        parent_span_id,
        parent_span_is_remote: false,
        span_kind: SpanKind::Internal,
        name: name.into(),
        start_time: SystemTime::now(),
        end_time: SystemTime::now(),
        attributes,
        dropped_attributes_count: 0,
        events: SpanEvents::default(),
        links: SpanLinks::default(),
        status: Status::Unset,
        instrumentation_scope: InstrumentationScope::builder("host-collectors-tests").build(),
    }
}

fn test_settings(protocol: &str) -> OtlpSettings {
    OtlpSettings {
        service_name: "test-svc".to_string(),
        instance_id: "test-1".to_string(),
        otlp_endpoint: "http://127.0.0.1:4317".to_string(),
        otlp_protocol: protocol.to_string(),
        otlp_headers: BTreeMap::new(),
        otlp_compression: None,
        otlp_timeout: Some(Duration::from_secs(2)),
        export_interval: None,
        export_timeout: None,
    }
}

#[test]
fn prefix_filter_respects_include_and_exclude() {
    let filter = PrefixFilter::new(
        vec!["system.".to_string(), "process.".to_string()],
        vec!["system.sensor.".to_string()],
    );
    assert!(filter.allows("system.cpu.usage"));
    assert!(!filter.allows("system.sensor.temperature.celsius"));
    assert!(!filter.allows("custom.metric"));
}

#[test]
fn prefix_filter_allows_when_include_is_empty() {
    let filter = PrefixFilter::new(vec![], vec!["system.sensor.".to_string()]);
    assert!(filter.allows("system.cpu.usage"));
    assert!(!filter.allows("system.sensor.temperature.celsius"));
}

#[test]
fn default_protocol_is_http_for_path_endpoint() {
    let protocol = default_protocol_for_endpoint(Some("http://127.0.0.1:4318/v1/metrics"));
    assert_eq!(protocol, "http/protobuf");
}

#[test]
fn default_protocol_is_grpc_for_root_endpoint() {
    let protocol = default_protocol_for_endpoint(Some("http://127.0.0.1:4317"));
    assert_eq!(protocol, "grpc");
    assert!(METRIC_PREFIX_SYSTEM.starts_with("system"));
}

#[test]
fn default_protocol_none_uses_grpc() {
    assert_eq!(default_protocol_for_endpoint(None), "grpc");
}

#[test]
fn default_protocol_host_only_endpoint_is_grpc() {
    assert_eq!(
        default_protocol_for_endpoint(Some("http://127.0.0.1:4317")),
        "grpc"
    );
}

#[test]
fn default_protocol_without_scheme_uses_grpc() {
    assert_eq!(
        default_protocol_for_endpoint(Some("127.0.0.1:4318/v1/metrics")),
        "grpc"
    );
}

#[test]
fn build_meter_provider_grpc_succeeds() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let settings = test_settings("grpc");
    rt.block_on(async {
        let result = build_meter_provider(&settings);
        assert!(result.is_ok(), "grpc builder: {result:?}");
    });
}

#[test]
fn build_meter_provider_http_protobuf_succeeds() {
    let mut settings = test_settings("http/protobuf");
    settings.otlp_endpoint = "http://127.0.0.1:4318/v1/metrics".to_string();
    let result = build_meter_provider(&settings);
    assert!(result.is_ok(), "http/protobuf builder: {result:?}");
}

#[test]
fn build_meter_provider_rejects_unknown_protocol() {
    let settings = test_settings("h2");
    let err = build_meter_provider(&settings).unwrap_err();
    assert!(
        err.to_string().contains("unsupported OTLP protocol"),
        "{err}"
    );
}

#[test]
fn hostname_returns_non_empty() {
    let h = hostname();
    assert!(!h.trim().is_empty());
}

#[test]
fn build_meter_provider_honors_export_interval() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let mut settings = test_settings("grpc");
    settings.export_interval = Some(Duration::from_secs(30));
    rt.block_on(async {
        let result = build_meter_provider(&settings);
        assert!(result.is_ok(), "periodic reader with interval: {result:?}");
    });
}

#[test]
fn init_meter_provider_sets_global_provider() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let settings = test_settings("grpc");
    rt.block_on(async {
        let result = init_meter_provider(&settings);
        assert!(result.is_ok(), "init meter provider: {result:?}");
    });
}

#[test]
fn build_meter_provider_grpc_invalid_endpoint_returns_error() {
    let mut settings = test_settings("grpc");
    settings.otlp_endpoint = "not a valid endpoint".to_string();
    let err = build_meter_provider(&settings).unwrap_err();
    assert!(!err.to_string().trim().is_empty());
}

#[test]
fn build_meter_provider_http_invalid_endpoint_returns_error() {
    let mut settings = test_settings("http/protobuf");
    settings.otlp_endpoint = "http:// bad-endpoint".to_string();
    let err = build_meter_provider(&settings).unwrap_err();
    assert!(!err.to_string().trim().is_empty());
}

#[test]
fn build_meter_provider_http_extreme_timeout_uses_safe_client_path() {
    let mut settings = test_settings("http/protobuf");
    settings.otlp_endpoint = "http://127.0.0.1:4318/v1/metrics".to_string();
    settings.otlp_timeout = Some(Duration::MAX);
    let result = build_meter_provider(&settings);
    assert!(
        result.is_ok(),
        "http exporter with extreme timeout: {result:?}"
    );
}

#[test]
fn init_meter_provider_propagates_build_errors() {
    let mut settings = test_settings("grpc");
    settings.otlp_endpoint = "not a valid endpoint".to_string();
    let err = init_meter_provider(&settings).unwrap_err();
    assert!(!err.to_string().trim().is_empty());
}

#[test]
fn build_tracer_provider_grpc_succeeds() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let settings = test_settings("grpc");
    rt.block_on(async {
        let result = build_tracer_provider(&settings);
        assert!(result.is_ok(), "grpc tracer builder: {result:?}");
    });
}

#[test]
fn build_tracer_provider_http_protobuf_succeeds() {
    let mut settings = test_settings("http/protobuf");
    settings.otlp_endpoint = "http://127.0.0.1:4318/v1/traces".to_string();
    let result = build_tracer_provider(&settings);
    assert!(result.is_ok(), "http/protobuf tracer builder: {result:?}");
}

#[test]
fn build_tracer_provider_rejects_unknown_protocol() {
    let settings = test_settings("h2");
    let err = build_tracer_provider(&settings).unwrap_err();
    assert!(
        err.to_string().contains("unsupported OTLP protocol"),
        "{err}"
    );
}

#[test]
fn init_tracer_provider_sets_global_provider() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let settings = test_settings("grpc");
    rt.block_on(async {
        let result = init_tracer_provider(&settings);
        assert!(result.is_ok(), "init tracer provider: {result:?}");
    });
}

#[test]
fn init_tracer_provider_propagates_build_errors() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let mut settings = test_settings("grpc");
    settings.otlp_endpoint = "not a valid endpoint".to_string();
    rt.block_on(async {
        let err = init_tracer_provider(&settings).unwrap_err();
        assert!(!err.to_string().trim().is_empty());
    });
}

#[test]
fn archive_storage_config_disabled_sets_expected_defaults() {
    let disabled = ArchiveStorageConfig::disabled("metrics");
    assert!(!disabled.enabled);
    assert_eq!(disabled.archive_dir, "");
    assert_eq!(disabled.max_file_bytes, 0);
    assert_eq!(disabled.retain_files, 0);
    assert_eq!(disabled.file_stem, "metrics");
}

#[test]
fn json_archive_writer_from_config_copies_fields_and_initializes_state() {
    let config = ArchiveStorageConfig {
        enabled: true,
        archive_dir: "/tmp/archive".to_string(),
        max_file_bytes: 128,
        retain_files: 3,
        file_stem: "host".to_string(),
    };
    let writer = JsonArchiveWriter::from_config(&config);

    assert!(writer.healthy);
    assert!(writer.last_error.is_none());
    assert_eq!(writer.total_records, 0);
    assert_eq!(writer.total_bytes, 0);
}

#[test]
fn json_archive_writer_disabled_noops_without_mutating_state() {
    let config = ArchiveStorageConfig::disabled("metrics");
    let mut writer = JsonArchiveWriter::from_config(&config);

    writer.write_json_line(&serde_json::json!({"key": "value"}));

    assert_eq!(writer.total_records, 0);
    assert_eq!(writer.total_bytes, 0);
    assert!(writer.healthy);
    assert!(writer.last_error.is_none());
}

#[test]
fn json_archive_writer_writes_and_tracks_bytes() {
    let dir = unique_temp_dir("host_collectors_archive_ok");
    fs::create_dir_all(&dir).expect("create temp directory");

    let config = ArchiveStorageConfig {
        enabled: true,
        archive_dir: dir.to_string_lossy().into_owned(),
        max_file_bytes: 1024,
        retain_files: 0,
        file_stem: "metrics".to_string(),
    };
    let mut writer = JsonArchiveWriter::from_config(&config);
    let payload = serde_json::json!({"name": "cpu", "value": 1});
    let expected_line = serde_json::to_string(&payload).expect("serialize payload");

    writer.write_json_line(&payload);
    writer.write_json_line(&payload);

    assert!(writer.healthy);
    assert!(writer.last_error.is_none());
    assert_eq!(writer.total_records, 2);
    assert_eq!(writer.total_bytes, (expected_line.len() as u64 + 1) * 2);

    fs::remove_dir_all(&dir).expect("remove temp directory");
}

#[test]
fn json_archive_writer_sets_health_false_when_archive_dir_is_invalid() {
    let dir = unique_temp_dir("host_collectors_archive_err");
    fs::create_dir_all(&dir).expect("create temp directory");
    let blocker = dir.join("not_a_dir");
    fs::write(&blocker, b"file blocks create_dir_all").expect("create blocker file");

    let config = ArchiveStorageConfig {
        enabled: true,
        archive_dir: blocker.to_string_lossy().into_owned(),
        max_file_bytes: 1024,
        retain_files: 1,
        file_stem: "metrics".to_string(),
    };
    let mut writer = JsonArchiveWriter::from_config(&config);
    writer.write_json_line(&serde_json::json!({"name": "cpu"}));

    assert!(!writer.healthy);
    assert!(writer.last_error.is_some());
    assert_eq!(writer.total_records, 0);
    assert_eq!(writer.total_bytes, 0);

    fs::remove_dir_all(&dir).expect("remove temp directory");
}

#[test]
fn json_archive_writer_rotates_and_prunes_files() {
    let dir = unique_temp_dir("host_collectors_archive_rotate");
    fs::create_dir_all(&dir).expect("create temp directory");
    let base = dir.join("metrics.ndjson");
    fs::write(&base, vec![b'x'; 20]).expect("seed oversized current file");
    fs::write(format!("{}.1", base.to_string_lossy()), b"older-1").expect("seed rotated file 1");
    fs::write(format!("{}.2", base.to_string_lossy()), b"older-2").expect("seed rotated file 2");
    fs::write(format!("{}.4", base.to_string_lossy()), b"prune-me").expect("seed prune candidate");

    let config = ArchiveStorageConfig {
        enabled: true,
        archive_dir: dir.to_string_lossy().into_owned(),
        max_file_bytes: 10,
        retain_files: 2,
        file_stem: "metrics".to_string(),
    };
    let mut writer = JsonArchiveWriter::from_config(&config);
    writer.write_json_line(&serde_json::json!({"rotated": true}));

    assert!(base.exists());
    assert!(PathBuf::from(format!("{}.1", base.to_string_lossy())).exists());
    assert!(PathBuf::from(format!("{}.2", base.to_string_lossy())).exists());
    assert!(PathBuf::from(format!("{}.3", base.to_string_lossy())).exists());
    assert!(!PathBuf::from(format!("{}.4", base.to_string_lossy())).exists());

    fs::remove_dir_all(&dir).expect("remove temp directory");
}

#[test]
fn stdout_span_exporter_exports_span_data_and_rejects_after_shutdown() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let mut exporter = StdoutSpanExporter::default();
    let resource = Resource::builder_empty()
        .with_schema_url(
            [KeyValue::new("service.name", "host-collectors")],
            "https://opentelemetry.io/schemas/1.0.0",
        )
        .build();
    exporter.set_resource(&resource);

    let root = make_span_data("root", SpanId::INVALID, vec![]);
    let child = make_span_data(
        "child",
        SpanId::from_bytes([0, 0, 0, 0, 0, 0, 0, 7]),
        vec![KeyValue::new("component", "unit-test")],
    );
    rt.block_on(async {
        exporter
            .export(vec![root, child])
            .await
            .expect("export should succeed before shutdown");
    });

    exporter.shutdown().expect("shutdown succeeds");
    let err = rt
        .block_on(async { exporter.export(vec![]).await })
        .expect_err("export after shutdown must fail");
    assert!(matches!(err, OTelSdkError::AlreadyShutdown));
}

#[test]
fn build_tracer_provider_stdout_succeeds() {
    let settings = test_settings("stdout");
    let result = build_tracer_provider(&settings);
    assert!(result.is_ok(), "stdout tracer builder: {result:?}");
}
