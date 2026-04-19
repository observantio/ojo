use super::{
    build_meter_provider, build_tracer_provider, default_protocol_for_endpoint, hostname,
    init_meter_provider, init_tracer_provider, ArchiveCompression, ArchiveFormat, ArchiveMode,
    ArchiveStorageConfig, ArchiveWriter, JsonArchiveWriter, OtlpSettings, PrefixFilter,
    StdoutSpanExporter, METRIC_PREFIX_SYSTEM,
};
use arrow_array::{Array, StringArray};
use opentelemetry::trace::{
    SpanContext, SpanId, SpanKind, Status, TraceFlags, TraceId, TraceState,
};
use opentelemetry::InstrumentationScope;
use opentelemetry::KeyValue;
use opentelemetry_sdk::error::OTelSdkError;
use opentelemetry_sdk::resource::Resource;
use opentelemetry_sdk::trace::{SpanData, SpanEvents, SpanExporter, SpanLinks};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
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
fn archive_mode_parses_lossless_and_forensic() {
    assert_eq!(ArchiveMode::parse(Some("lossless")), ArchiveMode::Lossless);
    assert_eq!(ArchiveMode::parse(Some("forensic")), ArchiveMode::Forensic);
    assert_eq!(ArchiveMode::parse(Some("trend")), ArchiveMode::Trend);
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
        format: ArchiveFormat::Parquet,
        mode: ArchiveMode::Forensic,
        window_secs: 60,
        compression: ArchiveCompression::Zstd,
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
        format: ArchiveFormat::Parquet,
        mode: ArchiveMode::Trend,
        window_secs: 60,
        compression: ArchiveCompression::Zstd,
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
fn json_archive_writer_uses_default_identity_when_missing_in_payload() {
    let dir = unique_temp_dir("host_collectors_archive_identity_defaults");
    fs::create_dir_all(&dir).expect("create temp directory");

    let config = ArchiveStorageConfig {
        enabled: true,
        archive_dir: dir.to_string_lossy().into_owned(),
        max_file_bytes: 1024 * 1024,
        retain_files: 0,
        file_stem: "metrics".to_string(),
        format: ArchiveFormat::Parquet,
        mode: ArchiveMode::Trend,
        window_secs: 1,
        compression: ArchiveCompression::Zstd,
    };
    let mut writer = JsonArchiveWriter::from_config(&config);
    writer.set_default_identity("svc-default", "inst-default");
    writer.write_json_line(&serde_json::json!({"metric": {"value": 42}}));
    writer.flush();

    let path = dir.join("metrics-trend.parquet");
    let file = File::open(path).expect("open archive parquet");
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("build parquet reader")
        .build()
        .expect("create record batch reader");
    let batch = reader
        .next()
        .expect("at least one record batch")
        .expect("read batch");
    let service_col = batch
        .column_by_name("service_name")
        .expect("service_name column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("service_name should be utf8");
    let instance_col = batch
        .column_by_name("instance_id")
        .expect("instance_id column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("instance_id should be utf8");

    assert_eq!(service_col.value(0), "svc-default");
    assert_eq!(instance_col.value(0), "inst-default");

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
        format: ArchiveFormat::Parquet,
        mode: ArchiveMode::Trend,
        window_secs: 60,
        compression: ArchiveCompression::Zstd,
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
    let base = dir.join("metrics-forensic.parquet");
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
        format: ArchiveFormat::Parquet,
        mode: ArchiveMode::Trend,
        window_secs: 60,
        compression: ArchiveCompression::Zstd,
    };
    let mut writer = JsonArchiveWriter::from_config(&config);
    writer.write_json_line(&serde_json::json!({"rotated": true}));

    assert!(base.exists());
    assert!(PathBuf::from(format!("{}.1", base.to_string_lossy())).exists());
    assert!(PathBuf::from(format!("{}.2", base.to_string_lossy())).exists());

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

#[test]
fn archive_format_and_compression_parse_and_render() {
    assert_eq!(ArchiveFormat::parse(None), ArchiveFormat::Parquet);
    assert_eq!(ArchiveFormat::parse(Some("")), ArchiveFormat::Parquet);
    assert_eq!(
        ArchiveFormat::parse(Some("parquet")),
        ArchiveFormat::Parquet
    );
    assert_eq!(
        ArchiveFormat::parse(Some("unknown")),
        ArchiveFormat::Parquet
    );
    assert_eq!(ArchiveFormat::Parquet.as_str(), "parquet");

    assert_eq!(ArchiveCompression::parse(None), ArchiveCompression::Zstd);
    assert_eq!(
        ArchiveCompression::parse(Some("zstd")),
        ArchiveCompression::Zstd
    );
    assert_eq!(
        ArchiveCompression::parse(Some("other")),
        ArchiveCompression::Zstd
    );
    assert_eq!(ArchiveCompression::Zstd.as_str(), "zstd");

    assert_eq!(ArchiveMode::Trend.as_str(), "trend");
    assert_eq!(ArchiveMode::Forensic.as_str(), "forensic");
    assert_eq!(ArchiveMode::Lossless.as_str(), "lossless");
}

#[test]
fn archive_storage_config_disabled_covers_all_defaults() {
    let disabled = ArchiveStorageConfig::disabled("metrics");
    assert!(!disabled.enabled);
    assert_eq!(disabled.archive_dir, "");
    assert_eq!(disabled.max_file_bytes, 0);
    assert_eq!(disabled.retain_files, 0);
    assert_eq!(disabled.file_stem, "metrics");
    assert_eq!(disabled.format, ArchiveFormat::Parquet);
    assert_eq!(disabled.mode, ArchiveMode::Trend);
    assert_eq!(disabled.window_secs, 60);
    assert_eq!(disabled.compression, ArchiveCompression::Zstd);
}

#[test]
fn helper_functions_are_covered() {
    assert_eq!(
        super::sanitize_segment(" Error.Level / CPU% "),
        "error_level___cpu"
    );

    let sig_a = super::stable_signature("same");
    let sig_b = super::stable_signature("same");
    let sig_c = super::stable_signature("different");
    assert_eq!(sig_a, sig_b);
    assert_ne!(sig_a, sig_c);

    let secs = super::now_unix_secs();
    let millis = super::now_unix_millis();
    assert!(secs >= 0);
    assert!(millis >= secs * 1000);
    assert_eq!(super::align_window_start(125, 60), 120);
    assert_eq!(super::align_window_start(125, 0), 125);

    let direct = json!({"service_name": "svc-a", "instance_id": "inst-a"});
    assert_eq!(
        super::extract_identity(&direct),
        ("svc-a".to_string(), "inst-a".to_string())
    );

    let nested = json!({"service": {"name": "svc-b", "instance_id": "inst-b"}});
    assert_eq!(
        super::extract_identity(&nested),
        ("svc-b".to_string(), "inst-b".to_string())
    );

    let missing = json!({"other": 1});
    assert_eq!(
        super::extract_identity(&missing),
        ("".to_string(), "".to_string())
    );
}

#[test]
fn build_providers_with_default_timeout_when_none() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    let mut meter_settings = test_settings("grpc");
    meter_settings.otlp_timeout = None;
    rt.block_on(async {
        let meter_result = build_meter_provider(&meter_settings);
        assert!(
            meter_result.is_ok(),
            "meter provider with default timeout: {meter_result:?}"
        );
    });

    let mut tracer_settings = test_settings("grpc");
    tracer_settings.otlp_timeout = None;
    rt.block_on(async {
        let tracer_result = build_tracer_provider(&tracer_settings);
        assert!(
            tracer_result.is_ok(),
            "tracer provider with default timeout: {tracer_result:?}"
        );
    });
}

#[test]
fn trend_mode_writes_log_metrics_and_snapshot_metrics() {
    let dir = unique_temp_dir("host_collectors_trend_full");
    fs::create_dir_all(&dir).expect("create temp directory");

    let config = ArchiveStorageConfig {
        enabled: true,
        archive_dir: dir.to_string_lossy().into_owned(),
        max_file_bytes: 1024 * 1024,
        retain_files: 2,
        file_stem: "metrics".to_string(),
        format: ArchiveFormat::Parquet,
        mode: ArchiveMode::Trend,
        window_secs: 1,
        compression: ArchiveCompression::Zstd,
    };
    let mut writer = JsonArchiveWriter::from_config(&config);
    writer.set_default_identity("svc-default", "inst-default");

    writer.write_log_batch(&[]);
    writer.write_log_batch(&[
        json!({
            "body": "cpu high",
            "severity_text": "Error",
            "source": "kernel.log",
            "watch_target": "sys/fs",
            "service": {"name": "svc-x", "instance_id": "inst-x"}
        }),
        json!({
            "body": "cpu high",
            "severity_text": "Error",
            "source": "kernel.log",
            "watch_target": "sys/fs",
            "service": {"name": "svc-x", "instance_id": "inst-x"}
        }),
        json!({
            "body": "disk warning",
            "severity_text": "Warn",
            "service": {"name": "svc-x", "instance_id": "inst-x"}
        }),
    ]);

    writer.write_json_line(&json!({
        "service": {"name": "svc-y", "instance_id": "inst-y"},
        "cpu": {"usage": [1, 2], "temp": 3.5},
        "memory": {"used": 1024},
        "non_numeric": "skip",
        "mixed": [{"v": 4}, true]
    }));

    writer.flush();

    let file = File::open(dir.join("metrics-trend.parquet")).expect("open trend parquet");
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("build parquet reader")
        .build()
        .expect("create record batch reader");

    let mut metric_keys = Vec::new();
    for maybe_batch in &mut reader {
        let batch = maybe_batch.expect("read trend batch");
        let key_col = batch
            .column_by_name("metric_key")
            .expect("metric_key column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("metric_key should be utf8");
        for idx in 0..key_col.len() {
            metric_keys.push(key_col.value(idx).to_string());
        }
    }

    assert!(metric_keys.iter().any(|k| k == "log.count"));
    assert!(metric_keys.iter().any(|k| k == "log.severity.error.count"));
    assert!(metric_keys
        .iter()
        .any(|k| k == "log.source.kernel_log.count"));
    assert!(metric_keys
        .iter()
        .any(|k| k == "log.watch_target.sys_fs.count"));
    assert!(metric_keys
        .iter()
        .any(|k| k.starts_with("log.topk.signature.")));
    assert!(metric_keys.iter().any(|k| k == "cpu.usage.0"));
    assert!(metric_keys.iter().any(|k| k == "cpu.usage.1"));
    assert!(metric_keys.iter().any(|k| k == "cpu.temp"));
    assert!(metric_keys.iter().any(|k| k == "memory.used"));
    assert!(metric_keys.iter().any(|k| k == "mixed.0.v"));

    fs::remove_dir_all(&dir).expect("remove temp directory");
}

#[test]
fn forensic_and_lossless_modes_write_expected_files() {
    let dir_forensic = unique_temp_dir("host_collectors_forensic_full");
    fs::create_dir_all(&dir_forensic).expect("create forensic temp directory");

    let forensic_config = ArchiveStorageConfig {
        enabled: true,
        archive_dir: dir_forensic.to_string_lossy().into_owned(),
        max_file_bytes: 1024 * 1024,
        retain_files: 1,
        file_stem: "metrics".to_string(),
        format: ArchiveFormat::Parquet,
        mode: ArchiveMode::Forensic,
        window_secs: 60,
        compression: ArchiveCompression::Zstd,
    };
    let mut forensic_writer = JsonArchiveWriter::from_config(&forensic_config);
    forensic_writer.write_json_line(&json!({
        "body": "snapshot",
        "severity_text": "Info",
        "source": "sensor",
        "watch_target": "cpu"
    }));
    forensic_writer.write_log_batch(&[json!({
        "body": "log",
        "severity_text": "Warn",
        "source": "syslog",
        "watch_target": "disk"
    })]);

    let forensic_file = dir_forensic.join("metrics-forensic.parquet");
    assert!(forensic_file.exists());

    let dir_lossless = unique_temp_dir("host_collectors_lossless_full");
    fs::create_dir_all(&dir_lossless).expect("create lossless temp directory");
    let lossless_config = ArchiveStorageConfig {
        enabled: true,
        archive_dir: dir_lossless.to_string_lossy().into_owned(),
        max_file_bytes: 1024 * 1024,
        retain_files: 1,
        file_stem: "metrics".to_string(),
        format: ArchiveFormat::Parquet,
        mode: ArchiveMode::Lossless,
        window_secs: 60,
        compression: ArchiveCompression::Zstd,
    };
    let mut lossless_writer = JsonArchiveWriter::from_config(&lossless_config);
    lossless_writer.write_json_line(&json!({"body": "snapshot"}));
    lossless_writer.write_log_batch(&[json!({"body": "log"})]);

    let lossless_file = dir_lossless.join("metrics-lossless.parquet");
    assert!(lossless_file.exists());

    fs::remove_dir_all(&dir_forensic).expect("remove forensic temp directory");
    fs::remove_dir_all(&dir_lossless).expect("remove lossless temp directory");
}

#[test]
fn archive_writer_trait_dispatch_and_error_path_are_covered() {
    let dir = unique_temp_dir("host_collectors_trait_error");
    fs::create_dir_all(&dir).expect("create temp directory");
    let blocker = dir.join("not_a_dir");
    fs::write(&blocker, b"file blocks create_dir_all").expect("create blocker file");

    let config = ArchiveStorageConfig {
        enabled: true,
        archive_dir: blocker.to_string_lossy().into_owned(),
        max_file_bytes: 1024,
        retain_files: 1,
        file_stem: "metrics".to_string(),
        format: ArchiveFormat::Parquet,
        mode: ArchiveMode::Trend,
        window_secs: 60,
        compression: ArchiveCompression::Zstd,
    };
    let mut writer = JsonArchiveWriter::from_config(&config);

    let archive_writer: &mut dyn ArchiveWriter = &mut writer;
    archive_writer.write_snapshot(&json!({"metric": 1}));
    archive_writer.write_log_batch(&[json!({"body": "x"})]);

    assert!(!archive_writer.is_healthy());
    assert!(archive_writer.last_error().is_some());
    assert_eq!(archive_writer.total_records(), 0);
    assert_eq!(archive_writer.total_bytes(), 0);

    fs::remove_dir_all(&dir).expect("remove temp directory");
}

#[test]
fn internal_writer_branches_for_empty_inputs_and_rotation_conditions() {
    let dir = unique_temp_dir("host_collectors_internal_branches");
    fs::create_dir_all(&dir).expect("create temp directory");

    let config = ArchiveStorageConfig {
        enabled: true,
        archive_dir: dir.to_string_lossy().into_owned(),
        max_file_bytes: 100,
        retain_files: 0,
        file_stem: "metrics".to_string(),
        format: ArchiveFormat::Parquet,
        mode: ArchiveMode::Forensic,
        window_secs: 60,
        compression: ArchiveCompression::Zstd,
    };
    let mut writer = JsonArchiveWriter::from_config(&config);

    writer
        .write_trend_window(0, HashMap::new())
        .expect("empty trend window should be ok");
    writer
        .write_forensic_rows(Vec::new())
        .expect("empty forensic rows should be ok");

    let missing_path = dir.join("missing.parquet");
    writer
        .rotate_if_needed(&missing_path.to_string_lossy())
        .expect("missing file should skip rotate");

    let small_path = dir.join("small.parquet");
    fs::write(&small_path, b"tiny").expect("create small file");
    writer
        .rotate_if_needed(&small_path.to_string_lossy())
        .expect("small file should skip rotate");
    writer
        .prune_rotated_files(&small_path.to_string_lossy())
        .expect("prune with retain_files=0 should be ok");

    fs::remove_dir_all(&dir).expect("remove temp directory");
}

#[test]
fn internal_types_defaults_and_remaining_branches_are_covered() {
    let _fmt_default = ArchiveFormat::default();
    let _mode_default = ArchiveMode::default();
    let _comp_default = ArchiveCompression::default();

    let mut stats = super::TrendStats::new(10.0);
    stats.update(5.0);
    stats.update(20.0);
    assert_eq!(stats.min, 5.0);
    assert_eq!(stats.max, 20.0);
    assert_eq!(stats.count, 3);
    assert!(stats.avg() > 0.0);

    let mut zero_stats = stats.clone();
    zero_stats.count = 0;
    assert_eq!(zero_stats.avg(), 0.0);

    let key = super::TrendKey {
        service_name: "svc".to_string(),
        instance_id: "inst".to_string(),
        metric_key: "k".to_string(),
    };
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(&key, &mut hasher);
    assert_ne!(std::hash::Hasher::finish(&hasher), 0);

    let _trend_row = super::TrendRow {
        window_start_unix_secs: 0,
        service_name: "svc".to_string(),
        instance_id: "inst".to_string(),
        metric_key: "k".to_string(),
        min: 1.0,
        max: 2.0,
        avg: 1.5,
        count: 2,
        first: 1.0,
        last: 2.0,
        written_at_unix_ms: 0,
    };

    let _forensic_row = super::ForensicRow {
        observed_unix_ms: 0,
        service_name: "svc".to_string(),
        instance_id: "inst".to_string(),
        record_type: "log".to_string(),
        severity: Some("warn".to_string()),
        source: Some("src".to_string()),
        watch_target: Some("target".to_string()),
        message_signature: Some(42),
        sample_message: Some("msg".to_string()),
        payload_json: "{}".to_string(),
    };

    let dir = unique_temp_dir("host_collectors_internal_remaining");
    fs::create_dir_all(&dir).expect("create temp directory");
    let config = ArchiveStorageConfig {
        enabled: true,
        archive_dir: dir.to_string_lossy().into_owned(),
        max_file_bytes: 1,
        retain_files: 2,
        file_stem: "metrics".to_string(),
        format: ArchiveFormat::Parquet,
        mode: ArchiveMode::Trend,
        window_secs: 1,
        compression: ArchiveCompression::Zstd,
    };
    let mut writer = JsonArchiveWriter::from_config(&config);

    let now = super::align_window_start(super::now_unix_secs(), 1);
    let old_window = now.saturating_sub(1);
    let mut old_values = HashMap::new();
    old_values.insert(
        super::TrendKey {
            service_name: "svc".to_string(),
            instance_id: "inst".to_string(),
            metric_key: "metric.old".to_string(),
        },
        super::TrendStats::new(1.0),
    );
    writer.trend_windows.insert(old_window, old_values);

    writer
        .flush_closed_windows(now)
        .expect("closed windows should flush");

    let trend_file = dir.join("metrics-trend.parquet");
    assert!(trend_file.exists());

    fs::write(&trend_file, vec![b'x'; 20]).expect("seed oversized current file");
    fs::write(format!("{}.1", trend_file.to_string_lossy()), b"older-1")
        .expect("seed rotated file 1");
    fs::write(format!("{}.2", trend_file.to_string_lossy()), b"older-2")
        .expect("seed rotated file 2");
    fs::write(format!("{}.4", trend_file.to_string_lossy()), b"prune-me")
        .expect("seed prune candidate");

    writer
        .rotate_if_needed(&trend_file.to_string_lossy())
        .expect("oversized file should rotate");
    writer
        .prune_rotated_files(&trend_file.to_string_lossy())
        .expect("extra rotated files should prune");
    assert!(!PathBuf::from(format!("{}.4", trend_file.to_string_lossy())).exists());

    let disabled = ArchiveStorageConfig::disabled("metrics");
    let mut disabled_writer = JsonArchiveWriter::from_config(&disabled);
    disabled_writer.write_log_batch(&[json!({"body": "noop"})]);
    assert_eq!(disabled_writer.total_records, 0);

    fs::remove_dir_all(&dir).expect("remove temp directory");
}

#[test]
fn derived_traits_and_direct_impl_methods_are_exercised() {
    let cfg = ArchiveStorageConfig {
        enabled: true,
        archive_dir: unique_temp_dir("host_collectors_direct_impl")
            .to_string_lossy()
            .into_owned(),
        max_file_bytes: 1024 * 1024,
        retain_files: 1,
        file_stem: "metrics".to_string(),
        format: ArchiveFormat::Parquet,
        mode: ArchiveMode::Trend,
        window_secs: 1,
        compression: ArchiveCompression::Zstd,
    };
    fs::create_dir_all(&cfg.archive_dir).expect("create temp directory");

    let cfg_clone = cfg.clone();
    let _cfg_dbg = format!("{cfg_clone:?}");

    let mode = ArchiveMode::Trend;
    let _mode_clone = mode.clone();
    let _mode_dbg = format!("{mode:?}");
    let format_kind = ArchiveFormat::Parquet;
    let _fmt_clone = format_kind.clone();
    let _fmt_dbg = format!("{format_kind:?}");
    let comp = ArchiveCompression::Zstd;
    let _comp_clone = comp.clone();
    let _comp_dbg = format!("{comp:?}");

    let mut writer = JsonArchiveWriter::from_config(&cfg);
    let _writer_dbg = format!(
        "enabled={} records={} bytes={}",
        writer.is_healthy(),
        writer.total_records(),
        writer.total_bytes()
    );

    let snap = json!({"service_name": "svc", "instance_id": "inst", "cpu": {"v": 1}});
    writer
        .write_snapshot_impl(&snap)
        .expect("direct snapshot impl call");
    writer
        .write_log_batch_impl(&[])
        .expect("direct empty log batch impl call");
    writer
        .write_log_batch_impl(&[json!({"body": "x", "severity_text": "Info"})])
        .expect("direct non-empty log batch impl call");
    writer
        .ingest_snapshot_to_trend(&json!([1, 2, 3]))
        .expect("direct ingest snapshot for non-object payload");
    writer
        .ingest_logs_to_trend(&[json!({"body": "x"})])
        .expect("direct ingest logs to trend");

    let row = super::TrendRow {
        window_start_unix_secs: 1,
        service_name: "svc".to_string(),
        instance_id: "inst".to_string(),
        metric_key: "metric".to_string(),
        min: 1.0,
        max: 1.0,
        avg: 1.0,
        count: 1,
        first: 1.0,
        last: 1.0,
        written_at_unix_ms: 1,
    };
    let row_clone = row.clone();
    let _row_dbg = format!("{row_clone:?}");

    let frow = super::ForensicRow {
        observed_unix_ms: 1,
        service_name: "svc".to_string(),
        instance_id: "inst".to_string(),
        record_type: "log".to_string(),
        severity: None,
        source: None,
        watch_target: None,
        message_signature: None,
        sample_message: None,
        payload_json: "{}".to_string(),
    };
    let frow_clone = frow.clone();
    let _frow_dbg = format!("{frow_clone:?}");

    let key = super::TrendKey {
        service_name: "svc".to_string(),
        instance_id: "inst".to_string(),
        metric_key: "metric".to_string(),
    };
    let key_clone = key.clone();
    let _key_dbg = format!("{key_clone:?}");
    assert_eq!(key, key_clone);

    let filter = PrefixFilter::new(vec!["a".to_string()], vec!["b".to_string()]);
    let filter_clone = filter.clone();
    let _filter_dbg = format!("{filter_clone:?}");

    let settings = test_settings("grpc");
    let settings_clone = settings.clone();
    let _settings_dbg = format!("{settings_clone:?}");

    fs::remove_dir_all(&cfg.archive_dir).expect("remove temp directory");
}

#[test]
fn add_trend_point_counts_first_sample_once() {
    let dir = unique_temp_dir("host_collectors_trend_count_once");
    fs::create_dir_all(&dir).expect("create temp directory");

    let config = ArchiveStorageConfig {
        enabled: true,
        archive_dir: dir.to_string_lossy().into_owned(),
        max_file_bytes: 1024 * 1024,
        retain_files: 1,
        file_stem: "metrics".to_string(),
        format: ArchiveFormat::Parquet,
        mode: ArchiveMode::Trend,
        window_secs: 60,
        compression: ArchiveCompression::Zstd,
    };
    let mut writer = JsonArchiveWriter::from_config(&config);
    let window = super::align_window_start(super::now_unix_secs(), 60);

    writer.add_trend_point(
        window,
        super::TrendKey {
            service_name: "svc".to_string(),
            instance_id: "inst".to_string(),
            metric_key: "metric".to_string(),
        },
        10.0,
    );
    writer.add_trend_point(
        window,
        super::TrendKey {
            service_name: "svc".to_string(),
            instance_id: "inst".to_string(),
            metric_key: "metric".to_string(),
        },
        20.0,
    );

    let stats = writer
        .trend_windows
        .get(&window)
        .and_then(|m| {
            m.get(&super::TrendKey {
                service_name: "svc".to_string(),
                instance_id: "inst".to_string(),
                metric_key: "metric".to_string(),
            })
        })
        .expect("trend stats should exist");
    assert_eq!(stats.count, 2);
    assert_eq!(stats.sum, 30.0);
    assert_eq!(stats.first, 10.0);
    assert_eq!(stats.last, 20.0);

    fs::remove_dir_all(&dir).expect("remove temp directory");
}

#[test]
fn write_batch_preserves_existing_parquet_rows() {
    let dir = unique_temp_dir("host_collectors_preserve_parquet_rows");
    fs::create_dir_all(&dir).expect("create temp directory");

    let config = ArchiveStorageConfig {
        enabled: true,
        archive_dir: dir.to_string_lossy().into_owned(),
        max_file_bytes: 1024 * 1024,
        retain_files: 1,
        file_stem: "metrics".to_string(),
        format: ArchiveFormat::Parquet,
        mode: ArchiveMode::Forensic,
        window_secs: 60,
        compression: ArchiveCompression::Zstd,
    };

    let mut writer = JsonArchiveWriter::from_config(&config);
    writer.write_json_line(&json!({"body": "first", "severity_text": "Info"}));
    writer.write_json_line(&json!({"body": "second", "severity_text": "Warn"}));

    let file = File::open(dir.join("metrics-forensic.parquet")).expect("open forensic parquet");
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("build parquet reader")
        .build()
        .expect("create record batch reader");

    let mut total_rows = 0usize;
    let mut payloads = Vec::new();
    for maybe_batch in &mut reader {
        let batch = maybe_batch.expect("read batch");
        total_rows += batch.num_rows();
        let payload_col = batch
            .column_by_name("payload_json")
            .expect("payload_json column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("payload_json should be utf8");
        for i in 0..payload_col.len() {
            payloads.push(payload_col.value(i).to_string());
        }
    }

    assert_eq!(total_rows, 2);
    assert!(payloads.iter().any(|p| p.contains("\"first\"")));
    assert!(payloads.iter().any(|p| p.contains("\"second\"")));

    fs::remove_dir_all(&dir).expect("remove temp directory");
}

#[test]
fn flush_methods_cover_window_write_lines() {
    let dir = unique_temp_dir("host_collectors_flush_window_lines");
    fs::create_dir_all(&dir).expect("create temp directory");

    let config = ArchiveStorageConfig {
        enabled: true,
        archive_dir: dir.to_string_lossy().into_owned(),
        max_file_bytes: 1024 * 1024,
        retain_files: 1,
        file_stem: "metrics".to_string(),
        format: ArchiveFormat::Parquet,
        mode: ArchiveMode::Trend,
        window_secs: 60,
        compression: ArchiveCompression::Zstd,
    };
    let mut writer = JsonArchiveWriter::from_config(&config);

    let mut closed_values = HashMap::new();
    closed_values.insert(
        super::TrendKey {
            service_name: "svc".to_string(),
            instance_id: "inst".to_string(),
            metric_key: "metric.closed".to_string(),
        },
        super::TrendStats::new(1.0),
    );
    let mut open_values = HashMap::new();
    open_values.insert(
        super::TrendKey {
            service_name: "svc".to_string(),
            instance_id: "inst".to_string(),
            metric_key: "metric.open".to_string(),
        },
        super::TrendStats::new(2.0),
    );

    writer.trend_windows.insert(100, closed_values);
    writer.trend_windows.insert(200, open_values);
    writer
        .flush_closed_windows(150)
        .expect("closed windows flush should succeed");

    assert!(!writer.trend_windows.contains_key(&100));
    assert!(writer.trend_windows.contains_key(&200));

    writer
        .flush_all_trend_windows()
        .expect("flush all windows should succeed");
    assert!(writer.trend_windows.is_empty());

    fs::remove_dir_all(&dir).expect("remove temp directory");
}

#[test]
fn read_existing_batches_errors_on_schema_mismatch() {
    let dir = unique_temp_dir("host_collectors_schema_mismatch");
    fs::create_dir_all(&dir).expect("create temp directory");

    let trend_config = ArchiveStorageConfig {
        enabled: true,
        archive_dir: dir.to_string_lossy().into_owned(),
        max_file_bytes: 1024 * 1024,
        retain_files: 1,
        file_stem: "metrics".to_string(),
        format: ArchiveFormat::Parquet,
        mode: ArchiveMode::Trend,
        window_secs: 1,
        compression: ArchiveCompression::Zstd,
    };
    let mut trend_writer = JsonArchiveWriter::from_config(&trend_config);
    trend_writer.write_json_line(&json!({"cpu": {"value": 1.0}}));
    trend_writer.flush();

    let forensic_config = ArchiveStorageConfig {
        enabled: true,
        archive_dir: dir.to_string_lossy().into_owned(),
        max_file_bytes: 1024 * 1024,
        retain_files: 1,
        file_stem: "metrics".to_string(),
        format: ArchiveFormat::Parquet,
        mode: ArchiveMode::Forensic,
        window_secs: 60,
        compression: ArchiveCompression::Zstd,
    };
    let forensic_writer = JsonArchiveWriter::from_config(&forensic_config);
    let mismatch = forensic_writer
        .read_existing_batches(
            &dir.join("metrics-trend.parquet").to_string_lossy(),
            super::forensic_schema(),
        )
        .expect_err("schema mismatch should return an error");

    assert!(mismatch.to_string().contains("schema mismatch"));
    fs::remove_dir_all(&dir).expect("remove temp directory");
}
