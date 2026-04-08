use super::{
    build_meter_provider, build_tracer_provider, default_protocol_for_endpoint, hostname,
    init_meter_provider, init_tracer_provider, OtlpSettings, PrefixFilter, METRIC_PREFIX_SYSTEM,
};
use std::collections::BTreeMap;
use std::time::Duration;

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
