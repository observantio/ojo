use anyhow::Result;
use opentelemetry::global;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{Protocol, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{
    metrics::{PeriodicReader, SdkMeterProvider},
    Resource,
};
use std::collections::BTreeMap;
use std::time::Duration;

pub const METRIC_PREFIX_SYSTEM: &str = "system.";

#[derive(Clone, Debug)]
pub struct PrefixFilter {
    include: Vec<String>,
    exclude: Vec<String>,
}

impl PrefixFilter {
    pub fn new(include: Vec<String>, exclude: Vec<String>) -> Self {
        Self { include, exclude }
    }

    #[must_use]
    pub fn allows(&self, name: &str) -> bool {
        let include_match =
            self.include.is_empty() || self.include.iter().any(|p| name.starts_with(p));
        let exclude_match = self.exclude.iter().any(|p| name.starts_with(p));
        include_match && !exclude_match
    }
}

#[derive(Clone, Debug)]
pub struct OtlpSettings {
    pub service_name: String,
    pub instance_id: String,
    pub otlp_endpoint: String,
    pub otlp_protocol: String,
    pub otlp_headers: BTreeMap<String, String>,
    pub otlp_compression: Option<String>,
    pub otlp_timeout: Option<Duration>,
    pub export_interval: Option<Duration>,
    pub export_timeout: Option<Duration>,
}

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

pub fn build_meter_provider(settings: &OtlpSettings) -> Result<SdkMeterProvider> {
    let timeout = settings.otlp_timeout.unwrap_or(DEFAULT_TIMEOUT);

    let exporter = match settings.otlp_protocol.as_str() {
        "http/protobuf" => {
            let mut builder = opentelemetry_otlp::MetricExporter::builder()
                .with_http()
                .with_protocol(Protocol::HttpBinary)
                .with_endpoint(settings.otlp_endpoint.clone())
                .with_timeout(timeout);
            #[cfg(not(target_os = "solaris"))]
            {
                builder = builder.with_http_client(
                    reqwest::blocking::Client::builder()
                        .timeout(timeout)
                        .build()?,
                );
            }
            builder.build()?
        }
        "grpc" => opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint(settings.otlp_endpoint.clone())
            .with_timeout(timeout)
            .build()?,
        other => {
            anyhow::bail!(
                "unsupported OTLP protocol: {other:?}; expected \"http/protobuf\" or \"grpc\""
            );
        }
    };

    let mut reader_builder = PeriodicReader::builder(exporter);
    if let Some(interval) = settings.export_interval {
        reader_builder = reader_builder.with_interval(interval);
    }
    let reader = reader_builder.build();

    let provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(
            Resource::builder_empty()
                .with_attributes([
                    KeyValue::new("service.name", settings.service_name.clone()),
                    KeyValue::new("service.instance.id", settings.instance_id.clone()),
                    KeyValue::new("host.name", hostname()),
                    KeyValue::new("os.type", std::env::consts::OS.to_string()),
                    KeyValue::new("host.arch", std::env::consts::ARCH.to_string()),
                ])
                .build(),
        )
        .build();

    Ok(provider)
}

pub fn init_meter_provider(settings: &OtlpSettings) -> Result<SdkMeterProvider> {
    let provider = build_meter_provider(settings)?;
    global::set_meter_provider(provider.clone());
    Ok(provider)
}

pub fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| gethostname::gethostname().to_string_lossy().into_owned())
}

pub fn default_protocol_for_endpoint(endpoint: Option<&str>) -> String {
    match endpoint {
        Some(value) if has_non_root_path(value) => "http/protobuf".to_string(),
        _ => "grpc".to_string(),
    }
}

fn has_non_root_path(endpoint: &str) -> bool {
    if let Some((_, rest)) = endpoint.split_once("://") {
        if let Some((_, path)) = rest.split_once('/') {
            return !path.is_empty();
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::{
        build_meter_provider, default_protocol_for_endpoint, hostname, init_meter_provider,
        OtlpSettings, PrefixFilter, METRIC_PREFIX_SYSTEM,
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
}
