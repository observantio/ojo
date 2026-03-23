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

#[cfg(not(target_os = "solaris"))]
use reqwest::blocking::Client;

pub const METRIC_PREFIX_SYSTEM: &str = "system.";
pub const METRIC_PREFIX_PROCESS: &str = "process.";

#[derive(Clone, Debug)]
pub struct PrefixFilter {
    include: Vec<String>,
    exclude: Vec<String>,
}

impl PrefixFilter {
    pub fn new(include: Vec<String>, exclude: Vec<String>) -> Self {
        Self { include, exclude }
    }

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

impl OtlpSettings {
    pub fn apply_env(&self) {
        std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", &self.otlp_endpoint);
        std::env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", &self.otlp_protocol);

        if !self.otlp_headers.is_empty() {
            let headers = self
                .otlp_headers
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(",");
            std::env::set_var("OTEL_EXPORTER_OTLP_HEADERS", headers);
        } else {
            std::env::remove_var("OTEL_EXPORTER_OTLP_HEADERS");
        }

        if let Some(compression) = &self.otlp_compression {
            std::env::set_var("OTEL_EXPORTER_OTLP_COMPRESSION", compression);
        } else {
            std::env::remove_var("OTEL_EXPORTER_OTLP_COMPRESSION");
        }

        if let Some(timeout) = self.otlp_timeout {
            std::env::set_var("OTEL_EXPORTER_OTLP_TIMEOUT", timeout.as_secs().to_string());
        } else {
            std::env::remove_var("OTEL_EXPORTER_OTLP_TIMEOUT");
        }

        if let Some(interval) = self.export_interval {
            std::env::set_var(
                "OTEL_METRIC_EXPORT_INTERVAL",
                interval.as_millis().to_string(),
            );
        } else {
            std::env::remove_var("OTEL_METRIC_EXPORT_INTERVAL");
        }

        if let Some(timeout) = self.export_timeout {
            std::env::set_var(
                "OTEL_METRIC_EXPORT_TIMEOUT",
                timeout.as_millis().to_string(),
            );
        } else {
            std::env::remove_var("OTEL_METRIC_EXPORT_TIMEOUT");
        }
    }
}

pub fn build_meter_provider(settings: &OtlpSettings) -> Result<SdkMeterProvider> {
    settings.apply_env();

    let exporter = match settings.otlp_protocol.as_str() {
        "http/protobuf" => {
            let mut builder = opentelemetry_otlp::MetricExporter::builder()
                .with_http()
                .with_protocol(Protocol::HttpBinary)
                .with_endpoint(settings.otlp_endpoint.clone());
            #[cfg(not(target_os = "solaris"))]
            {
                builder = builder.with_http_client(Client::new());
            }
            if let Some(timeout) = settings.otlp_timeout {
                builder = builder.with_timeout(timeout);
            }
            builder.build()?
        }
        _ => {
            let mut builder = opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_endpoint(settings.otlp_endpoint.clone());
            if let Some(timeout) = settings.otlp_timeout {
                builder = builder.with_timeout(timeout);
            }
            builder.build()?
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
        .unwrap_or_else(|_| "unknown-host".to_string())
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
    use super::{default_protocol_for_endpoint, PrefixFilter, METRIC_PREFIX_SYSTEM};

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
}
