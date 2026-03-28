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
                builder = builder.with_http_client(reqwest::blocking::Client::new());
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
mod tests;
