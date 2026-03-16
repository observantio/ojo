use crate::config::Config;
use anyhow::Result;
use opentelemetry::global;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{Protocol, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{
    metrics::{PeriodicReader, SdkMeterProvider},
    Resource,
};
use reqwest::blocking::Client;

pub fn init_meter_provider(cfg: &Config) -> Result<SdkMeterProvider> {
    let exporter = match cfg.otlp_protocol.as_str() {
        "http/protobuf" => {
            let mut builder = opentelemetry_otlp::MetricExporter::builder()
                .with_http()
                .with_http_client(Client::new())
                .with_protocol(Protocol::HttpBinary)
                .with_endpoint(cfg.otlp_endpoint.clone());
            if let Some(timeout) = cfg.export_timeout {
                builder = builder.with_timeout(timeout);
            }
            builder.build()?
        }
        _ => {
            let mut builder = opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_endpoint(cfg.otlp_endpoint.clone());
            if let Some(timeout) = cfg.export_timeout {
                builder = builder.with_timeout(timeout);
            }
            builder.build()?
        }
    };

    let mut reader_builder = PeriodicReader::builder(exporter);
    if let Some(interval) = cfg.export_interval {
        reader_builder = reader_builder.with_interval(interval);
    }
    let reader = reader_builder.build();

    let provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(
            Resource::builder_empty()
                .with_attributes([
                    KeyValue::new("service.name", cfg.service_name.clone()),
                    KeyValue::new("service.instance.id", cfg.instance_id.clone()),
                    KeyValue::new("host.name", hostname()),
                    KeyValue::new("os.type", std::env::consts::OS.to_string()),
                    KeyValue::new("host.arch", std::env::consts::ARCH.to_string()),
                ])
                .build(),
        )
        .build();

    global::set_meter_provider(provider.clone());
    Ok(provider)
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown-host".to_string())
}
