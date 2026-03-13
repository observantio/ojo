use crate::config::Config;
use anyhow::Result;
use opentelemetry::global;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{Protocol, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{metrics::{PeriodicReader, SdkMeterProvider}, Resource};
use reqwest::blocking::Client;
use std::fs;

pub fn init_meter_provider(cfg: &Config) -> Result<SdkMeterProvider> {
    let exporter = match cfg.otlp_protocol.as_str() {
        "http/protobuf" => opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_http_client(Client::new())
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(cfg.otlp_endpoint.clone())
            .build()?,
        _ => opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint(cfg.otlp_endpoint.clone())
            .build()?,
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
    fs::read_to_string("/proc/sys/kernel/hostname")
        .map(|value| value.trim().to_string())
        .unwrap_or_else(|_| "unknown-host".to_string())
}
