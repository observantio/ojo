use crate::config::Config;
use anyhow::Result;
use host_collectors::{init_meter_provider as init_shared_provider, OtlpSettings};
use opentelemetry_sdk::metrics::SdkMeterProvider;

pub fn init_meter_provider(cfg: &Config) -> Result<SdkMeterProvider> {
    init_shared_provider(&OtlpSettings {
        service_name: cfg.service_name.clone(),
        instance_id: cfg.instance_id.clone(),
        otlp_endpoint: cfg.otlp_endpoint.clone(),
        otlp_protocol: cfg.otlp_protocol.clone(),
        otlp_headers: cfg.otlp_headers.clone(),
        otlp_compression: cfg.otlp_compression.clone(),
        otlp_timeout: cfg.otlp_timeout,
        export_interval: cfg.export_interval,
        export_timeout: cfg.export_timeout,
    })
}
