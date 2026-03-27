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

#[cfg(test)]
mod tests {
    use super::init_meter_provider;
    use crate::config::Config;
    use std::collections::BTreeMap;
    use std::time::Duration;

    fn test_config(protocol: &str, endpoint: &str) -> Config {
        Config {
            service_name: "test-service".to_string(),
            instance_id: "test-instance".to_string(),
            poll_interval: Duration::from_secs(1),
            include_process_metrics: false,
            process_include_pid_label: false,
            process_include_command_label: true,
            process_include_state_label: true,
            otlp_endpoint: endpoint.to_string(),
            otlp_protocol: protocol.to_string(),
            otlp_headers: BTreeMap::new(),
            otlp_compression: None,
            otlp_timeout: Some(Duration::from_secs(1)),
            export_interval: None,
            export_timeout: None,
            metrics_include: vec![],
            metrics_exclude: vec![],
        }
    }

    #[test]
    fn init_meter_provider_rejects_unsupported_protocol() {
        let cfg = test_config("invalid", "http://127.0.0.1:4317");
        let err = init_meter_provider(&cfg).unwrap_err();
        assert!(
            err.to_string().contains("unsupported OTLP protocol"),
            "{err}"
        );
    }

    #[test]
    fn init_meter_provider_accepts_http_protobuf() {
        let cfg = test_config("http/protobuf", "http://127.0.0.1:4318/v1/metrics");
        let provider = init_meter_provider(&cfg);
        assert!(provider.is_ok(), "{provider:?}");
    }
}
