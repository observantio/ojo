use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::env;
use std::path::Path;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Config {
    pub service_name: String,
    pub instance_id: String,
    pub poll_interval: Duration,
    pub include_process_metrics: bool,
    pub otlp_endpoint: String,
    pub otlp_protocol: String,
    pub otlp_headers: BTreeMap<String, String>,
    pub otlp_compression: Option<String>,
    pub otlp_timeout: Option<Duration>,
    pub export_interval: Option<Duration>,
    pub export_timeout: Option<Duration>,
    pub metrics_include: Vec<String>,
    pub metrics_exclude: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileConfig {
    service: Option<ServiceSection>,
    collection: Option<CollectionSection>,
    export: Option<ExportSection>,
    metrics: Option<MetricSection>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ServiceSection {
    name: Option<String>,
    instance_id: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct CollectionSection {
    poll_interval_secs: Option<u64>,
    include_process_metrics: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ExportSection {
    otlp: Option<OtlpSection>,
    batch: Option<BatchSection>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct OtlpSection {
    endpoint: Option<String>,
    protocol: Option<String>,
    token: Option<String>,
    token_header: Option<String>,
    headers: Option<BTreeMap<String, String>>,
    compression: Option<String>,
    timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct BatchSection {
    interval_secs: Option<u64>,
    timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct MetricSection {
    include: Option<Vec<String>>,
    exclude: Option<Vec<String>>,
}

impl Config {
    pub fn load() -> Result<Self> {
        let args = env::args().collect::<Vec<_>>();
        let config_path = args
            .windows(2)
            .find(|pair| pair[0] == "--config")
            .map(|pair| pair[1].clone())
            .or_else(|| env::var("PROC_OTEL_CONFIG").ok())
            .unwrap_or_else(|| "ojo.yaml".to_string());

        let file_cfg = load_yaml_config_file(&config_path)?;
        validate_required_yaml_fields(&file_cfg, &config_path)?;

        let service = file_cfg.service.unwrap_or_default();
        let collection = file_cfg.collection.unwrap_or_default();
        let export = file_cfg.export.unwrap_or_default();
        let otlp = export.otlp.unwrap_or_default();
        let batch = export.batch.unwrap_or_default();
        let metrics = file_cfg.metrics.unwrap_or_default();

        let mut otlp_headers = otlp.headers.unwrap_or_default();
        if let Some(token) = otlp.token {
            let header = otlp
                .token_header
                .unwrap_or_else(|| "authorization".to_string());
            otlp_headers.insert(header, token);
        }

        let otlp_endpoint = otlp
            .endpoint
            .clone()
            .or_else(|| env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok())
            .unwrap_or_else(|| "http://127.0.0.1:4317".to_string());
        let otlp_protocol = otlp
            .protocol
            .or_else(|| env::var("OTEL_EXPORTER_OTLP_PROTOCOL").ok())
            .unwrap_or_else(|| default_protocol_for_endpoint(Some(&otlp_endpoint)));

        Ok(Self {
            service_name: service
                .name
                .or_else(|| env::var("OTEL_SERVICE_NAME").ok())
                .unwrap_or_else(|| "ojo".to_string()),
            instance_id: service
                .instance_id
                .or_else(|| env::var("OTEL_SERVICE_INSTANCE_ID").ok())
                .unwrap_or_else(hostname_fallback),
            poll_interval: Duration::from_secs(
                collection
                    .poll_interval_secs
                    .or_else(|| {
                        env::var("PROC_POLL_INTERVAL_SECS")
                            .ok()
                            .and_then(|v| v.parse().ok())
                    })
                    .unwrap_or(5),
            ),
            include_process_metrics: collection
                .include_process_metrics
                .or_else(|| {
                    env::var("PROC_INCLUDE_PROCESS_METRICS")
                        .ok()
                        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                })
                .unwrap_or(false),
            otlp_endpoint,
            otlp_protocol,
            otlp_headers,
            otlp_compression: otlp
                .compression
                .or_else(|| env::var("OTEL_EXPORTER_OTLP_COMPRESSION").ok()),
            otlp_timeout: otlp.timeout_secs.map(Duration::from_secs).or_else(|| {
                env::var("OTEL_EXPORTER_OTLP_TIMEOUT")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .map(Duration::from_secs)
            }),
            export_interval: batch.interval_secs.map(Duration::from_secs).or_else(|| {
                env::var("OTEL_METRIC_EXPORT_INTERVAL")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .map(Duration::from_millis)
            }),
            export_timeout: batch.timeout_secs.map(Duration::from_secs).or_else(|| {
                env::var("OTEL_METRIC_EXPORT_TIMEOUT")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .map(Duration::from_millis)
            }),
            metrics_include: metrics.include.unwrap_or_default(),
            metrics_exclude: metrics.exclude.unwrap_or_default(),
        })
    }

    pub fn apply_otel_env(&self) {
        env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", &self.otlp_endpoint);
        env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", &self.otlp_protocol);

        if !self.otlp_headers.is_empty() {
            let headers = self
                .otlp_headers
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join(",");
            env::set_var("OTEL_EXPORTER_OTLP_HEADERS", headers);
        }

        if let Some(compression) = &self.otlp_compression {
            env::set_var("OTEL_EXPORTER_OTLP_COMPRESSION", compression);
        }
        if let Some(timeout) = self.otlp_timeout {
            env::set_var("OTEL_EXPORTER_OTLP_TIMEOUT", timeout.as_secs().to_string());
        }
        if let Some(interval) = self.export_interval {
            env::set_var(
                "OTEL_METRIC_EXPORT_INTERVAL",
                interval.as_millis().to_string(),
            );
        }
        if let Some(timeout) = self.export_timeout {
            env::set_var(
                "OTEL_METRIC_EXPORT_TIMEOUT",
                timeout.as_millis().to_string(),
            );
        }
    }
}

fn load_yaml_config_file(config_path: &str) -> Result<FileConfig> {
    let path = Path::new(config_path);

    if !path.exists() {
        return Err(anyhow!(
            "config file '{}' was not found. Pass --config <path> or set PROC_OTEL_CONFIG to a valid YAML file.",
            config_path
        ));
    }

    if !path.is_file() {
        return Err(anyhow!(
            "config path '{}' is not a file. Provide a YAML file path.",
            config_path
        ));
    }

    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config file '{}'", config_path))?;

    if contents.trim().is_empty() {
        return Err(anyhow!(
            "config file '{}' is empty. Add required sections like service, collection, and export.otlp.",
            config_path
        ));
    }

    serde_yaml::from_str::<FileConfig>(&contents).with_context(|| {
        format!(
            "failed to parse YAML in '{}'. Check indentation and key/value structure.",
            config_path
        )
    })
}

fn validate_required_yaml_fields(file_cfg: &FileConfig, config_path: &str) -> Result<()> {
    let mut missing = Vec::new();

    let service = file_cfg.service.as_ref();
    let collection = file_cfg.collection.as_ref();
    let export = file_cfg.export.as_ref();
    let otlp = export.and_then(|section| section.otlp.as_ref());

    if service
        .and_then(|section| section.name.as_ref())
        .map(|value| value.trim().is_empty())
        .unwrap_or(true)
    {
        missing.push("service.name");
    }

    if service
        .and_then(|section| section.instance_id.as_ref())
        .map(|value| value.trim().is_empty())
        .unwrap_or(true)
    {
        missing.push("service.instance_id");
    }

    if collection
        .and_then(|section| section.poll_interval_secs)
        .is_none()
    {
        missing.push("collection.poll_interval_secs");
    }

    if otlp
        .and_then(|section| section.endpoint.as_ref())
        .map(|value| value.trim().is_empty())
        .unwrap_or(true)
    {
        missing.push("export.otlp.endpoint");
    }

    if otlp
        .and_then(|section| section.protocol.as_ref())
        .map(|value| value.trim().is_empty())
        .unwrap_or(true)
    {
        missing.push("export.otlp.protocol");
    }

    if missing.is_empty() {
        return Ok(());
    }

    Err(anyhow!(
        "config file '{}' is missing required attributes:\n- {}\n\nExpected minimal structure:\nservice:\n  name: linux\n  instance_id: linux-0001\ncollection:\n  poll_interval_secs: 5\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf",
        config_path,
        missing.join("\n- ")
    ))
}

fn hostname_fallback() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown-host".to_string())
}

fn default_protocol_for_endpoint(endpoint: Option<&str>) -> String {
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
