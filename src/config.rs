use anyhow::{Context, Result};
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
    pub dump_snapshot: bool,
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
        let dump_snapshot = args.contains(&"--dump-snapshot".to_string())
            || env::var("PROC_DUMP_SNAPSHOT")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);

        let config_path = args
            .windows(2)
            .find(|pair| pair[0] == "--config")
            .map(|pair| pair[1].clone())
            .or_else(|| env::var("PROC_OTEL_CONFIG").ok())
            .unwrap_or_else(|| "ojo.yaml".to_string());

        let file_cfg = if Path::new(&config_path).exists() {
            let contents = std::fs::read_to_string(&config_path)
                .with_context(|| format!("failed to read config file {}", config_path))?;
            serde_yaml::from_str::<FileConfig>(&contents)
                .with_context(|| format!("failed to parse config file {}", config_path))?
        } else {
            FileConfig::default()
        };

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
                .unwrap_or(cfg!(target_os = "windows")),
            dump_snapshot,
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
        set_otel_env_var("OTEL_EXPORTER_OTLP_ENDPOINT", &self.otlp_endpoint);
        set_otel_env_var("OTEL_EXPORTER_OTLP_PROTOCOL", &self.otlp_protocol);

        if !self.otlp_headers.is_empty() {
            let headers = self
                .otlp_headers
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join(",");
            set_otel_env_var("OTEL_EXPORTER_OTLP_HEADERS", headers);
        }

        if let Some(compression) = &self.otlp_compression {
            set_otel_env_var("OTEL_EXPORTER_OTLP_COMPRESSION", compression);
        }
        if let Some(timeout) = self.otlp_timeout {
            set_otel_env_var("OTEL_EXPORTER_OTLP_TIMEOUT", timeout.as_secs().to_string());
        }
        if let Some(interval) = self.export_interval {
            set_otel_env_var(
                "OTEL_METRIC_EXPORT_INTERVAL",
                interval.as_millis().to_string(),
            );
        }
        if let Some(timeout) = self.export_timeout {
            set_otel_env_var(
                "OTEL_METRIC_EXPORT_TIMEOUT",
                timeout.as_millis().to_string(),
            );
        }
    }
}

#[inline]
fn set_otel_env_var<K, V>(key: K, value: V)
where
    K: AsRef<std::ffi::OsStr>,
    V: AsRef<std::ffi::OsStr>,
{
    unsafe { env::set_var(key, value) }
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
