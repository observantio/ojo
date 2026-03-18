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
    metrics: Option<MetricConfig>,
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
    groups: Option<Vec<String>>,
    include: Option<Vec<String>>,
    exclude: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
enum MetricConfig {
    Section(MetricSection),
    Groups(Vec<String>),
    Group(String),
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
        let (metric_groups, mut metrics_include, metrics_exclude) =
            parse_metric_config(file_cfg.metrics)?;

        metrics_include.extend(expand_metric_groups(&metric_groups)?);
        dedup_in_place(&mut metrics_include);

        let mut otlp_headers = env_otlp_headers();
        if let Some(headers) = otlp.headers {
            for (key, value) in headers {
                otlp_headers.insert(key, value);
            }
        }
        if let Some(token) = otlp.token {
            let header = otlp
                .token_header
                .unwrap_or_else(|| "authorization".to_string());
            otlp_headers.insert(header, token);
        }

        let otlp_endpoint = otlp
            .endpoint
            .clone()
            .or_else(|| env_otlp_var("ENDPOINT"))
            .unwrap_or_else(|| "http://127.0.0.1:4317".to_string());
        let otlp_protocol = otlp
            .protocol
            .or_else(|| env_otlp_var("PROTOCOL"))
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
                .or_else(|| env_otlp_var("COMPRESSION")),
            otlp_timeout: otlp.timeout_secs.map(Duration::from_secs).or_else(|| {
                env_otlp_var("TIMEOUT")
                    .and_then(|v| v.parse::<u64>().ok())
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
            metrics_include,
            metrics_exclude,
        })
    }

    pub fn apply_otel_env(&self) {
        set_otel_env_var("OTEL_EXPORTER_OTLP_ENDPOINT", &self.otlp_endpoint);
        set_otel_env_var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", &self.otlp_endpoint);
        set_otel_env_var("OTEL_EXPORTER_OTLP_PROTOCOL", &self.otlp_protocol);
        set_otel_env_var("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", &self.otlp_protocol);

        if !self.otlp_headers.is_empty() {
            let headers = self
                .otlp_headers
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join(",");
            set_otel_env_var("OTEL_EXPORTER_OTLP_HEADERS", &headers);
            set_otel_env_var("OTEL_EXPORTER_OTLP_METRICS_HEADERS", &headers);
        }

        if let Some(compression) = &self.otlp_compression {
            set_otel_env_var("OTEL_EXPORTER_OTLP_COMPRESSION", compression);
            set_otel_env_var("OTEL_EXPORTER_OTLP_METRICS_COMPRESSION", compression);
        }
        if let Some(timeout) = self.otlp_timeout {
            set_otel_env_var("OTEL_EXPORTER_OTLP_TIMEOUT", timeout.as_secs().to_string());
            set_otel_env_var(
                "OTEL_EXPORTER_OTLP_METRICS_TIMEOUT",
                timeout.as_secs().to_string(),
            );
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
        Some(value)
            if has_non_root_path(value)
                || endpoint_port(value) == Some(4318)
                || endpoint_explicit_protocol(value) == Some("http/protobuf") =>
        {
            "http/protobuf".to_string()
        }
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

fn endpoint_port(endpoint: &str) -> Option<u16> {
    let (_, rest) = endpoint.split_once("://")?;
    let authority = rest.split('/').next()?;

    if authority.starts_with('[') {
        let end = authority.find(']')?;
        let tail = authority.get(end + 1..)?;
        return tail.strip_prefix(':')?.parse::<u16>().ok();
    }

    let (_, port) = authority.rsplit_once(':')?;
    port.parse::<u16>().ok()
}

fn endpoint_explicit_protocol(endpoint: &str) -> Option<&'static str> {
    let lower = endpoint.to_ascii_lowercase();
    if lower.contains("/v1/metrics") {
        return Some("http/protobuf");
    }
    None
}

fn env_otlp_var(suffix: &str) -> Option<String> {
    let metrics_key = format!("OTEL_EXPORTER_OTLP_METRICS_{suffix}");
    if let Ok(value) = env::var(metrics_key) {
        if !value.trim().is_empty() {
            return Some(value);
        }
    }

    let generic_key = format!("OTEL_EXPORTER_OTLP_{suffix}");
    env::var(generic_key)
        .ok()
        .filter(|v| !v.trim().is_empty())
}

fn env_otlp_headers() -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();

    if let Ok(raw) = env::var("OTEL_EXPORTER_OTLP_HEADERS") {
        parse_otlp_headers_into(&raw, &mut out);
    }
    if let Ok(raw) = env::var("OTEL_EXPORTER_OTLP_METRICS_HEADERS") {
        parse_otlp_headers_into(&raw, &mut out);
    }

    out
}

fn parse_otlp_headers_into(raw: &str, out: &mut BTreeMap<String, String>) {
    for part in raw.split(',') {
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };

        let key = key.trim();
        if key.is_empty() {
            continue;
        }

        out.insert(key.to_string(), value.trim().to_string());
    }
}

fn parse_metric_config(
    metrics: Option<MetricConfig>,
) -> Result<(Vec<String>, Vec<String>, Vec<String>)> {
    let (groups, include, exclude) = match metrics {
        None => (Vec::new(), Vec::new(), Vec::new()),
        Some(MetricConfig::Group(group)) => (vec![group], Vec::new(), Vec::new()),
        Some(MetricConfig::Groups(groups)) => (groups, Vec::new(), Vec::new()),
        Some(MetricConfig::Section(section)) => (
            section.groups.unwrap_or_default(),
            section.include.unwrap_or_default(),
            section.exclude.unwrap_or_default(),
        ),
    };

    Ok((groups, include, exclude))
}

fn expand_metric_groups(groups: &[String]) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for raw in groups {
        let group = raw.trim().to_ascii_lowercase();
        let patterns = match group.as_str() {
            "cpu" => vec!["system.cpu.", "process.cpu."],
            "memory" => vec![
                "system.memory.",
                "system.swap.",
                "windows.memory.",
                "process.memory.",
            ],
            "disk" => vec!["system.disk.", "process.disk.", "process.io."],
            "network" => vec![
                "system.network.",
                "system.socket.",
                "system.linux.net.",
                "system.windows.net.",
            ],
            "process" => vec!["process."],
            "filesystem" => vec!["system.filesystem."],
            "linux" => vec!["system.linux."],
            "windows" => vec!["system.windows.", "windows."],
            "host" => vec!["system."],
            other => {
                anyhow::bail!(
                    "unknown metrics group '{}'. supported groups: cpu, memory, disk, network, process, filesystem, linux, windows, host",
                    other
                )
            }
        };

        out.extend(patterns.into_iter().map(str::to_string));
    }
    Ok(out)
}

fn dedup_in_place(values: &mut Vec<String>) {
    let mut seen = std::collections::BTreeSet::new();
    values.retain(|value| seen.insert(value.clone()));
}
