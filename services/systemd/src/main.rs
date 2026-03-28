use anyhow::{anyhow, Context, Result};
use host_collectors::{init_meter_provider, OtlpSettings, PrefixFilter};
use opentelemetry::metrics::Gauge;
use opentelemetry::KeyValue;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::env;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};
use tracing_subscriber::EnvFilter;

#[derive(Clone, Debug)]
struct Config {
    service_name: String,
    instance_id: String,
    poll_interval: Duration,
    otlp_endpoint: String,
    otlp_protocol: String,
    metrics_include: Vec<String>,
    metrics_exclude: Vec<String>,
    once: bool,
}

#[derive(Clone)]
struct Instruments {
    up: Gauge<u64>,
}

impl Instruments {
    fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self {
            up: meter.u64_gauge("system.systemd.up").build(),
        }
    }
}

fn parse_bool_env(name: &str) -> Option<bool> {
    env::var(name).ok().and_then(|v| {
        let n = v.trim().to_ascii_lowercase();
        if matches!(n.as_str(), "1" | "true" | "yes" | "on") {
            Some(true)
        } else if matches!(n.as_str(), "0" | "false" | "no" | "off") {
            Some(false)
        } else {
            None
        }
    })
}

fn collect_systemd_up() -> u64 {
    if parse_bool_env("OJO_SYSTEMD_SIMULATE_UP").unwrap_or(true) {
        1
    } else {
        0
    }
}

fn record_u64(instrument: &Gauge<u64>, filter: &PrefixFilter, name: &str, value: u64) {
    if filter.allows(name) {
        instrument.record(value, &[] as &[KeyValue]);
    }
}

fn resolve_default_config_path(local_name: &str, repo_relative: &str) -> String {
    if Path::new(local_name).exists() {
        local_name.to_string()
    } else {
        repo_relative.to_string()
    }
}

fn load_yaml_config_file(config_path: &str) -> Result<FileConfig> {
    let path = Path::new(config_path);
    if !path.exists() {
        return Err(anyhow!("config file '{}' was not found", config_path));
    }
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config file '{}'", config_path))?;
    if contents.trim().is_empty() {
        return Err(anyhow!("config file '{}' is empty", config_path));
    }
    serde_yaml::from_str::<FileConfig>(&contents)
        .with_context(|| format!("failed to parse YAML in '{}'", config_path))
}

impl Config {
    fn load() -> Result<Self> {
        let args = env::args().collect::<Vec<_>>();
        Self::load_from_args(&args)
    }

    fn load_from_args(args: &[String]) -> Result<Self> {
        let once =
            args.iter().any(|a| a == "--once") || parse_bool_env("OJO_RUN_ONCE").unwrap_or(false);
        let config_path = args
            .windows(2)
            .find(|p| p[0] == "--config")
            .map(|p| p[1].clone())
            .or_else(|| env::var("OJO_SYSTEMD_CONFIG").ok())
            .unwrap_or_else(|| {
                resolve_default_config_path("systemd.yaml", "services/systemd/systemd.yaml")
            });

        let file_cfg = load_yaml_config_file(&config_path)?;
        let service = file_cfg.service.unwrap_or_default();
        let collection = file_cfg.collection.unwrap_or_default();
        let export = file_cfg.export.unwrap_or_default();
        let otlp = export.otlp.unwrap_or_default();
        let metrics = file_cfg.metrics.unwrap_or_default();

        let otlp_endpoint = otlp
            .endpoint
            .or_else(|| env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok())
            .unwrap_or_else(|| "http://127.0.0.1:4318/v1/metrics".to_string());
        let otlp_protocol = otlp
            .protocol
            .or_else(|| env::var("OTEL_EXPORTER_OTLP_PROTOCOL").ok())
            .unwrap_or_else(|| "http/protobuf".to_string());

        Ok(Self {
            service_name: service.name.unwrap_or_else(|| "ojo-systemd".to_string()),
            instance_id: service
                .instance_id
                .unwrap_or_else(host_collectors::hostname),
            poll_interval: Duration::from_secs(collection.poll_interval_secs.unwrap_or(10).max(1)),
            otlp_endpoint,
            otlp_protocol,
            metrics_include: metrics
                .include
                .unwrap_or_else(|| vec!["system.systemd.".to_string()]),
            metrics_exclude: metrics.exclude.unwrap_or_default(),
            once,
        })
    }
}

fn run() -> Result<()> {
    let cfg = Config::load()?;
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init()
        .ok();

    let provider = init_meter_provider(&OtlpSettings {
        service_name: cfg.service_name.clone(),
        instance_id: cfg.instance_id.clone(),
        otlp_endpoint: cfg.otlp_endpoint.clone(),
        otlp_protocol: cfg.otlp_protocol.clone(),
        otlp_headers: BTreeMap::new(),
        otlp_compression: None,
        otlp_timeout: None,
        export_interval: None,
        export_timeout: None,
    })?;
    let meter = opentelemetry::global::meter("ojo-systemd");
    let instruments = Instruments::new(&meter);
    let filter = PrefixFilter::new(cfg.metrics_include.clone(), cfg.metrics_exclude.clone());

    #[cfg(test)]
    let mut iterations = 0u64;
    loop {
        let started_at = Instant::now();
        record_u64(
            &instruments.up,
            &filter,
            "system.systemd.up",
            collect_systemd_up(),
        );
        let _ = provider.force_flush();

        #[cfg(test)]
        {
            iterations += 1;
            let max = env::var("OJO_TEST_MAX_ITERATIONS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(1);
            if iterations >= max {
                break;
            }
        }

        if cfg.once {
            break;
        }

        let deadline = started_at + cfg.poll_interval;
        while Instant::now() < deadline {
            thread::sleep(Duration::from_millis(100));
        }
    }

    let _ = provider.shutdown();
    Ok(())
}

#[cfg(not(test))]
fn main() -> Result<()> {
    run()
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
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ExportSection {
    otlp: Option<OtlpSection>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct OtlpSection {
    endpoint: Option<String>,
    protocol: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct MetricSection {
    include: Option<Vec<String>>,
    exclude: Option<Vec<String>>,
}

#[cfg(test)]
mod tests;
