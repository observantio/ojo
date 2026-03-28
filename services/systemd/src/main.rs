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

mod platform;

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
    source_available: Gauge<u64>,
    up: Gauge<u64>,
    units_total: Gauge<u64>,
    units_active: Gauge<u64>,
    units_inactive: Gauge<u64>,
    units_failed: Gauge<u64>,
    units_activating: Gauge<u64>,
    units_deactivating: Gauge<u64>,
    units_reloading: Gauge<u64>,
    units_not_found: Gauge<u64>,
    units_maintenance: Gauge<u64>,
    jobs_queued: Gauge<u64>,
    jobs_running: Gauge<u64>,
    failed_units_reported: Gauge<u64>,
    units_failed_ratio: Gauge<f64>,
    units_active_ratio: Gauge<f64>,
}

impl Instruments {
    fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self {
            source_available: meter.u64_gauge("system.systemd.source.available").build(),
            up: meter.u64_gauge("system.systemd.up").build(),
            units_total: meter.u64_gauge("system.systemd.units.total").build(),
            units_active: meter.u64_gauge("system.systemd.units.active").build(),
            units_inactive: meter.u64_gauge("system.systemd.units.inactive").build(),
            units_failed: meter.u64_gauge("system.systemd.units.failed").build(),
            units_activating: meter.u64_gauge("system.systemd.units.activating").build(),
            units_deactivating: meter.u64_gauge("system.systemd.units.deactivating").build(),
            units_reloading: meter.u64_gauge("system.systemd.units.reloading").build(),
            units_not_found: meter.u64_gauge("system.systemd.units.not_found").build(),
            units_maintenance: meter.u64_gauge("system.systemd.units.maintenance").build(),
            jobs_queued: meter.u64_gauge("system.systemd.jobs.queued").build(),
            jobs_running: meter.u64_gauge("system.systemd.jobs.running").build(),
            failed_units_reported: meter
                .u64_gauge("system.systemd.failed_units.reported")
                .build(),
            units_failed_ratio: meter
                .f64_gauge("system.systemd.units.failed.ratio")
                .with_unit("1")
                .build(),
            units_active_ratio: meter
                .f64_gauge("system.systemd.units.active.ratio")
                .with_unit("1")
                .build(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SystemdSnapshot {
    pub(crate) available: bool,
    pub(crate) units_total: u64,
    pub(crate) units_active: u64,
    pub(crate) units_inactive: u64,
    pub(crate) units_failed: u64,
    pub(crate) units_activating: u64,
    pub(crate) units_deactivating: u64,
    pub(crate) units_reloading: u64,
    pub(crate) units_not_found: u64,
    pub(crate) units_maintenance: u64,
    pub(crate) jobs_queued: u64,
    pub(crate) jobs_running: u64,
    pub(crate) failed_units_reported: u64,
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

fn parse_u64_env(name: &str) -> Option<u64> {
    env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
}

fn simulated_snapshot_from_env() -> Option<SystemdSnapshot> {
    let simulate_up = parse_bool_env("OJO_SYSTEMD_SIMULATE_UP")?;
    let units_total = parse_u64_env("OJO_SYSTEMD_SIMULATE_UNITS_TOTAL").unwrap_or(1);
    let units_active = parse_u64_env("OJO_SYSTEMD_SIMULATE_UNITS_ACTIVE")
        .unwrap_or(if simulate_up { units_total } else { 0 });
    let units_inactive = parse_u64_env("OJO_SYSTEMD_SIMULATE_UNITS_INACTIVE")
        .unwrap_or(units_total.saturating_sub(units_active));
    let units_failed = parse_u64_env("OJO_SYSTEMD_SIMULATE_UNITS_FAILED").unwrap_or(0);

    Some(SystemdSnapshot {
        available: simulate_up,
        units_total,
        units_active,
        units_inactive,
        units_failed,
        units_activating: parse_u64_env("OJO_SYSTEMD_SIMULATE_UNITS_ACTIVATING").unwrap_or(0),
        units_deactivating: parse_u64_env("OJO_SYSTEMD_SIMULATE_UNITS_DEACTIVATING").unwrap_or(0),
        units_reloading: parse_u64_env("OJO_SYSTEMD_SIMULATE_UNITS_RELOADING").unwrap_or(0),
        units_not_found: parse_u64_env("OJO_SYSTEMD_SIMULATE_UNITS_NOT_FOUND").unwrap_or(0),
        units_maintenance: parse_u64_env("OJO_SYSTEMD_SIMULATE_UNITS_MAINTENANCE").unwrap_or(0),
        jobs_queued: parse_u64_env("OJO_SYSTEMD_SIMULATE_JOBS_QUEUED").unwrap_or(0),
        jobs_running: parse_u64_env("OJO_SYSTEMD_SIMULATE_JOBS_RUNNING").unwrap_or(0),
        failed_units_reported: parse_u64_env("OJO_SYSTEMD_SIMULATE_FAILED_UNITS_REPORTED")
            .unwrap_or(units_failed),
    })
}

fn collect_snapshot() -> SystemdSnapshot {
    simulated_snapshot_from_env().unwrap_or_else(platform::collect_snapshot)
}

fn record_u64(instrument: &Gauge<u64>, filter: &PrefixFilter, name: &str, value: u64) {
    if filter.allows(name) {
        instrument.record(value, &[] as &[KeyValue]);
    }
}

fn record_f64(instrument: &Gauge<f64>, filter: &PrefixFilter, name: &str, value: f64) {
    if filter.allows(name) {
        instrument.record(value, &[] as &[KeyValue]);
    }
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn snapshot_up(snapshot: &SystemdSnapshot) -> u64 {
    if snapshot.available {
        1
    } else {
        0
    }
}

fn record_snapshot(instruments: &Instruments, filter: &PrefixFilter, snapshot: &SystemdSnapshot) {
    record_u64(
        &instruments.source_available,
        filter,
        "system.systemd.source.available",
        snapshot_up(snapshot),
    );
    record_u64(
        &instruments.up,
        filter,
        "system.systemd.up",
        snapshot_up(snapshot),
    );
    record_u64(
        &instruments.units_total,
        filter,
        "system.systemd.units.total",
        snapshot.units_total,
    );
    record_u64(
        &instruments.units_active,
        filter,
        "system.systemd.units.active",
        snapshot.units_active,
    );
    record_u64(
        &instruments.units_inactive,
        filter,
        "system.systemd.units.inactive",
        snapshot.units_inactive,
    );
    record_u64(
        &instruments.units_failed,
        filter,
        "system.systemd.units.failed",
        snapshot.units_failed,
    );
    record_u64(
        &instruments.units_activating,
        filter,
        "system.systemd.units.activating",
        snapshot.units_activating,
    );
    record_u64(
        &instruments.units_deactivating,
        filter,
        "system.systemd.units.deactivating",
        snapshot.units_deactivating,
    );
    record_u64(
        &instruments.units_reloading,
        filter,
        "system.systemd.units.reloading",
        snapshot.units_reloading,
    );
    record_u64(
        &instruments.units_not_found,
        filter,
        "system.systemd.units.not_found",
        snapshot.units_not_found,
    );
    record_u64(
        &instruments.units_maintenance,
        filter,
        "system.systemd.units.maintenance",
        snapshot.units_maintenance,
    );
    record_u64(
        &instruments.jobs_queued,
        filter,
        "system.systemd.jobs.queued",
        snapshot.jobs_queued,
    );
    record_u64(
        &instruments.jobs_running,
        filter,
        "system.systemd.jobs.running",
        snapshot.jobs_running,
    );
    record_u64(
        &instruments.failed_units_reported,
        filter,
        "system.systemd.failed_units.reported",
        snapshot.failed_units_reported,
    );
    record_f64(
        &instruments.units_failed_ratio,
        filter,
        "system.systemd.units.failed.ratio",
        ratio(snapshot.units_failed, snapshot.units_total),
    );
    record_f64(
        &instruments.units_active_ratio,
        filter,
        "system.systemd.units.active.ratio",
        ratio(snapshot.units_active, snapshot.units_total),
    );
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
        let snapshot = collect_snapshot();
        record_snapshot(&instruments, &filter, &snapshot);
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
