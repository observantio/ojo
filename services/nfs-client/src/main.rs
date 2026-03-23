use anyhow::{anyhow, Context, Result};
use host_collectors::{
    default_protocol_for_endpoint, init_meter_provider, OtlpSettings, PrefixFilter,
};
use opentelemetry::metrics::Gauge;
use opentelemetry::KeyValue;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::env;
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

mod platform;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExportState {
    Pending,
    Connected,
    Reconnecting,
}

#[derive(Clone, Debug)]
struct Config {
    service_name: String,
    instance_id: String,
    poll_interval: Duration,
    otlp_endpoint: String,
    otlp_protocol: String,
    otlp_headers: BTreeMap<String, String>,
    otlp_compression: Option<String>,
    otlp_timeout: Option<Duration>,
    export_interval: Option<Duration>,
    export_timeout: Option<Duration>,
    metrics_include: Vec<String>,
    metrics_exclude: Vec<String>,
    nfs_client: NfsClientConfig,
    once: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct NfsClientConfig {
    pub(crate) executable: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct NfsClientSnapshot {
    pub(crate) available: bool,
    pub(crate) mounts: u64,
    pub(crate) rpc_calls_total: u64,
    pub(crate) rpc_retransmissions_total: u64,
    pub(crate) rpc_auth_refreshes_total: u64,
}

#[derive(Clone, Debug, Default)]
struct PrevState {
    last: Option<(NfsClientSnapshot, Instant)>,
}

#[derive(Clone, Debug, Default)]
struct NfsRates {
    rpc_calls_rate: f64,
    rpc_retransmissions_rate: f64,
}

impl PrevState {
    fn derive(&mut self, current: &NfsClientSnapshot) -> NfsRates {
        let now = Instant::now();
        let Some((previous, previous_at)) = &self.last else {
            self.last = Some((current.clone(), now));
            return NfsRates::default();
        };
        let elapsed = now.duration_since(*previous_at).as_secs_f64();
        if elapsed <= 0.0 {
            self.last = Some((current.clone(), now));
            return NfsRates::default();
        }
        let rates = NfsRates {
            rpc_calls_rate: saturating_rate(
                previous.rpc_calls_total,
                current.rpc_calls_total,
                elapsed,
            ),
            rpc_retransmissions_rate: saturating_rate(
                previous.rpc_retransmissions_total,
                current.rpc_retransmissions_total,
                elapsed,
            ),
        };
        self.last = Some((current.clone(), now));
        rates
    }
}

fn saturating_rate(previous: u64, current: u64, elapsed_secs: f64) -> f64 {
    if current < previous {
        return 0.0;
    }
    (current - previous) as f64 / elapsed_secs
}

struct Instruments {
    source_available: Gauge<u64>,
    mounts: Gauge<u64>,
    rpc_calls_total: Gauge<u64>,
    rpc_retransmissions_total: Gauge<u64>,
    rpc_auth_refreshes_total: Gauge<u64>,
    rpc_calls_rate: Gauge<f64>,
    rpc_retransmissions_rate: Gauge<f64>,
}

impl Instruments {
    fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self {
            source_available: meter
                .u64_gauge("system.nfs_client.source.available")
                .build(),
            mounts: meter.u64_gauge("system.nfs_client.mounts").build(),
            rpc_calls_total: meter.u64_gauge("system.nfs_client.rpc.calls.total").build(),
            rpc_retransmissions_total: meter
                .u64_gauge("system.nfs_client.rpc.retransmissions.total")
                .build(),
            rpc_auth_refreshes_total: meter
                .u64_gauge("system.nfs_client.rpc.auth_refreshes.total")
                .build(),
            rpc_calls_rate: meter
                .f64_gauge("system.nfs_client.rpc.calls.rate_per_second")
                .with_unit("{calls}/s")
                .build(),
            rpc_retransmissions_rate: meter
                .f64_gauge("system.nfs_client.rpc.retransmissions.rate_per_second")
                .with_unit("{retransmissions}/s")
                .build(),
        }
    }
}

fn main() -> Result<()> {
    let cfg = Config::load()?;
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let provider = init_meter_provider(&OtlpSettings {
        service_name: cfg.service_name.clone(),
        instance_id: cfg.instance_id.clone(),
        otlp_endpoint: cfg.otlp_endpoint.clone(),
        otlp_protocol: cfg.otlp_protocol.clone(),
        otlp_headers: cfg.otlp_headers.clone(),
        otlp_compression: cfg.otlp_compression.clone(),
        otlp_timeout: cfg.otlp_timeout,
        export_interval: cfg.export_interval,
        export_timeout: cfg.export_timeout,
    })?;
    let meter = opentelemetry::global::meter("ojo-nfs-client");
    let instruments = Instruments::new(&meter);
    let filter = PrefixFilter::new(cfg.metrics_include.clone(), cfg.metrics_exclude.clone());
    let mut prev = PrevState::default();

    let running = Arc::new(AtomicBool::new(true));
    let signal = Arc::clone(&running);
    ctrlc::set_handler(move || {
        signal.store(false, Ordering::SeqCst);
    })?;

    let mut export_state = ExportState::Pending;
    while running.load(Ordering::SeqCst) {
        let started_at = Instant::now();
        let snapshot = platform::collect_snapshot(&cfg.nfs_client);
        let rates = if snapshot.available {
            prev.derive(&snapshot)
        } else {
            prev.last = None;
            NfsRates::default()
        };
        record_snapshot(&instruments, &filter, &snapshot, &rates);

        match provider.force_flush() {
            Ok(()) => {
                debug!(
                    elapsed_ms = started_at.elapsed().as_millis(),
                    "force_flush ok"
                );
                match export_state {
                    ExportState::Pending => info!("Connected Successfully"),
                    ExportState::Reconnecting => info!("Reconnected Successfully"),
                    ExportState::Connected => {}
                }
                export_state = ExportState::Connected;
            }
            Err(err) => {
                debug!(
                    elapsed_ms = started_at.elapsed().as_millis(),
                    "force_flush err"
                );
                match export_state {
                    ExportState::Connected => {
                        warn!(error = %err, "Exporter flush failed; reconnecting")
                    }
                    ExportState::Pending | ExportState::Reconnecting => {
                        warn!(error = %err, "Exporter still unavailable")
                    }
                }
                export_state = ExportState::Reconnecting;
            }
        }

        if cfg.once {
            break;
        }
        let deadline = started_at + cfg.poll_interval;
        while Instant::now() < deadline && running.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(500));
        }
    }

    let _ = provider.shutdown();
    Ok(())
}

fn record_snapshot(
    instruments: &Instruments,
    filter: &PrefixFilter,
    snap: &NfsClientSnapshot,
    rates: &NfsRates,
) {
    record_u64(
        &instruments.source_available,
        filter,
        "system.nfs_client.source.available",
        if snap.available { 1 } else { 0 },
    );
    if !snap.available {
        return;
    }
    record_u64(
        &instruments.mounts,
        filter,
        "system.nfs_client.mounts",
        snap.mounts,
    );
    record_u64(
        &instruments.rpc_calls_total,
        filter,
        "system.nfs_client.rpc.calls.total",
        snap.rpc_calls_total,
    );
    record_u64(
        &instruments.rpc_retransmissions_total,
        filter,
        "system.nfs_client.rpc.retransmissions.total",
        snap.rpc_retransmissions_total,
    );
    record_u64(
        &instruments.rpc_auth_refreshes_total,
        filter,
        "system.nfs_client.rpc.auth_refreshes.total",
        snap.rpc_auth_refreshes_total,
    );
    record_f64(
        &instruments.rpc_calls_rate,
        filter,
        "system.nfs_client.rpc.calls.rate_per_second",
        rates.rpc_calls_rate,
    );
    record_f64(
        &instruments.rpc_retransmissions_rate,
        filter,
        "system.nfs_client.rpc.retransmissions.rate_per_second",
        rates.rpc_retransmissions_rate,
    );
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

#[derive(Clone, Debug, Default, Deserialize)]
struct FileConfig {
    service: Option<ServiceSection>,
    collection: Option<CollectionSection>,
    export: Option<ExportSection>,
    metrics: Option<MetricSection>,
    nfs_client: Option<NfsClientSection>,
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
struct NfsClientSection {
    executable: Option<String>,
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
    fn load() -> Result<Self> {
        let args = env::args().collect::<Vec<_>>();
        let once = args.iter().any(|arg| arg == "--once");
        let config_path = args
            .windows(2)
            .find(|pair| pair[0] == "--config")
            .map(|pair| pair[1].clone())
            .or_else(|| env::var("OJO_NFS_CLIENT_CONFIG").ok())
            .unwrap_or_else(|| {
                resolve_default_config_path(
                    "nfs-client.yaml",
                    "services/nfs-client/nfs-client.yaml",
                )
            });

        let file_cfg = load_yaml_config_file(&config_path)?;
        let service = file_cfg.service.unwrap_or_default();
        let collection = file_cfg.collection.unwrap_or_default();
        let export = file_cfg.export.unwrap_or_default();
        let otlp = export.otlp.unwrap_or_default();
        let batch = export.batch.unwrap_or_default();
        let metrics = file_cfg.metrics.unwrap_or_default();
        let nfs_client = file_cfg.nfs_client.unwrap_or_default();

        let otlp_endpoint = otlp
            .endpoint
            .or_else(|| env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok())
            .unwrap_or_else(|| "http://127.0.0.1:4318/v1/metrics".to_string());
        let otlp_protocol = otlp
            .protocol
            .or_else(|| env::var("OTEL_EXPORTER_OTLP_PROTOCOL").ok())
            .unwrap_or_else(|| default_protocol_for_endpoint(Some(&otlp_endpoint)));

        Ok(Self {
            service_name: service.name.unwrap_or_else(|| "ojo-nfs-client".to_string()),
            instance_id: service
                .instance_id
                .unwrap_or_else(host_collectors::hostname),
            poll_interval: Duration::from_secs(collection.poll_interval_secs.unwrap_or(10).max(1)),
            otlp_endpoint,
            otlp_protocol,
            otlp_headers: otlp.headers.unwrap_or_default(),
            otlp_compression: otlp.compression,
            otlp_timeout: otlp.timeout_secs.map(Duration::from_secs),
            export_interval: batch.interval_secs.map(Duration::from_secs),
            export_timeout: batch.timeout_secs.map(Duration::from_secs),
            metrics_include: metrics
                .include
                .unwrap_or_else(|| vec!["system.nfs_client.".to_string()]),
            metrics_exclude: metrics.exclude.unwrap_or_default(),
            nfs_client: NfsClientConfig {
                executable: nfs_client.executable,
            },
            once,
        })
    }
}

fn resolve_default_config_path(local_name: &str, repo_relative: &str) -> String {
    if Path::new(local_name).exists() {
        return local_name.to_string();
    }
    repo_relative.to_string()
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
