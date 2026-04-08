use anyhow::{anyhow, Context, Result};
use host_collectors::{
    default_protocol_for_endpoint, init_meter_provider, ArchiveStorageConfig, JsonArchiveWriter,
    OtlpSettings, PrefixFilter,
};
use opentelemetry::metrics::Gauge;
use opentelemetry::KeyValue;
use serde::{Deserialize, Serialize};
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FlushEvent {
    None,
    Connected,
    Reconnected,
    Reconnecting,
    StillUnavailable,
}

fn advance_export_state(current: ExportState, flush_succeeded: bool) -> (ExportState, FlushEvent) {
    if flush_succeeded {
        let event = match current {
            ExportState::Pending => FlushEvent::Connected,
            ExportState::Reconnecting => FlushEvent::Reconnected,
            ExportState::Connected => FlushEvent::None,
        };
        (ExportState::Connected, event)
    } else {
        let event = match current {
            ExportState::Connected => FlushEvent::Reconnecting,
            ExportState::Pending | ExportState::Reconnecting => FlushEvent::StillUnavailable,
        };
        (ExportState::Reconnecting, event)
    }
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
    postgres: PostgresConfig,
    archive: ArchiveStorageConfig,
    once: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct PostgresConfig {
    pub(crate) executable: String,
    pub(crate) uri: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct PostgresSnapshot {
    pub(crate) available: bool,
    pub(crate) up: bool,
    pub(crate) connections: u64,
    pub(crate) xact_commit_total: u64,
    pub(crate) xact_rollback_total: u64,
    pub(crate) deadlocks_total: u64,
    pub(crate) blks_read_total: u64,
    pub(crate) blks_hit_total: u64,
}

#[derive(Clone, Debug, Default)]
struct PostgresRates {
    commits_per_second: f64,
    rollbacks_per_second: f64,
}

#[derive(Clone, Debug, Default)]
struct PrevState {
    last: Option<(PostgresSnapshot, Instant)>,
}

impl PrevState {
    fn derive(&mut self, current: &PostgresSnapshot) -> PostgresRates {
        let now = Instant::now();
        let Some((previous, previous_at)) = &self.last else {
            self.last = Some((current.clone(), now));
            return PostgresRates::default();
        };
        let elapsed = now
            .checked_duration_since(*previous_at)
            .unwrap_or_default()
            .as_secs_f64();
        if elapsed <= 0.0 {
            self.last = Some((current.clone(), now));
            return PostgresRates::default();
        }
        let rates = PostgresRates {
            commits_per_second: saturating_rate(
                previous.xact_commit_total,
                current.xact_commit_total,
                elapsed,
            ),
            rollbacks_per_second: saturating_rate(
                previous.xact_rollback_total,
                current.xact_rollback_total,
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

fn derive_rates_or_reset(prev: &mut PrevState, snapshot: &PostgresSnapshot) -> PostgresRates {
    if snapshot.available {
        prev.derive(snapshot)
    } else {
        prev.last = None;
        PostgresRates::default()
    }
}

struct Instruments {
    source_available: Gauge<u64>,
    up: Gauge<u64>,
    connections: Gauge<u64>,
    xact_commit_total: Gauge<u64>,
    xact_rollback_total: Gauge<u64>,
    deadlocks_total: Gauge<u64>,
    blks_read_total: Gauge<u64>,
    blks_hit_total: Gauge<u64>,
    commits_rate: Gauge<f64>,
    rollbacks_rate: Gauge<f64>,
}

impl Instruments {
    fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self {
            source_available: meter.u64_gauge("system.postgres.source.available").build(),
            up: meter.u64_gauge("system.postgres.up").build(),
            connections: meter.u64_gauge("system.postgres.connections").build(),
            xact_commit_total: meter
                .u64_gauge("system.postgres.transactions.committed.total")
                .build(),
            xact_rollback_total: meter
                .u64_gauge("system.postgres.transactions.rolled_back.total")
                .build(),
            deadlocks_total: meter.u64_gauge("system.postgres.deadlocks.total").build(),
            blks_read_total: meter.u64_gauge("system.postgres.blocks.read.total").build(),
            blks_hit_total: meter.u64_gauge("system.postgres.blocks.hit.total").build(),
            commits_rate: meter
                .f64_gauge("system.postgres.transactions.committed.rate_per_second")
                .with_unit("{transactions}/s")
                .build(),
            rollbacks_rate: meter
                .f64_gauge("system.postgres.transactions.rolled_back.rate_per_second")
                .with_unit("{transactions}/s")
                .build(),
        }
    }
}

fn run() -> Result<()> {
    let dump_snapshot = env::args().any(|arg| arg == "--dump-snapshot");
    let cfg = Config::load()?;
    if dump_snapshot {
        let snapshot = platform::collect_snapshot(&cfg.postgres);
        println!("{}", serde_json::to_string_pretty(&snapshot)?);
        return Ok(());
    }
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
        otlp_headers: cfg.otlp_headers.clone(),
        otlp_compression: cfg.otlp_compression.clone(),
        otlp_timeout: cfg.otlp_timeout,
        export_interval: cfg.export_interval,
        export_timeout: cfg.export_timeout,
    })?;
    let meter = opentelemetry::global::meter("ojo-postgres");
    let instruments = Instruments::new(&meter);
    let filter = PrefixFilter::new(cfg.metrics_include.clone(), cfg.metrics_exclude.clone());
    let mut prev = PrevState::default();
    let mut archive = JsonArchiveWriter::from_config(&cfg.archive);

    let running = Arc::new(AtomicBool::new(true));
    install_signal_handler(&running);

    let mut export_state = ExportState::Pending;
    let mut continue_running = true;
    while continue_running && running.load(Ordering::SeqCst) {
        let started_at = Instant::now();
        let snapshot = platform::collect_snapshot(&cfg.postgres);
        if let Ok(raw) = serde_json::to_value(&snapshot) {
            archive.write_json_line(&raw);
        }
        let rates = derive_rates_or_reset(&mut prev, &snapshot);
        record_snapshot(&instruments, &filter, &snapshot, &rates);

        let flush_result = provider.force_flush();
        log_flush_result(started_at, flush_result.is_ok());

        let (next_state, event) = advance_export_state(export_state, flush_result.is_ok());
        handle_flush_event(
            event,
            flush_result
                .err()
                .as_ref()
                .map(|e| e as &dyn std::fmt::Display),
        );
        export_state = next_state;
        maybe_sleep_until_next_poll(cfg.once, started_at, cfg.poll_interval, &running);
        continue_running = !cfg.once;
    }

    let _ = provider.shutdown();
    Ok(())
}

fn make_stop_handler(signal: Arc<AtomicBool>) -> impl Fn() + Send + 'static {
    move || {
        signal.store(false, Ordering::SeqCst);
    }
}

fn install_signal_handler(running: &Arc<AtomicBool>) {
    if let Err(err) = ctrlc::set_handler(make_stop_handler(Arc::clone(running))) {
        warn!(error = %err, "failed to install signal handler");
    }
}

fn log_flush_result(started_at: Instant, flush_succeeded: bool) {
    let elapsed_ms = started_at.elapsed().as_millis();
    if flush_succeeded {
        debug!(elapsed_ms, "force_flush ok");
    } else {
        debug!(elapsed_ms, "force_flush err");
    }
}

fn handle_flush_event(event: FlushEvent, flush_error: Option<&dyn std::fmt::Display>) {
    if let Some(err) = flush_error {
        match event {
            FlushEvent::Reconnecting => {
                warn!(error = %err, "Exporter flush failed; reconnecting")
            }
            FlushEvent::StillUnavailable => {
                warn!(error = %err, "Exporter still unavailable")
            }
            FlushEvent::None | FlushEvent::Connected | FlushEvent::Reconnected => {}
        }
    } else {
        match event {
            FlushEvent::Connected => info!("Connected Successfully"),
            FlushEvent::Reconnected => info!("Reconnected Successfully"),
            FlushEvent::None | FlushEvent::Reconnecting | FlushEvent::StillUnavailable => {}
        }
    }
}

fn sleep_until(deadline: Instant, running: &AtomicBool, sleep_interval: Duration) {
    while Instant::now() < deadline && running.load(Ordering::SeqCst) {
        thread::sleep(sleep_interval);
    }
}

fn maybe_sleep_until_next_poll(
    once: bool,
    started_at: Instant,
    poll_interval: Duration,
    running: &AtomicBool,
) {
    if once {
        return;
    }
    let deadline = started_at + poll_interval;
    sleep_until(deadline, running, Duration::from_millis(500));
}

#[cfg(not(test))]
fn main() -> Result<()> {
    run()
}

fn record_snapshot(
    instruments: &Instruments,
    filter: &PrefixFilter,
    snap: &PostgresSnapshot,
    rates: &PostgresRates,
) {
    record_u64(
        &instruments.source_available,
        filter,
        "system.postgres.source.available",
        if snap.available { 1 } else { 0 },
    );
    record_u64(
        &instruments.up,
        filter,
        "system.postgres.up",
        if snap.up { 1 } else { 0 },
    );
    if !snap.available {
        return;
    }
    record_u64(
        &instruments.connections,
        filter,
        "system.postgres.connections",
        snap.connections,
    );
    record_u64(
        &instruments.xact_commit_total,
        filter,
        "system.postgres.transactions.committed.total",
        snap.xact_commit_total,
    );
    record_u64(
        &instruments.xact_rollback_total,
        filter,
        "system.postgres.transactions.rolled_back.total",
        snap.xact_rollback_total,
    );
    record_u64(
        &instruments.deadlocks_total,
        filter,
        "system.postgres.deadlocks.total",
        snap.deadlocks_total,
    );
    record_u64(
        &instruments.blks_read_total,
        filter,
        "system.postgres.blocks.read.total",
        snap.blks_read_total,
    );
    record_u64(
        &instruments.blks_hit_total,
        filter,
        "system.postgres.blocks.hit.total",
        snap.blks_hit_total,
    );
    record_f64(
        &instruments.commits_rate,
        filter,
        "system.postgres.transactions.committed.rate_per_second",
        rates.commits_per_second,
    );
    record_f64(
        &instruments.rollbacks_rate,
        filter,
        "system.postgres.transactions.rolled_back.rate_per_second",
        rates.rollbacks_per_second,
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
    postgres: Option<PostgresSection>,
    storage: Option<StorageSection>,
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
struct PostgresSection {
    executable: Option<String>,
    uri: Option<String>,
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

#[derive(Clone, Debug, Default, Deserialize)]
struct StorageSection {
    archive_enabled: Option<bool>,
    archive_dir: Option<String>,
    archive_max_file_bytes: Option<u64>,
    archive_retain_files: Option<usize>,
    archive_file_stem: Option<String>,
}

impl Config {
    fn load() -> Result<Self> {
        let args = env::args().collect::<Vec<_>>();
        Self::load_from_args(&args)
    }

    fn load_from_args(args: &[String]) -> Result<Self> {
        let once = args.iter().any(|arg| arg == "--once")
            || env::var("OJO_RUN_ONCE")
                .ok()
                .map(|v| {
                    matches!(
                        v.trim().to_ascii_lowercase().as_str(),
                        "1" | "true" | "yes" | "on"
                    )
                })
                .unwrap_or(false);
        let config_path = args
            .windows(2)
            .find(|pair| pair[0] == "--config")
            .map(|pair| pair[1].clone())
            .or_else(|| env::var("OJO_POSTGRES_CONFIG").ok())
            .unwrap_or_else(|| {
                resolve_default_config_path("postgres.yaml", "services/postgres/postgres.yaml")
            });

        let file_cfg = load_yaml_config_file(&config_path)?;
        let service = file_cfg.service.unwrap_or_default();
        let collection = file_cfg.collection.unwrap_or_default();
        let export = file_cfg.export.unwrap_or_default();
        let otlp = export.otlp.unwrap_or_default();
        let batch = export.batch.unwrap_or_default();
        let metrics = file_cfg.metrics.unwrap_or_default();
        let postgres = file_cfg.postgres.unwrap_or_default();
        let storage = file_cfg.storage.unwrap_or_default();

        let otlp_endpoint = otlp
            .endpoint
            .or_else(|| env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok())
            .unwrap_or_else(|| "http://127.0.0.1:4318/v1/metrics".to_string());
        let otlp_protocol = otlp
            .protocol
            .or_else(|| env::var("OTEL_EXPORTER_OTLP_PROTOCOL").ok())
            .unwrap_or_else(|| default_protocol_for_endpoint(Some(&otlp_endpoint)));

        Ok(Self {
            service_name: service.name.unwrap_or_else(|| "ojo-postgres".to_string()),
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
                .unwrap_or_else(|| vec!["system.postgres.".to_string()]),
            metrics_exclude: metrics.exclude.unwrap_or_default(),
            postgres: PostgresConfig {
                executable: postgres.executable.unwrap_or_else(|| "psql".to_string()),
                uri: postgres.uri.filter(|value| !value.trim().is_empty()),
            },
            archive: ArchiveStorageConfig {
                enabled: storage.archive_enabled.unwrap_or(true),
                archive_dir: storage
                    .archive_dir
                    .unwrap_or_else(|| "services/postgres/data".to_string()),
                max_file_bytes: storage.archive_max_file_bytes.unwrap_or(64 * 1024 * 1024),
                retain_files: storage.archive_retain_files.unwrap_or(8),
                file_stem: storage
                    .archive_file_stem
                    .unwrap_or_else(|| "postgres-snapshots".to_string()),
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

#[cfg(test)]
#[path = "tests/main_tests.rs"]
mod tests;
