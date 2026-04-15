use anyhow::{anyhow, Context, Result};
use host_collectors::{
    default_protocol_for_endpoint, init_meter_provider, ArchiveStorageConfig, JsonArchiveWriter,
    OtlpSettings, PrefixFilter,
};
use opentelemetry::metrics::{Counter, Gauge};
use opentelemetry::KeyValue;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::env;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

mod platform;

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
    nginx: NginxConfig,
    archive: ArchiveStorageConfig,
    once: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct NginxConfig {
    pub(crate) executable: String,
    pub(crate) status_url: String,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct NginxSnapshot {
    pub(crate) available: bool,
    pub(crate) up: bool,
    pub(crate) connections_active: u64,
    pub(crate) connections_reading: u64,
    pub(crate) connections_writing: u64,
    pub(crate) connections_waiting: u64,
    pub(crate) accepts_total: u64,
    pub(crate) handled_total: u64,
    pub(crate) requests_total: u64,
}

#[derive(Clone, Debug, Default)]
struct NginxRates {
    accepts_per_second: f64,
    requests_per_second: f64,
}

#[derive(Clone, Debug, Default)]
struct PrevState {
    last: Option<(NginxSnapshot, Instant)>,
}

impl PrevState {
    fn derive(&mut self, current: &NginxSnapshot) -> NginxRates {
        let now = Instant::now();
        let Some((previous, previous_at)) = &self.last else {
            self.last = Some((current.clone(), now));
            return NginxRates::default();
        };
        let elapsed = now
            .checked_duration_since(*previous_at)
            .unwrap_or_default()
            .as_secs_f64();
        if elapsed <= 0.0 {
            self.last = Some((current.clone(), now));
            return NginxRates::default();
        }
        let rates = NginxRates {
            accepts_per_second: saturating_rate(
                previous.accepts_total,
                current.accepts_total,
                elapsed,
            ),
            requests_per_second: saturating_rate(
                previous.requests_total,
                current.requests_total,
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

fn derive_rates_or_reset(prev: &mut PrevState, snapshot: &NginxSnapshot) -> NginxRates {
    if snapshot.available {
        prev.derive(snapshot)
    } else {
        prev.last = None;
        NginxRates::default()
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum NginxConnectionState {
    Unknown,
    Connected,
    Disconnected,
}

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

fn update_nginx_connection_state(
    previous: NginxConnectionState,
    snapshot: &NginxSnapshot,
) -> NginxConnectionState {
    let is_connected = snapshot.available && snapshot.up;
    match (previous, is_connected) {
        (NginxConnectionState::Unknown, true) => {
            info!("Nginx status endpoint connected successfully");
            NginxConnectionState::Connected
        }
        (NginxConnectionState::Unknown, false) => {
            warn!("Nginx status endpoint failed or Nginx not available");
            NginxConnectionState::Disconnected
        }
        (NginxConnectionState::Disconnected, true) => {
            info!("Nginx status endpoint reconnected successfully");
            NginxConnectionState::Connected
        }
        (NginxConnectionState::Disconnected, false) => {
            warn!("Nginx status endpoint still unavailable");
            NginxConnectionState::Disconnected
        }
        (NginxConnectionState::Connected, true) => NginxConnectionState::Connected,
        (NginxConnectionState::Connected, false) => {
            warn!("Nginx status endpoint disconnected");
            NginxConnectionState::Disconnected
        }
    }
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

fn handle_flush_event(event: FlushEvent, flush_error: Option<&dyn std::fmt::Display>) {
    if let Some(err) = flush_error {
        match event {
            FlushEvent::Reconnecting => warn!(error = %err, "Exporter flush failed; reconnecting"),
            FlushEvent::StillUnavailable => warn!(error = %err, "Exporter still unavailable"),
            FlushEvent::None | FlushEvent::Connected | FlushEvent::Reconnected => {}
        }
    } else {
        match event {
            FlushEvent::Connected => info!("Exporter connected successfully"),
            FlushEvent::Reconnected => info!("Exporter reconnected successfully"),
            FlushEvent::None | FlushEvent::Reconnecting | FlushEvent::StillUnavailable => {}
        }
    }
}

#[derive(Clone)]
struct Instruments {
    source_available: Gauge<u64>,
    up: Gauge<u64>,
    exporter_available: Gauge<u64>,
    exporter_reconnecting: Gauge<u64>,
    exporter_errors_total: Counter<u64>,
    connections_active: Gauge<u64>,
    connections_reading: Gauge<u64>,
    connections_writing: Gauge<u64>,
    connections_waiting: Gauge<u64>,
    accepts_total: Gauge<u64>,
    handled_total: Gauge<u64>,
    requests_total: Gauge<u64>,
    accepts_rate: Gauge<f64>,
    requests_rate: Gauge<f64>,
}

impl Instruments {
    fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self {
            source_available: meter.u64_gauge("system.nginx.source.available").build(),
            up: meter.u64_gauge("system.nginx.up").build(),
            exporter_available: meter.u64_gauge("system.nginx.exporter.available").build(),
            exporter_reconnecting: meter
                .u64_gauge("system.nginx.exporter.reconnecting")
                .build(),
            exporter_errors_total: meter
                .u64_counter("system.nginx.exporter.errors.total")
                .build(),
            connections_active: meter.u64_gauge("system.nginx.connections.active").build(),
            connections_reading: meter.u64_gauge("system.nginx.connections.reading").build(),
            connections_writing: meter.u64_gauge("system.nginx.connections.writing").build(),
            connections_waiting: meter.u64_gauge("system.nginx.connections.waiting").build(),
            accepts_total: meter
                .u64_gauge("system.nginx.connections.accepted.total")
                .build(),
            handled_total: meter
                .u64_gauge("system.nginx.connections.handled.total")
                .build(),
            requests_total: meter.u64_gauge("system.nginx.requests.total").build(),
            accepts_rate: meter
                .f64_gauge("system.nginx.connections.accepted.rate_per_second")
                .with_unit("{connections}/s")
                .build(),
            requests_rate: meter
                .f64_gauge("system.nginx.requests.rate_per_second")
                .with_unit("{requests}/s")
                .build(),
        }
    }
}

fn record_exporter_state(instruments: &Instruments, filter: &PrefixFilter, state: ExportState) {
    let connected = matches!(state, ExportState::Connected) as u64;
    let reconnecting = matches!(state, ExportState::Reconnecting) as u64;
    record_u64(
        &instruments.exporter_available,
        filter,
        "system.nginx.exporter.available",
        connected,
    );
    record_u64(
        &instruments.exporter_reconnecting,
        filter,
        "system.nginx.exporter.reconnecting",
        reconnecting,
    );
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

fn run() -> Result<()> {
    let dump_snapshot = env::args().any(|arg| arg == "--dump-snapshot");
    let cfg = Config::load()?;
    if dump_snapshot {
        let snapshot = platform::collect_snapshot(&cfg.nginx);
        println!("{}", serde_json::to_string_pretty(&snapshot)?);
        return Ok(());
    }
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init()
        .ok();
    info!(
        endpoint = %cfg.otlp_endpoint,
        protocol = %cfg.otlp_protocol,
        "Initializing metrics exporter"
    );

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
    let meter = opentelemetry::global::meter("ojo-nginx");
    let instruments = Instruments::new(&meter);
    let filter = PrefixFilter::new(cfg.metrics_include.clone(), cfg.metrics_exclude.clone());
    let mut prev = PrevState::default();
    let mut source_state = NginxConnectionState::Unknown;
    let mut export_state = ExportState::Pending;
    let mut archive = JsonArchiveWriter::from_config(&cfg.archive);

    #[cfg(test)]
    let mut iterations = 0u64;
    loop {
        let started_at = Instant::now();
        let snapshot = platform::collect_snapshot(&cfg.nginx);
        if let Ok(raw) = serde_json::to_value(&snapshot) {
            archive.write_json_line(&raw);
        }
        source_state = update_nginx_connection_state(source_state, &snapshot);
        let rates = derive_rates_or_reset(&mut prev, &snapshot);
        record_snapshot(&instruments, &filter, &snapshot, &rates);
        let flush_result = provider.force_flush();
        let (next_state, event) = advance_export_state(export_state, flush_result.is_ok());
        if flush_result.is_err() {
            instruments.exporter_errors_total.add(1, &[]);
        }
        record_exporter_state(&instruments, &filter, next_state);
        handle_flush_event(
            event,
            flush_result
                .as_ref()
                .err()
                .map(|err| err as &dyn std::fmt::Display),
        );
        export_state = next_state;

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

fn main() -> Result<()> {
    run()
}

fn record_snapshot(
    instruments: &Instruments,
    filter: &PrefixFilter,
    snap: &NginxSnapshot,
    rates: &NginxRates,
) {
    record_u64(
        &instruments.source_available,
        filter,
        "system.nginx.source.available",
        if snap.available { 1 } else { 0 },
    );
    record_u64(
        &instruments.up,
        filter,
        "system.nginx.up",
        if snap.up { 1 } else { 0 },
    );
    if !snap.available {
        return;
    }
    record_u64(
        &instruments.connections_active,
        filter,
        "system.nginx.connections.active",
        snap.connections_active,
    );
    record_u64(
        &instruments.connections_reading,
        filter,
        "system.nginx.connections.reading",
        snap.connections_reading,
    );
    record_u64(
        &instruments.connections_writing,
        filter,
        "system.nginx.connections.writing",
        snap.connections_writing,
    );
    record_u64(
        &instruments.connections_waiting,
        filter,
        "system.nginx.connections.waiting",
        snap.connections_waiting,
    );
    record_u64(
        &instruments.accepts_total,
        filter,
        "system.nginx.connections.accepted.total",
        snap.accepts_total,
    );
    record_u64(
        &instruments.handled_total,
        filter,
        "system.nginx.connections.handled.total",
        snap.handled_total,
    );
    record_u64(
        &instruments.requests_total,
        filter,
        "system.nginx.requests.total",
        snap.requests_total,
    );
    record_f64(
        &instruments.accepts_rate,
        filter,
        "system.nginx.connections.accepted.rate_per_second",
        rates.accepts_per_second,
    );
    record_f64(
        &instruments.requests_rate,
        filter,
        "system.nginx.requests.rate_per_second",
        rates.requests_per_second,
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
            .or_else(|| env::var("OJO_NGINX_CONFIG").ok())
            .unwrap_or_else(|| {
                resolve_default_config_path("nginx.yaml", "services/nginx/nginx.yaml")
            });

        let file_cfg = load_yaml_config_file(&config_path)?;
        let service = file_cfg.service.unwrap_or_default();
        let collection = file_cfg.collection.unwrap_or_default();
        let export = file_cfg.export.unwrap_or_default();
        let otlp = export.otlp.unwrap_or_default();
        let batch = export.batch.unwrap_or_default();
        let metrics = file_cfg.metrics.unwrap_or_default();
        let nginx = file_cfg.nginx.unwrap_or_default();
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
            service_name: service.name.unwrap_or_else(|| "ojo-nginx".to_string()),
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
                .unwrap_or_else(|| vec!["system.nginx.".to_string()]),
            metrics_exclude: metrics.exclude.unwrap_or_default(),
            nginx: NginxConfig {
                executable: nginx.executable.unwrap_or_else(|| "curl".to_string()),
                status_url: nginx
                    .status_url
                    .unwrap_or_else(|| "http://127.0.0.1/nginx_status".to_string()),
            },
            archive: ArchiveStorageConfig {
                enabled: storage.archive_enabled.unwrap_or(true),
                archive_dir: storage
                    .archive_dir
                    .unwrap_or_else(|| "services/nginx/data".to_string()),
                max_file_bytes: storage.archive_max_file_bytes.unwrap_or(64 * 1024 * 1024),
                retain_files: storage.archive_retain_files.unwrap_or(8),
                file_stem: storage
                    .archive_file_stem
                    .unwrap_or_else(|| "nginx-snapshots".to_string()),
            },
            once,
        })
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileConfig {
    service: Option<ServiceSection>,
    collection: Option<CollectionSection>,
    export: Option<ExportSection>,
    metrics: Option<MetricSection>,
    nginx: Option<NginxSection>,
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
struct NginxSection {
    executable: Option<String>,
    status_url: Option<String>,
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

#[cfg(test)]
mod tests;
