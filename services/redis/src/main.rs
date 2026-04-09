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
    redis: RedisConfig,
    archive: ArchiveStorageConfig,
    once: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RedisConfig {
    pub(crate) executable: String,
    pub(crate) host: Option<String>,
    pub(crate) port: Option<u16>,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct RedisSnapshot {
    pub(crate) available: bool,
    pub(crate) up: bool,
    pub(crate) connected_clients: u64,
    pub(crate) blocked_clients: u64,
    pub(crate) memory_used_bytes: u64,
    pub(crate) memory_max_bytes: u64,
    pub(crate) uptime_seconds: u64,
    pub(crate) commands_processed_total: u64,
    pub(crate) connections_received_total: u64,
    pub(crate) keyspace_hits_total: u64,
    pub(crate) keyspace_misses_total: u64,
    pub(crate) expired_keys_total: u64,
    pub(crate) evicted_keys_total: u64,
}

#[derive(Clone, Debug, Default)]
struct RedisRates {
    commands_per_second: f64,
    connections_per_second: f64,
    hit_ratio: f64,
}

#[derive(Clone, Debug, Default)]
struct PrevState {
    last: Option<(RedisSnapshot, Instant)>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum RedisConnectionState {
    Unknown,
    Connected,
    Disconnected,
}

fn update_redis_connection_state(
    previous: RedisConnectionState,
    snapshot: &RedisSnapshot,
) -> RedisConnectionState {
    let is_connected = snapshot.available && snapshot.up;
    match (previous, is_connected) {
        (RedisConnectionState::Unknown, true) => {
            info!("Redis connected successfully");
            RedisConnectionState::Connected
        }
        (RedisConnectionState::Unknown, false) => {
            warn!("Redis connection failed or Redis not available");
            RedisConnectionState::Disconnected
        }
        (RedisConnectionState::Disconnected, true) => {
            info!("Redis reconnected successfully");
            RedisConnectionState::Connected
        }
        (RedisConnectionState::Disconnected, false) => {
            warn!("Redis still unavailable");
            RedisConnectionState::Disconnected
        }
        (RedisConnectionState::Connected, true) => RedisConnectionState::Connected,
        (RedisConnectionState::Connected, false) => {
            warn!("Redis disconnected");
            RedisConnectionState::Disconnected
        }
    }
}

impl PrevState {
    fn derive(&mut self, current: &RedisSnapshot) -> RedisRates {
        let now = Instant::now();
        let Some((previous, previous_at)) = &self.last else {
            self.last = Some((current.clone(), now));
            return RedisRates {
                hit_ratio: hit_ratio(current.keyspace_hits_total, current.keyspace_misses_total),
                ..RedisRates::default()
            };
        };
        let elapsed = now
            .checked_duration_since(*previous_at)
            .unwrap_or_default()
            .as_secs_f64();
        if elapsed <= 0.0 {
            self.last = Some((current.clone(), now));
            return RedisRates {
                hit_ratio: hit_ratio(current.keyspace_hits_total, current.keyspace_misses_total),
                ..RedisRates::default()
            };
        }
        let rates = RedisRates {
            commands_per_second: saturating_rate(
                previous.commands_processed_total,
                current.commands_processed_total,
                elapsed,
            ),
            connections_per_second: saturating_rate(
                previous.connections_received_total,
                current.connections_received_total,
                elapsed,
            ),
            hit_ratio: hit_ratio(current.keyspace_hits_total, current.keyspace_misses_total),
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

fn hit_ratio(hits: u64, misses: u64) -> f64 {
    let total = hits.saturating_add(misses);
    if total == 0 {
        0.0
    } else {
        hits as f64 / total as f64
    }
}

fn derive_rates_or_reset(prev: &mut PrevState, snapshot: &RedisSnapshot) -> RedisRates {
    if snapshot.available {
        prev.derive(snapshot)
    } else {
        prev.last = None;
        RedisRates::default()
    }
}

#[derive(Clone)]
struct Instruments {
    source_available: Gauge<u64>,
    up: Gauge<u64>,
    connected_clients: Gauge<u64>,
    blocked_clients: Gauge<u64>,
    memory_used_bytes: Gauge<u64>,
    memory_max_bytes: Gauge<u64>,
    uptime_seconds: Gauge<u64>,
    commands_total: Gauge<u64>,
    connections_total: Gauge<u64>,
    keyspace_hits_total: Gauge<u64>,
    keyspace_misses_total: Gauge<u64>,
    expired_keys_total: Gauge<u64>,
    evicted_keys_total: Gauge<u64>,
    commands_rate: Gauge<f64>,
    connections_rate: Gauge<f64>,
    hit_ratio: Gauge<f64>,
}

impl Instruments {
    fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self {
            source_available: meter.u64_gauge("system.redis.source.available").build(),
            up: meter.u64_gauge("system.redis.up").build(),
            connected_clients: meter.u64_gauge("system.redis.clients.connected").build(),
            blocked_clients: meter.u64_gauge("system.redis.clients.blocked").build(),
            memory_used_bytes: meter.u64_gauge("system.redis.memory.used.bytes").build(),
            memory_max_bytes: meter.u64_gauge("system.redis.memory.max.bytes").build(),
            uptime_seconds: meter.u64_gauge("system.redis.uptime.seconds").build(),
            commands_total: meter
                .u64_gauge("system.redis.commands.processed.total")
                .build(),
            connections_total: meter
                .u64_gauge("system.redis.connections.received.total")
                .build(),
            keyspace_hits_total: meter.u64_gauge("system.redis.keyspace.hits.total").build(),
            keyspace_misses_total: meter
                .u64_gauge("system.redis.keyspace.misses.total")
                .build(),
            expired_keys_total: meter.u64_gauge("system.redis.keys.expired.total").build(),
            evicted_keys_total: meter.u64_gauge("system.redis.keys.evicted.total").build(),
            commands_rate: meter
                .f64_gauge("system.redis.commands.processed.rate_per_second")
                .with_unit("{commands}/s")
                .build(),
            connections_rate: meter
                .f64_gauge("system.redis.connections.received.rate_per_second")
                .with_unit("{connections}/s")
                .build(),
            hit_ratio: meter
                .f64_gauge("system.redis.keyspace.hit.ratio")
                .with_unit("1")
                .build(),
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

fn run() -> Result<()> {
    let dump_snapshot = env::args().any(|arg| arg == "--dump-snapshot");
    let cfg = Config::load()?;
    if dump_snapshot {
        let snapshot = platform::collect_snapshot(&cfg.redis);
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
    let meter = opentelemetry::global::meter("ojo-redis");
    let instruments = Instruments::new(&meter);
    let filter = PrefixFilter::new(cfg.metrics_include.clone(), cfg.metrics_exclude.clone());
    let mut prev = PrevState::default();
    let mut connection_state = RedisConnectionState::Unknown;
    let mut archive = JsonArchiveWriter::from_config(&cfg.archive);

    #[cfg(test)]
    let mut iterations = 0u64;
    loop {
        let started_at = Instant::now();
        let snapshot = platform::collect_snapshot(&cfg.redis);
        if let Ok(raw) = serde_json::to_value(&snapshot) {
            archive.write_json_line(&raw);
        }
        connection_state = update_redis_connection_state(connection_state, &snapshot);
        let rates = derive_rates_or_reset(&mut prev, &snapshot);
        record_snapshot(&instruments, &filter, &snapshot, &rates);
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

fn record_snapshot(
    instruments: &Instruments,
    filter: &PrefixFilter,
    snap: &RedisSnapshot,
    rates: &RedisRates,
) {
    record_u64(
        &instruments.source_available,
        filter,
        "system.redis.source.available",
        if snap.available { 1 } else { 0 },
    );
    record_u64(
        &instruments.up,
        filter,
        "system.redis.up",
        if snap.up { 1 } else { 0 },
    );
    if !snap.available {
        record_u64(
            &instruments.connected_clients,
            filter,
            "system.redis.clients.connected",
            0,
        );
        record_u64(
            &instruments.blocked_clients,
            filter,
            "system.redis.clients.blocked",
            0,
        );
        record_u64(
            &instruments.memory_used_bytes,
            filter,
            "system.redis.memory.used.bytes",
            0,
        );
        record_u64(
            &instruments.memory_max_bytes,
            filter,
            "system.redis.memory.max.bytes",
            0,
        );
        record_u64(
            &instruments.uptime_seconds,
            filter,
            "system.redis.uptime.seconds",
            0,
        );
        record_u64(
            &instruments.commands_total,
            filter,
            "system.redis.commands.processed.total",
            0,
        );
        record_u64(
            &instruments.connections_total,
            filter,
            "system.redis.connections.received.total",
            0,
        );
        record_u64(
            &instruments.keyspace_hits_total,
            filter,
            "system.redis.keyspace.hits.total",
            0,
        );
        record_u64(
            &instruments.keyspace_misses_total,
            filter,
            "system.redis.keyspace.misses.total",
            0,
        );
        record_u64(
            &instruments.expired_keys_total,
            filter,
            "system.redis.keys.expired.total",
            0,
        );
        record_u64(
            &instruments.evicted_keys_total,
            filter,
            "system.redis.keys.evicted.total",
            0,
        );
        record_f64(
            &instruments.commands_rate,
            filter,
            "system.redis.commands.processed.rate_per_second",
            0.0,
        );
        record_f64(
            &instruments.connections_rate,
            filter,
            "system.redis.connections.received.rate_per_second",
            0.0,
        );
        record_f64(
            &instruments.hit_ratio,
            filter,
            "system.redis.keyspace.hit.ratio",
            0.0,
        );
        return;
    }
    record_u64(
        &instruments.connected_clients,
        filter,
        "system.redis.clients.connected",
        snap.connected_clients,
    );
    record_u64(
        &instruments.blocked_clients,
        filter,
        "system.redis.clients.blocked",
        snap.blocked_clients,
    );
    record_u64(
        &instruments.memory_used_bytes,
        filter,
        "system.redis.memory.used.bytes",
        snap.memory_used_bytes,
    );
    record_u64(
        &instruments.memory_max_bytes,
        filter,
        "system.redis.memory.max.bytes",
        snap.memory_max_bytes,
    );
    record_u64(
        &instruments.uptime_seconds,
        filter,
        "system.redis.uptime.seconds",
        snap.uptime_seconds,
    );
    record_u64(
        &instruments.commands_total,
        filter,
        "system.redis.commands.processed.total",
        snap.commands_processed_total,
    );
    record_u64(
        &instruments.connections_total,
        filter,
        "system.redis.connections.received.total",
        snap.connections_received_total,
    );
    record_u64(
        &instruments.keyspace_hits_total,
        filter,
        "system.redis.keyspace.hits.total",
        snap.keyspace_hits_total,
    );
    record_u64(
        &instruments.keyspace_misses_total,
        filter,
        "system.redis.keyspace.misses.total",
        snap.keyspace_misses_total,
    );
    record_u64(
        &instruments.expired_keys_total,
        filter,
        "system.redis.keys.expired.total",
        snap.expired_keys_total,
    );
    record_u64(
        &instruments.evicted_keys_total,
        filter,
        "system.redis.keys.evicted.total",
        snap.evicted_keys_total,
    );
    record_f64(
        &instruments.commands_rate,
        filter,
        "system.redis.commands.processed.rate_per_second",
        rates.commands_per_second,
    );
    record_f64(
        &instruments.connections_rate,
        filter,
        "system.redis.connections.received.rate_per_second",
        rates.connections_per_second,
    );
    record_f64(
        &instruments.hit_ratio,
        filter,
        "system.redis.keyspace.hit.ratio",
        rates.hit_ratio,
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
            .or_else(|| env::var("OJO_REDIS_CONFIG").ok())
            .unwrap_or_else(|| {
                resolve_default_config_path("redis.yaml", "services/redis/redis.yaml")
            });

        let file_cfg = load_yaml_config_file(&config_path)?;
        let service = file_cfg.service.unwrap_or_default();
        let collection = file_cfg.collection.unwrap_or_default();
        let export = file_cfg.export.unwrap_or_default();
        let otlp = export.otlp.unwrap_or_default();
        let batch = export.batch.unwrap_or_default();
        let metrics = file_cfg.metrics.unwrap_or_default();
        let redis = file_cfg.redis.unwrap_or_default();
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
            service_name: service.name.unwrap_or_else(|| "ojo-redis".to_string()),
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
                .unwrap_or_else(|| vec!["system.redis.".to_string()]),
            metrics_exclude: metrics.exclude.unwrap_or_default(),
            redis: RedisConfig {
                executable: redis.executable.unwrap_or_else(|| "redis-cli".to_string()),
                host: redis.host.filter(|v| !v.trim().is_empty()),
                port: redis.port,
                username: redis.username.filter(|v| !v.trim().is_empty()),
                password: redis.password.filter(|v| !v.trim().is_empty()),
            },
            archive: ArchiveStorageConfig {
                enabled: storage.archive_enabled.unwrap_or(true),
                archive_dir: storage
                    .archive_dir
                    .unwrap_or_else(|| "services/redis/data".to_string()),
                max_file_bytes: storage.archive_max_file_bytes.unwrap_or(64 * 1024 * 1024),
                retain_files: storage.archive_retain_files.unwrap_or(8),
                file_stem: storage
                    .archive_file_stem
                    .unwrap_or_else(|| "redis-snapshots".to_string()),
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
    redis: Option<RedisSection>,
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
struct RedisSection {
    executable: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    username: Option<String>,
    password: Option<String>,
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
