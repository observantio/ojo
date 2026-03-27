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

fn derive_rates_or_reset(prev: &mut PrevState, snapshot: &NfsClientSnapshot) -> NfsRates {
    if snapshot.available {
        prev.derive(snapshot)
    } else {
        prev.last = None;
        NfsRates::default()
    }
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
        let rates = derive_rates_or_reset(&mut prev, &snapshot);
        record_snapshot(&instruments, &filter, &snapshot, &rates);

        let flush_result = provider.force_flush();
        if flush_result.is_ok() {
            debug!(
                elapsed_ms = started_at.elapsed().as_millis(),
                "force_flush ok"
            );
        } else {
            debug!(
                elapsed_ms = started_at.elapsed().as_millis(),
                "force_flush err"
            );
        }

        let (next_state, event) = advance_export_state(export_state, flush_result.is_ok());
        if let Err(err) = flush_result {
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
        export_state = next_state;

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

#[cfg(test)]
mod tests {
    use super::{
        advance_export_state, derive_rates_or_reset, load_yaml_config_file, record_snapshot,
        resolve_default_config_path, saturating_rate, Config, ExportState, FlushEvent, Instruments,
        NfsClientSnapshot, NfsRates, PrevState,
    };
    use host_collectors::PrefixFilter;
    use std::fs;
    use std::sync::{Mutex, OnceLock};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn unique_temp_path(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("ojo-nfs-{name}-{}-{nanos}", std::process::id()))
    }

    #[test]
    fn saturating_rate_handles_counter_reset_and_normal_rate() {
        assert_eq!(saturating_rate(100, 90, 1.0), 0.0);
        assert_eq!(saturating_rate(10, 40, 2.0), 15.0);
    }

    #[test]
    fn prev_state_derive_initializes_and_then_computes_rates() {
        let mut state = PrevState::default();
        let first = NfsClientSnapshot {
            rpc_calls_total: 100,
            rpc_retransmissions_total: 10,
            ..NfsClientSnapshot::default()
        };
        let rates = state.derive(&first);
        assert_eq!(rates.rpc_calls_rate, 0.0);
        assert_eq!(rates.rpc_retransmissions_rate, 0.0);

        state.last = Some((first, Instant::now() - Duration::from_secs(2)));
        let second = NfsClientSnapshot {
            rpc_calls_total: 180,
            rpc_retransmissions_total: 16,
            ..NfsClientSnapshot::default()
        };
        let rates = state.derive(&second);
        assert!(rates.rpc_calls_rate > 30.0);
        assert!(rates.rpc_retransmissions_rate > 2.0);
    }

    #[test]
    fn derive_rates_or_reset_resets_state_when_snapshot_unavailable() {
        let mut state = PrevState::default();
        let available = NfsClientSnapshot {
            available: true,
            rpc_calls_total: 10,
            rpc_retransmissions_total: 1,
            ..NfsClientSnapshot::default()
        };
        let _ = derive_rates_or_reset(&mut state, &available);
        assert!(state.last.is_some());

        let unavailable = NfsClientSnapshot::default();
        let rates = derive_rates_or_reset(&mut state, &unavailable);
        assert_eq!(rates.rpc_calls_rate, 0.0);
        assert_eq!(rates.rpc_retransmissions_rate, 0.0);
        assert!(state.last.is_none());
    }

    #[test]
    fn advance_export_state_covers_all_transitions() {
        assert_eq!(
            advance_export_state(ExportState::Pending, true),
            (ExportState::Connected, FlushEvent::Connected)
        );
        assert_eq!(
            advance_export_state(ExportState::Reconnecting, true),
            (ExportState::Connected, FlushEvent::Reconnected)
        );
        assert_eq!(
            advance_export_state(ExportState::Connected, true),
            (ExportState::Connected, FlushEvent::None)
        );
        assert_eq!(
            advance_export_state(ExportState::Connected, false),
            (ExportState::Reconnecting, FlushEvent::Reconnecting)
        );
        assert_eq!(
            advance_export_state(ExportState::Pending, false),
            (ExportState::Reconnecting, FlushEvent::StillUnavailable)
        );
        assert_eq!(
            advance_export_state(ExportState::Reconnecting, false),
            (ExportState::Reconnecting, FlushEvent::StillUnavailable)
        );
    }

    #[test]
    fn resolve_default_config_path_prefers_local_file_when_present() {
        let local = unique_temp_path("nfs-local.yaml");
        fs::write(&local, "service: {}\n").expect("write local");
        let chosen = resolve_default_config_path(local.to_string_lossy().as_ref(), "fallback.yaml");
        assert_eq!(chosen, local.to_string_lossy());
        fs::remove_file(&local).expect("cleanup local");

        let missing = unique_temp_path("nfs-missing.yaml");
        let chosen =
            resolve_default_config_path(missing.to_string_lossy().as_ref(), "fallback.yaml");
        assert_eq!(chosen, "fallback.yaml");
    }

    #[test]
    fn load_yaml_config_file_handles_missing_empty_and_valid_yaml() {
        let missing = unique_temp_path("nfs-missing-config.yaml");
        let missing_err = load_yaml_config_file(missing.to_string_lossy().as_ref()).unwrap_err();
        assert!(
            missing_err.to_string().contains("was not found"),
            "{missing_err}"
        );

        let empty = unique_temp_path("nfs-empty-config.yaml");
        fs::write(&empty, " \n").expect("write empty");
        let empty_err = load_yaml_config_file(empty.to_string_lossy().as_ref()).unwrap_err();
        assert!(empty_err.to_string().contains("is empty"), "{empty_err}");
        fs::remove_file(&empty).expect("cleanup empty");

        let valid = unique_temp_path("nfs-valid-config.yaml");
        fs::write(
            &valid,
            "service:\n  name: ojo-nfs-client\n  instance_id: nfs-1\ncollection:\n  poll_interval_secs: 2\nnfs_client:\n  executable: nfsstat\n",
        )
        .expect("write valid");
        let parsed = load_yaml_config_file(valid.to_string_lossy().as_ref());
        assert!(parsed.is_ok(), "{parsed:?}");
        fs::remove_file(&valid).expect("cleanup valid");
    }

    #[test]
    fn config_load_reads_yaml_and_applies_defaults() {
        let _guard = env_lock().lock().expect("env lock");
        let path = unique_temp_path("nfs-load.yaml");
        fs::write(
            &path,
            "service:\n  name: nfs-svc\n  instance_id: nfs-01\ncollection:\n  poll_interval_secs: 1\nnfs_client:\n  executable: nfsstat\n",
        )
        .expect("write config");

        std::env::set_var("OJO_NFS_CLIENT_CONFIG", &path);
        std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
        std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");

        let cfg = Config::load().expect("load config");
        assert_eq!(cfg.service_name, "nfs-svc");
        assert_eq!(cfg.instance_id, "nfs-01");
        assert_eq!(cfg.poll_interval, Duration::from_secs(1));
        assert_eq!(cfg.otlp_endpoint, "http://127.0.0.1:4318/v1/metrics");
        assert_eq!(cfg.otlp_protocol, "http/protobuf");
        assert_eq!(cfg.metrics_include, vec!["system.nfs_client.".to_string()]);
        assert_eq!(cfg.nfs_client.executable.as_deref(), Some("nfsstat"));

        std::env::remove_var("OJO_NFS_CLIENT_CONFIG");
        fs::remove_file(&path).expect("cleanup config");
    }

    #[test]
    fn config_load_uses_otlp_env_fallback_when_export_section_missing() {
        let _guard = env_lock().lock().expect("env lock");
        let path = unique_temp_path("nfs-env-load.yaml");
        fs::write(
            &path,
            "service:\n  name: nfs-svc\n  instance_id: nfs-01\ncollection:\n  poll_interval_secs: 2\nnfs_client:\n  executable: nfsstat\n",
        )
        .expect("write config");

        std::env::set_var("OJO_NFS_CLIENT_CONFIG", &path);
        std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4317");
        std::env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc");

        let cfg = Config::load().expect("load config");
        assert_eq!(cfg.otlp_endpoint, "http://127.0.0.1:4317");
        assert_eq!(cfg.otlp_protocol, "grpc");

        std::env::remove_var("OJO_NFS_CLIENT_CONFIG");
        std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
        std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");
        fs::remove_file(&path).expect("cleanup config");
    }

    #[test]
    fn record_snapshot_handles_unavailable_and_available_samples() {
        let meter = opentelemetry::global::meter("nfs-test-meter");
        let instruments = Instruments::new(&meter);
        let filter = PrefixFilter::new(vec!["system.nfs_client.".to_string()], vec![]);

        let unavailable = NfsClientSnapshot::default();
        record_snapshot(&instruments, &filter, &unavailable, &NfsRates::default());

        let available = NfsClientSnapshot {
            available: true,
            mounts: 2,
            rpc_calls_total: 100,
            rpc_retransmissions_total: 5,
            rpc_auth_refreshes_total: 1,
        };
        let rates = NfsRates {
            rpc_calls_rate: 10.0,
            rpc_retransmissions_rate: 0.5,
        };
        record_snapshot(&instruments, &filter, &available, &rates);
    }

    #[test]
    fn main_runs_once_with_temp_config() {
        let _guard = env_lock().lock().expect("env lock");
        let path = unique_temp_path("nfs-main-once.yaml");
        fs::write(
            &path,
            "service:\n  name: nfs-main-test\n  instance_id: nfs-main-01\ncollection:\n  poll_interval_secs: 1\nnfs_client:\n  executable: /definitely/missing/nfsstat\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
        )
        .expect("write config");

        std::env::set_var("OJO_NFS_CLIENT_CONFIG", &path);
        std::env::set_var("OJO_RUN_ONCE", "1");
        let result = super::main();
        assert!(result.is_ok(), "{result:?}");
        std::env::remove_var("OJO_NFS_CLIENT_CONFIG");
        std::env::remove_var("OJO_RUN_ONCE");
        fs::remove_file(&path).expect("cleanup config");
    }
}
