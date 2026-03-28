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
    mysql: MysqlConfig,
    once: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct MysqlConfig {
    pub(crate) executable: String,
    pub(crate) host: Option<String>,
    pub(crate) port: Option<u16>,
    pub(crate) user: Option<String>,
    pub(crate) password: Option<String>,
    pub(crate) database: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct MysqlSnapshot {
    pub(crate) available: bool,
    pub(crate) up: bool,
    pub(crate) connections: u64,
    pub(crate) threads_running: u64,
    pub(crate) queries_total: u64,
    pub(crate) slow_queries_total: u64,
    pub(crate) bytes_received_total: u64,
    pub(crate) bytes_sent_total: u64,
}

#[derive(Clone, Debug, Default)]
struct MysqlRates {
    queries_per_second: f64,
    bytes_received_per_second: f64,
    bytes_sent_per_second: f64,
}

#[derive(Clone, Debug, Default)]
struct PrevState {
    last: Option<(MysqlSnapshot, Instant)>,
}

impl PrevState {
    fn derive(&mut self, current: &MysqlSnapshot) -> MysqlRates {
        let now = Instant::now();
        let Some((previous, previous_at)) = &self.last else {
            self.last = Some((current.clone(), now));
            return MysqlRates::default();
        };
        let elapsed = now
            .checked_duration_since(*previous_at)
            .unwrap_or_default()
            .as_secs_f64();
        if elapsed <= 0.0 {
            self.last = Some((current.clone(), now));
            return MysqlRates::default();
        }
        let rates = MysqlRates {
            queries_per_second: saturating_rate(
                previous.queries_total,
                current.queries_total,
                elapsed,
            ),
            bytes_received_per_second: saturating_rate(
                previous.bytes_received_total,
                current.bytes_received_total,
                elapsed,
            ),
            bytes_sent_per_second: saturating_rate(
                previous.bytes_sent_total,
                current.bytes_sent_total,
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

fn derive_rates_or_reset(prev: &mut PrevState, snapshot: &MysqlSnapshot) -> MysqlRates {
    if snapshot.available {
        prev.derive(snapshot)
    } else {
        prev.last = None;
        MysqlRates::default()
    }
}

struct Instruments {
    source_available: Gauge<u64>,
    up: Gauge<u64>,
    connections: Gauge<u64>,
    threads_running: Gauge<u64>,
    queries_total: Gauge<u64>,
    slow_queries_total: Gauge<u64>,
    bytes_received_total: Gauge<u64>,
    bytes_sent_total: Gauge<u64>,
    queries_rate: Gauge<f64>,
    bytes_received_rate: Gauge<f64>,
    bytes_sent_rate: Gauge<f64>,
}

impl Instruments {
    fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self {
            source_available: meter.u64_gauge("system.mysql.source.available").build(),
            up: meter.u64_gauge("system.mysql.up").build(),
            connections: meter.u64_gauge("system.mysql.connections").build(),
            threads_running: meter.u64_gauge("system.mysql.threads.running").build(),
            queries_total: meter.u64_gauge("system.mysql.queries.total").build(),
            slow_queries_total: meter.u64_gauge("system.mysql.slow_queries.total").build(),
            bytes_received_total: meter.u64_gauge("system.mysql.bytes.received.total").build(),
            bytes_sent_total: meter.u64_gauge("system.mysql.bytes.sent.total").build(),
            queries_rate: meter
                .f64_gauge("system.mysql.queries.rate_per_second")
                .with_unit("{queries}/s")
                .build(),
            bytes_received_rate: meter
                .f64_gauge("system.mysql.bytes.received.rate_per_second")
                .with_unit("By/s")
                .build(),
            bytes_sent_rate: meter
                .f64_gauge("system.mysql.bytes.sent.rate_per_second")
                .with_unit("By/s")
                .build(),
        }
    }
}

fn make_stop_handler(signal: Arc<AtomicBool>) -> impl Fn() + Send + 'static {
    move || {
        signal.store(false, Ordering::SeqCst);
    }
}

fn install_signal_handler(running: &Arc<AtomicBool>) -> Result<()> {
    ctrlc::set_handler(make_stop_handler(Arc::clone(running)))?;
    Ok(())
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

fn run() -> Result<()> {
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
    let meter = opentelemetry::global::meter("ojo-mysql");
    let instruments = Instruments::new(&meter);
    let filter = PrefixFilter::new(cfg.metrics_include.clone(), cfg.metrics_exclude.clone());
    let mut prev = PrevState::default();

    let running = Arc::new(AtomicBool::new(true));
    install_signal_handler(&running)?;

    let mut export_state = ExportState::Pending;
    while running.load(Ordering::SeqCst) {
        let started_at = Instant::now();
        let snapshot = platform::collect_snapshot(&cfg.mysql);
        let rates = derive_rates_or_reset(&mut prev, &snapshot);
        record_snapshot(&instruments, &filter, &snapshot, &rates);

        let flush_result = provider.force_flush();
        log_flush_result(started_at, flush_result.is_ok());

        let (next_state, event) = advance_export_state(export_state, flush_result.is_ok());
        handle_flush_event(
            event,
            flush_result
                .as_ref()
                .err()
                .map(|err| err as &dyn std::fmt::Display),
        );
        export_state = next_state;
        maybe_sleep_until_next_poll(cfg.once, started_at, cfg.poll_interval, &running);
        if cfg.once {
            break;
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
    snap: &MysqlSnapshot,
    rates: &MysqlRates,
) {
    record_u64(
        &instruments.source_available,
        filter,
        "system.mysql.source.available",
        if snap.available { 1 } else { 0 },
    );
    record_u64(
        &instruments.up,
        filter,
        "system.mysql.up",
        if snap.up { 1 } else { 0 },
    );
    if !snap.available {
        return;
    }
    record_u64(
        &instruments.connections,
        filter,
        "system.mysql.connections",
        snap.connections,
    );
    record_u64(
        &instruments.threads_running,
        filter,
        "system.mysql.threads.running",
        snap.threads_running,
    );
    record_u64(
        &instruments.queries_total,
        filter,
        "system.mysql.queries.total",
        snap.queries_total,
    );
    record_u64(
        &instruments.slow_queries_total,
        filter,
        "system.mysql.slow_queries.total",
        snap.slow_queries_total,
    );
    record_u64(
        &instruments.bytes_received_total,
        filter,
        "system.mysql.bytes.received.total",
        snap.bytes_received_total,
    );
    record_u64(
        &instruments.bytes_sent_total,
        filter,
        "system.mysql.bytes.sent.total",
        snap.bytes_sent_total,
    );
    record_f64(
        &instruments.queries_rate,
        filter,
        "system.mysql.queries.rate_per_second",
        rates.queries_per_second,
    );
    record_f64(
        &instruments.bytes_received_rate,
        filter,
        "system.mysql.bytes.received.rate_per_second",
        rates.bytes_received_per_second,
    );
    record_f64(
        &instruments.bytes_sent_rate,
        filter,
        "system.mysql.bytes.sent.rate_per_second",
        rates.bytes_sent_per_second,
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
    mysql: Option<MysqlSection>,
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
struct MysqlSection {
    executable: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    user: Option<String>,
    password: Option<String>,
    database: Option<String>,
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
            .or_else(|| env::var("OJO_MYSQL_CONFIG").ok())
            .unwrap_or_else(|| {
                resolve_default_config_path("mysql.yaml", "services/mysql/mysql.yaml")
            });

        let file_cfg = load_yaml_config_file(&config_path)?;
        let service = file_cfg.service.unwrap_or_default();
        let collection = file_cfg.collection.unwrap_or_default();
        let export = file_cfg.export.unwrap_or_default();
        let otlp = export.otlp.unwrap_or_default();
        let batch = export.batch.unwrap_or_default();
        let metrics = file_cfg.metrics.unwrap_or_default();
        let mysql = file_cfg.mysql.unwrap_or_default();

        let otlp_endpoint = otlp
            .endpoint
            .or_else(|| env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok())
            .unwrap_or_else(|| "http://127.0.0.1:4318/v1/metrics".to_string());
        let otlp_protocol = otlp
            .protocol
            .or_else(|| env::var("OTEL_EXPORTER_OTLP_PROTOCOL").ok())
            .unwrap_or_else(|| default_protocol_for_endpoint(Some(&otlp_endpoint)));

        Ok(Self {
            service_name: service.name.unwrap_or_else(|| "ojo-mysql".to_string()),
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
                .unwrap_or_else(|| vec!["system.mysql.".to_string()]),
            metrics_exclude: metrics.exclude.unwrap_or_default(),
            mysql: MysqlConfig {
                executable: mysql.executable.unwrap_or_else(|| "mysql".to_string()),
                host: mysql.host.filter(|v| !v.trim().is_empty()),
                port: mysql.port,
                user: mysql.user.filter(|v| !v.trim().is_empty()),
                password: mysql.password.filter(|v| !v.trim().is_empty()),
                database: mysql.database.filter(|v| !v.trim().is_empty()),
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
        advance_export_state, derive_rates_or_reset, handle_flush_event, load_yaml_config_file,
        log_flush_result, make_stop_handler, maybe_sleep_until_next_poll, record_snapshot,
        resolve_default_config_path, saturating_rate, sleep_until, Config, ExportState, FlushEvent,
        Instruments, MysqlRates, MysqlSnapshot, PrevState,
    };
    use host_collectors::PrefixFilter;
    use std::fs;
    use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc, Mutex, OnceLock};
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
        std::env::temp_dir().join(format!("ojo-mysql-{name}-{}-{nanos}", std::process::id()))
    }

    #[test]
    fn saturating_rate_handles_counter_reset_and_normal_rate() {
        assert_eq!(saturating_rate(100, 90, 1.0), 0.0);
        assert_eq!(saturating_rate(10, 40, 2.0), 15.0);
    }

    #[test]
    fn prev_state_derive_initializes_and_then_computes_rates() {
        let mut state = PrevState::default();
        let first = MysqlSnapshot {
            queries_total: 100,
            bytes_received_total: 1_000,
            bytes_sent_total: 2_000,
            ..MysqlSnapshot::default()
        };
        let rates = state.derive(&first);
        assert_eq!(rates.queries_per_second, 0.0);
        assert!(state.last.is_some());

        state.last = Some((first, Instant::now() - Duration::from_secs(2)));
        let second = MysqlSnapshot {
            queries_total: 160,
            bytes_received_total: 1_600,
            bytes_sent_total: 2_900,
            ..MysqlSnapshot::default()
        };
        let rates = state.derive(&second);
        assert!(rates.queries_per_second > 25.0);
        assert!(rates.bytes_received_per_second > 250.0);
        assert!(rates.bytes_sent_per_second > 400.0);
    }

    #[test]
    fn prev_state_derive_resets_on_non_progressing_time() {
        let mut state = PrevState {
            last: Some((
                MysqlSnapshot::default(),
                Instant::now() + Duration::from_secs(1),
            )),
        };
        let rates = state.derive(&MysqlSnapshot::default());
        assert_eq!(rates.queries_per_second, 0.0);
        assert_eq!(rates.bytes_received_per_second, 0.0);
        assert_eq!(rates.bytes_sent_per_second, 0.0);
    }

    #[test]
    fn derive_rates_or_reset_resets_state_when_snapshot_unavailable() {
        let mut state = PrevState::default();
        let available = MysqlSnapshot {
            available: true,
            queries_total: 10,
            bytes_received_total: 20,
            bytes_sent_total: 30,
            ..MysqlSnapshot::default()
        };
        let _ = derive_rates_or_reset(&mut state, &available);
        assert!(state.last.is_some());

        let unavailable = MysqlSnapshot::default();
        let rates = derive_rates_or_reset(&mut state, &unavailable);
        assert_eq!(rates.queries_per_second, 0.0);
        assert_eq!(rates.bytes_received_per_second, 0.0);
        assert_eq!(rates.bytes_sent_per_second, 0.0);
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
    fn flush_helpers_cover_success_failure_and_sleep_paths() {
        let now = Instant::now();
        log_flush_result(now, true);
        log_flush_result(now, false);

        handle_flush_event(FlushEvent::Connected, None);
        handle_flush_event(FlushEvent::Reconnected, None);
        handle_flush_event(FlushEvent::None, None);
        handle_flush_event(FlushEvent::Reconnecting, Some(&"err"));
        handle_flush_event(FlushEvent::StillUnavailable, Some(&"err"));
        handle_flush_event(FlushEvent::Connected, Some(&"err"));

        let running = AtomicBool::new(false);
        sleep_until(
            Instant::now() + Duration::from_millis(5),
            &running,
            Duration::from_millis(1),
        );

        let running = AtomicBool::new(true);
        sleep_until(
            Instant::now() + Duration::from_millis(2),
            &running,
            Duration::from_millis(1),
        );

        maybe_sleep_until_next_poll(false, Instant::now(), Duration::from_millis(2), &running);
        maybe_sleep_until_next_poll(true, Instant::now(), Duration::from_secs(1), &running);
    }

    #[test]
    fn stop_handler_sets_running_false() {
        let running = Arc::new(AtomicBool::new(true));
        let stop = make_stop_handler(Arc::clone(&running));
        stop();
        assert!(!running.load(Ordering::SeqCst));
    }

    #[test]
    fn resolve_default_config_path_prefers_local_file_when_present() {
        let local = unique_temp_path("mysql-local.yaml");
        fs::write(&local, "service: {}\n").expect("write local");
        let chosen = resolve_default_config_path(local.to_string_lossy().as_ref(), "fallback.yaml");
        assert_eq!(chosen, local.to_string_lossy());
        fs::remove_file(&local).expect("cleanup local");

        let missing = unique_temp_path("mysql-missing.yaml");
        let chosen =
            resolve_default_config_path(missing.to_string_lossy().as_ref(), "fallback.yaml");
        assert_eq!(chosen, "fallback.yaml");
    }

    #[test]
    fn load_yaml_config_file_handles_missing_empty_and_valid_yaml() {
        let missing = unique_temp_path("mysql-missing-config.yaml");
        let missing_err = load_yaml_config_file(missing.to_string_lossy().as_ref()).unwrap_err();
        assert!(
            missing_err.to_string().contains("was not found"),
            "{missing_err}"
        );

        let empty = unique_temp_path("mysql-empty-config.yaml");
        fs::write(&empty, " \n").expect("write empty");
        let empty_err = load_yaml_config_file(empty.to_string_lossy().as_ref()).unwrap_err();
        assert!(empty_err.to_string().contains("is empty"), "{empty_err}");
        fs::remove_file(&empty).expect("cleanup empty");

        let valid = unique_temp_path("mysql-valid-config.yaml");
        fs::write(
            &valid,
            "service:\n  name: ojo-mysql\n  instance_id: mysql-1\ncollection:\n  poll_interval_secs: 2\nmysql:\n  executable: mysql\n",
        )
        .expect("write valid");
        let parsed = load_yaml_config_file(valid.to_string_lossy().as_ref());
        assert!(parsed.is_ok(), "{parsed:?}");
        fs::remove_file(&valid).expect("cleanup valid");
    }

    #[test]
    fn config_load_reads_yaml_and_applies_defaults() {
        let _guard = env_lock().lock().expect("env lock");
        let path = unique_temp_path("mysql-load.yaml");
        fs::write(
            &path,
            "service:\n  name: mysql-svc\n  instance_id: mysql-01\ncollection:\n  poll_interval_secs: 1\nmysql:\n  executable: mysql\n  host: '  '\n  user: root\n  password: secret\n  database: app\n",
        )
        .expect("write config");

        std::env::set_var("OJO_MYSQL_CONFIG", &path);
        std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
        std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");

        let cfg = Config::load().expect("load config");
        assert_eq!(cfg.service_name, "mysql-svc");
        assert_eq!(cfg.instance_id, "mysql-01");
        assert_eq!(cfg.poll_interval, Duration::from_secs(1));
        assert_eq!(cfg.otlp_endpoint, "http://127.0.0.1:4318/v1/metrics");
        assert_eq!(cfg.otlp_protocol, "http/protobuf");
        assert_eq!(cfg.metrics_include, vec!["system.mysql.".to_string()]);
        assert!(cfg.mysql.host.is_none());
        assert_eq!(cfg.mysql.user.as_deref(), Some("root"));
        assert_eq!(cfg.mysql.password.as_deref(), Some("secret"));
        assert_eq!(cfg.mysql.database.as_deref(), Some("app"));

        std::env::remove_var("OJO_MYSQL_CONFIG");
        fs::remove_file(&path).expect("cleanup config");
    }

    #[test]
    fn config_load_uses_otlp_env_fallback_when_export_section_missing() {
        let _guard = env_lock().lock().expect("env lock");
        let path = unique_temp_path("mysql-env-load.yaml");
        fs::write(
            &path,
            "service:\n  name: mysql-svc\n  instance_id: mysql-01\ncollection:\n  poll_interval_secs: 2\nmysql:\n  executable: mysql\n",
        )
        .expect("write config");

        std::env::set_var("OJO_MYSQL_CONFIG", &path);
        std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4317");
        std::env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc");

        let cfg = Config::load().expect("load config");
        assert_eq!(cfg.otlp_endpoint, "http://127.0.0.1:4317");
        assert_eq!(cfg.otlp_protocol, "grpc");

        std::env::remove_var("OJO_MYSQL_CONFIG");
        std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
        std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");
        fs::remove_file(&path).expect("cleanup config");
    }

    #[test]
    fn config_load_uses_repo_default_config_path_when_env_not_set() {
        let _guard = env_lock().lock().expect("env lock");
        std::env::remove_var("OJO_MYSQL_CONFIG");
        std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
        std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");

        let cfg = Config::load().expect("load default config");
        assert!(!cfg.service_name.is_empty());
    }

    #[test]
    fn config_load_from_args_supports_config_flag_and_once_aliases() {
        let _guard = env_lock().lock().expect("env lock");
        let path = unique_temp_path("mysql-args-load.yaml");
        fs::write(
            &path,
            "collection:\n  poll_interval_secs: 2\nmysql:\n  host: db\n",
        )
        .expect("write config");

        std::env::remove_var("OJO_MYSQL_CONFIG");
        std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
        std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");

        let args = vec![
            "ojo-mysql".to_string(),
            "--config".to_string(),
            path.to_string_lossy().to_string(),
            "--once".to_string(),
        ];
        let cfg = Config::load_from_args(&args).expect("load args config");
        assert_eq!(cfg.service_name, "ojo-mysql");
        assert_eq!(cfg.mysql.executable, "mysql");
        assert_eq!(cfg.mysql.host.as_deref(), Some("db"));
        assert!(cfg.once);

        std::env::set_var("OJO_RUN_ONCE", "yes");
        let cfg = Config::load_from_args(&args[..3]).expect("load with env yes");
        assert!(cfg.once);

        std::env::set_var("OJO_RUN_ONCE", "on");
        let cfg = Config::load_from_args(&args[..3]).expect("load with env on");
        assert!(cfg.once);

        std::env::remove_var("OJO_RUN_ONCE");
        fs::remove_file(&path).expect("cleanup config");
    }

    #[test]
    fn config_load_from_args_errors_for_missing_config_path() {
        let _guard = env_lock().lock().expect("env lock");
        std::env::remove_var("OJO_MYSQL_CONFIG");
        let missing = unique_temp_path("mysql-missing-from-args.yaml");
        let args = vec![
            "ojo-mysql".to_string(),
            "--config".to_string(),
            missing.to_string_lossy().to_string(),
        ];
        let err = Config::load_from_args(&args).unwrap_err();
        assert!(err.to_string().contains("was not found"), "{err}");
    }

    #[test]
    fn load_yaml_config_file_errors_for_directory_and_invalid_yaml() {
        let dir = unique_temp_path("mysql-config-dir");
        fs::create_dir_all(&dir).expect("mkdir");
        let dir_err = load_yaml_config_file(dir.to_string_lossy().as_ref()).unwrap_err();
        assert!(
            dir_err.to_string().contains("failed to read config file"),
            "{dir_err}"
        );
        fs::remove_dir_all(&dir).expect("cleanup dir");

        let invalid = unique_temp_path("mysql-invalid-config.yaml");
        fs::write(&invalid, "service: [\n").expect("write invalid");
        let parse_err = load_yaml_config_file(invalid.to_string_lossy().as_ref()).unwrap_err();
        assert!(
            parse_err.to_string().contains("failed to parse YAML"),
            "{parse_err}"
        );
        fs::remove_file(&invalid).expect("cleanup invalid");
    }

    #[test]
    fn record_snapshot_handles_unavailable_and_available_samples() {
        let meter = opentelemetry::global::meter("mysql-test-meter");
        let instruments = Instruments::new(&meter);
        let filter = PrefixFilter::new(vec!["system.mysql.".to_string()], vec![]);

        let unavailable = MysqlSnapshot::default();
        record_snapshot(&instruments, &filter, &unavailable, &MysqlRates::default());

        let available = MysqlSnapshot {
            available: true,
            up: true,
            connections: 3,
            threads_running: 2,
            queries_total: 100,
            slow_queries_total: 4,
            bytes_received_total: 500,
            bytes_sent_total: 800,
        };
        let rates = MysqlRates {
            queries_per_second: 10.0,
            bytes_received_per_second: 50.0,
            bytes_sent_per_second: 80.0,
        };
        record_snapshot(&instruments, &filter, &available, &rates);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_platform_collect_snapshot_wrapper_is_callable() {
        let cfg = super::MysqlConfig {
            executable: "/definitely/missing/mysql".to_string(),
            ..super::MysqlConfig::default()
        };
        let snap = super::platform::collect_snapshot(&cfg);
        assert!(!snap.available);
    }

    #[test]
    fn main_runs_once_with_temp_config() {
        let _guard = env_lock().lock().expect("env lock");
        let path = unique_temp_path("mysql-main-once.yaml");
        fs::write(
            &path,
            "service:\n  name: mysql-main-test\n  instance_id: mysql-main-01\ncollection:\n  poll_interval_secs: 1\nmysql:\n  executable: /definitely/missing/mysql\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
        )
        .expect("write config");

        std::env::set_var("OJO_MYSQL_CONFIG", &path);
        std::env::set_var("OJO_RUN_ONCE", "1");
        let result = super::run();
        assert!(result.is_ok(), "{result:?}");
        std::env::remove_var("OJO_MYSQL_CONFIG");
        std::env::remove_var("OJO_RUN_ONCE");
        fs::remove_file(&path).expect("cleanup config");
    }
}
