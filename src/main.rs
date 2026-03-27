mod catalog;
mod collector;
mod config;
mod delta;
#[cfg(any(target_os = "linux", target_os = "android"))]
mod linux;
mod metrics;
mod model;
mod otel;
#[cfg(target_os = "solaris")]
mod solaris;
#[cfg(target_os = "windows")]
mod windows;

use anyhow::Result;
use config::Config;
use delta::PrevState;
use metrics::{MetricFilter, ProcMetrics, ProcessLabelConfig};
use opentelemetry::global;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::Instant;
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

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

fn compute_sleep_duration(
    elapsed: std::time::Duration,
    poll_interval: std::time::Duration,
    running: bool,
) -> Option<std::time::Duration> {
    if running && elapsed < poll_interval {
        Some(poll_interval - elapsed)
    } else {
        None
    }
}

fn main() -> Result<()> {
    let cfg = Config::load()?;
    cfg.apply_otel_env();
    let run_once = std::env::var("OJO_RUN_ONCE")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init()
        .ok();

    let provider = otel::init_meter_provider(&cfg)?;
    let meter = global::meter("procfs");
    let instruments = ProcMetrics::new(
        meter,
        MetricFilter::new(cfg.metrics_include.clone(), cfg.metrics_exclude.clone()),
        ProcessLabelConfig {
            include_pid: cfg.process_include_pid_label,
            include_command: cfg.process_include_command_label,
            include_state: cfg.process_include_state_label,
        },
    );

    let running = Arc::new(AtomicBool::new(true));
    if !run_once {
        let signal = Arc::clone(&running);
        ctrlc::set_handler(move || {
            signal.store(false, Ordering::SeqCst);
        })?;
    }

    let mut prev = PrevState::default();
    let mut export_state = ExportState::Pending;

    while running.load(Ordering::SeqCst) {
        let started_at = Instant::now();
        debug!("poll tick start");

        let snap = collector::collect_snapshot(cfg.include_process_metrics)?;
        debug!(
            elapsed_ms = started_at.elapsed().as_millis(),
            process_count = snap.system.process_count,
            disks = snap.disks.len(),
            net_ifaces = snap.net.len(),
            procs = snap.processes.len(),
            "snapshot collected"
        );

        let derived = prev.derive(&snap, cfg.poll_interval);
        instruments.record(&snap, &derived, cfg.include_process_metrics);
        debug!(
            elapsed_ms = started_at.elapsed().as_millis(),
            "metrics recorded"
        );

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
        if run_once {
            break;
        }

        debug!(
            elapsed_ms = started_at.elapsed().as_millis(),
            "poll tick done"
        );

        let elapsed = started_at.elapsed();
        if let Some(sleep_for) =
            compute_sleep_duration(elapsed, cfg.poll_interval, running.load(Ordering::SeqCst))
        {
            thread::sleep(sleep_for);
        }
    }

    if let Err(err) = provider.shutdown() {
        warn!(error = %err, "Ojo provider shutdown encountered an error");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{advance_export_state, compute_sleep_duration, ExportState, FlushEvent};
    use std::fs;
    use std::sync::{Mutex, OnceLock};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn unique_temp_path(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("ojo-main-{name}-{}-{nanos}", std::process::id()))
    }

    #[test]
    fn export_state_transitions_cover_success_and_failure_paths() {
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
    fn compute_sleep_duration_only_sleeps_when_running_and_before_deadline() {
        assert_eq!(
            compute_sleep_duration(Duration::from_millis(100), Duration::from_secs(1), true),
            Some(Duration::from_millis(900))
        );
        assert_eq!(
            compute_sleep_duration(Duration::from_secs(1), Duration::from_secs(1), true),
            None
        );
        assert_eq!(
            compute_sleep_duration(Duration::from_secs(2), Duration::from_secs(1), true),
            None
        );
        assert_eq!(
            compute_sleep_duration(Duration::from_millis(100), Duration::from_secs(1), false),
            None
        );
    }

    #[test]
    fn main_returns_error_when_config_missing() {
        let _guard = env_lock().lock().expect("env lock");
        std::env::set_var("PROC_OTEL_CONFIG", "/definitely/missing/ojo.yaml");
        let result = super::main();
        assert!(result.is_err());
        std::env::remove_var("PROC_OTEL_CONFIG");
    }

    #[test]
    fn main_runs_once_with_valid_config() {
        let _guard = env_lock().lock().expect("env lock");
        let path = unique_temp_path("valid-config.yaml");
        fs::write(
            &path,
            "service:\n  name: ojo-test\n  instance_id: ojo-test-01\ncollection:\n  poll_interval_secs: 1\n  include_process_metrics: false\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
        )
        .expect("write config");

        std::env::set_var("PROC_OTEL_CONFIG", &path);
        std::env::set_var("OJO_RUN_ONCE", "1");

        let result = super::main();
        assert!(result.is_ok(), "{result:?}");

        std::env::remove_var("PROC_OTEL_CONFIG");
        std::env::remove_var("OJO_RUN_ONCE");
        fs::remove_file(path).expect("cleanup config");
    }

    #[test]
    fn main_runs_once_with_process_metrics_enabled() {
        let _guard = env_lock().lock().expect("env lock");
        let path = unique_temp_path("valid-config-procs.yaml");
        fs::write(
            &path,
            "service:\n  name: ojo-test-procs\n  instance_id: ojo-test-procs-01\ncollection:\n  poll_interval_secs: 1\n  include_process_metrics: true\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
        )
        .expect("write config");

        std::env::set_var("PROC_OTEL_CONFIG", &path);
        std::env::set_var("OJO_RUN_ONCE", "1");

        let result = super::main();
        assert!(result.is_ok(), "{result:?}");

        std::env::remove_var("PROC_OTEL_CONFIG");
        std::env::remove_var("OJO_RUN_ONCE");
        fs::remove_file(path).expect("cleanup config");
    }
}
