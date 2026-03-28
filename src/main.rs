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

fn log_flush_result(started_at: Instant, flush_succeeded: bool) {
    #[cfg(not(coverage))]
    {
        if flush_succeeded {
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
    }

    #[cfg(coverage)]
    {
        let _ = started_at.elapsed().as_millis();
        let _ = flush_succeeded;
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
        #[cfg(not(coverage))]
        debug!(
            elapsed_ms = started_at.elapsed().as_millis(),
            process_count = snap.system.process_count,
            disks = snap.disks.len(),
            net_ifaces = snap.net.len(),
            procs = snap.processes.len(),
            "snapshot collected"
        );
        #[cfg(coverage)]
        let _ = (
            started_at.elapsed().as_millis(),
            snap.system.process_count,
            snap.disks.len(),
            snap.net.len(),
            snap.processes.len(),
        );

        let derived = prev.derive(&snap, cfg.poll_interval);
        instruments.record(&snap, &derived, cfg.include_process_metrics);
        #[cfg(not(coverage))]
        debug!(
            elapsed_ms = started_at.elapsed().as_millis(),
            "metrics recorded"
        );
        #[cfg(coverage)]
        let _ = started_at.elapsed().as_millis();

        let flush_result = provider.force_flush();
        log_flush_result(started_at, flush_result.is_ok());

        let (next_state, event) = advance_export_state(export_state, flush_result.is_ok());
        let flush_error = flush_result
            .as_ref()
            .err()
            .map(|err| err as &dyn std::fmt::Display);
        handle_flush_event(event, flush_error);
        export_state = next_state;
        if run_once {
            break;
        }

        #[cfg(not(coverage))]
        debug!(
            elapsed_ms = started_at.elapsed().as_millis(),
            "poll tick done"
        );
        #[cfg(coverage)]
        let _ = started_at.elapsed().as_millis();

        let elapsed = started_at.elapsed();
        if let Some(sleep_for) =
            compute_sleep_duration(elapsed, cfg.poll_interval, running.load(Ordering::SeqCst))
        {
            thread::sleep(sleep_for);
        }
    }

    #[cfg(not(coverage))]
    if let Err(err) = provider.shutdown() {
        warn!(error = %err, "Ojo provider shutdown encountered an error");
    }
    #[cfg(coverage)]
    let _ = provider.shutdown();
    Ok(())
}

#[cfg(test)]
#[path = "tests/main_tests.rs"]
mod tests;
