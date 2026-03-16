mod catalog;
mod collector;
mod config;
mod delta;
#[cfg(not(target_os = "windows"))]
mod linuxcollect;
mod metrics;
mod model;
mod otel;
#[cfg(target_os = "windows")]
mod wincollect;

use anyhow::Result;
use config::Config;
use delta::PrevState;
use metrics::{MetricFilter, ProcMetrics};
use opentelemetry::global;
use serde_json::to_string_pretty;
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

fn main() -> Result<()> {
    let cfg = Config::load()?;

    if cfg.dump_snapshot {
        let snap = collector::collect_snapshot(cfg.include_process_metrics)?;
        println!("{}", to_string_pretty(&snap)?);
        return Ok(());
    }

    cfg.apply_otel_env();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("ojo=debug,opentelemetry=info")),
        )
        .init();

    info!(
        endpoint = cfg.otlp_endpoint.as_str(),
        protocol = cfg.otlp_protocol.as_str(),
        "OTLP export configured"
    );

    let provider = otel::init_meter_provider(&cfg)?;
    let meter = global::meter("procfs");
    let instruments = ProcMetrics::new(
        meter,
        MetricFilter::new(cfg.metrics_include.clone(), cfg.metrics_exclude.clone()),
    );

    let running = Arc::new(AtomicBool::new(true));
    let signal = Arc::clone(&running);
    ctrlc::set_handler(move || {
        signal.store(false, Ordering::SeqCst);
    })?;

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
                let err_msg = err.to_string();

                debug!(
                    elapsed_ms = started_at.elapsed().as_millis(),
                    "force_flush err"
                );

                match export_state {
                    ExportState::Connected => {
                        warn!(err = err_msg.as_str(), "Exporter flush failed; reconnecting");
                    }
                    ExportState::Pending | ExportState::Reconnecting => {
                        warn!(err = err_msg.as_str(), "Exporter still unavailable");
                    }
                }

                export_state = ExportState::Reconnecting;
            }
        }

        debug!(elapsed_ms = started_at.elapsed().as_millis(), "poll tick done");

        let elapsed = started_at.elapsed();
        if elapsed < cfg.poll_interval && running.load(Ordering::SeqCst) {
            thread::sleep(cfg.poll_interval - elapsed);
        }
    }

    if let Err(err) = provider.shutdown() {
        let err_msg = err.to_string();
        warn!(
            err = err_msg.as_str(),
            "Ojo provider shutdown encountered an error"
        );
    }

    Ok(())
}
