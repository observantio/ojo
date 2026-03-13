mod collector;
mod config;
mod delta;
mod view;
mod metrics;
mod model;
mod otel;

use anyhow::Result;
use config::Config;
use delta::PrevState;
use view::{build_registry, list_metrics, render_metric_header, render_metric_row};
use metrics::{MetricFilter, ProcMetrics};
use opentelemetry::global;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::Instant;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExportState {
    Pending,
    Connected,
    Reconnecting,
}

fn main() -> Result<()> {
    let cfg = Config::load()?;
    cfg.apply_otel_env();

    if cfg.list_metrics {
        let snap = collector::collect_snapshot(cfg.include_process_metrics)?;
        let mut prev = PrevState::default();
        let derived = prev.derive(&snap, cfg.poll_interval);
        list_metrics(&snap, &derived);
        return Ok(());
    }

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            if cfg.metric_query.is_some() {
                EnvFilter::new("warn")
            } else {
                EnvFilter::new("info")
            }
        }))
        .init();

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
    let mut printed_metric_header = false;
    let mut export_state = ExportState::Pending;

    while running.load(Ordering::SeqCst) {
        let started_at = Instant::now();

        let snap = collector::collect_snapshot(cfg.include_process_metrics)?;
        let derived = prev.derive(&snap, cfg.poll_interval);
        instruments.record(&snap, &derived, cfg.include_process_metrics);

        if let Some(metric) = cfg.metric_query.as_deref() {
            let registry = build_registry(&snap, &derived);
            let view = registry
                .get(metric)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("unknown metric '{metric}'. Use --metrics to list available names."))?;
            if !printed_metric_header {
                render_metric_header(&view);
                printed_metric_header = true;
            }
            render_metric_row(&view);
        }

        match provider.force_flush() {
            Ok(()) => {
                match export_state {
                    ExportState::Pending => info!("Ojo has successfully connected ......."),
                    ExportState::Reconnecting => info!("Ojo has reconnecte ........"),
                    ExportState::Connected => {}
                }
                export_state = ExportState::Connected;
            }
            Err(err) => {
                match export_state {
                    ExportState::Connected => warn!(error = %err, "Trying to reconnect ..."),
                    ExportState::Pending | ExportState::Reconnecting => {
                        warn!(error = %err, "We could not reconnect yet ...")
                    }
                }
                export_state = ExportState::Reconnecting;
            }
        }

        let elapsed = started_at.elapsed();
        if elapsed < cfg.poll_interval && running.load(Ordering::SeqCst) {
            thread::sleep(cfg.poll_interval - elapsed);
        }
    }

    if let Err(err) = provider.shutdown() {
        warn!(error = %err, "Ojo provider shutdown encountered an error");
    }
    Ok(())
}
