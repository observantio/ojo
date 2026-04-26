mod buffers;
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
mod tiered_queue;
#[cfg(target_os = "windows")]
mod windows;

use anyhow::Result;
use buffers::IntervalBuffer;
use config::Config;
use delta::PrevState;
use host_collectors::JsonArchiveWriter;
use metrics::{MetricFilter, ProcMetrics, ProcessLabelConfig, StreamCardinalityConfig};
use opentelemetry::global;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
#[cfg(not(coverage))]
use std::thread;
use std::time::Instant;
use tiered_queue::{BufferedInterval, TieredReplayQueue};
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

fn update_offline_buffer(offline_buffer: &mut IntervalBuffer<()>, flush_succeeded: bool) {
    if flush_succeeded {
        while offline_buffer.pop().is_some() {}
        #[cfg(not(coverage))]
        debug!(
            buffered_intervals = offline_buffer.len(),
            "offline buffer drained"
        );
        #[cfg(coverage)]
        let _ = offline_buffer.len();
        return;
    }

    let dropped_oldest = offline_buffer.push(());
    #[cfg(not(coverage))]
    debug!(
        buffered_intervals = offline_buffer.len(),
        "offline buffer appended"
    );
    #[cfg(coverage)]
    let _ = offline_buffer.len();

    if dropped_oldest {
        #[cfg(not(coverage))]
        warn!(
            dropped_intervals = offline_buffer.dropped_intervals(),
            "offline buffer is full; dropping oldest failed interval marker"
        );
        #[cfg(coverage)]
        let _ = offline_buffer.dropped_intervals();
    }
}

fn make_stop_handler(signal: Arc<AtomicBool>) -> impl Fn() + Send + 'static {
    move || {
        signal.store(false, Ordering::SeqCst);
    }
}

fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|arg| arg == flag)
}

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let cfg = Config::load()?;
    info!(
        host_type = cfg.host_type.as_str(),
        tiered_replay_enabled = cfg.tiered_replay.enabled,
        "configuration loaded"
    );
    cfg.apply_otel_env();
    let dump_snapshot = has_flag(&args, "--dump-snapshot");
    let run_once = dump_snapshot
        || std::env::var("OJO_RUN_ONCE")
            .ok()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

    if dump_snapshot {
        let snap = collector::collect_snapshot(cfg.include_process_metrics, cfg.host_type.clone())?;
        let snapshot_json =
            serde_json::to_string_pretty(&snap).expect("snapshot serialization should not fail");
        println!("{snapshot_json}");
        return Ok(());
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init()
        .ok();

    let provider = otel::init_meter_provider(&cfg)?;
    let meter = global::meter("procfs");
    let instruments = ProcMetrics::new_with_cardinality(
        meter,
        MetricFilter::new(cfg.metrics_include.clone(), cfg.metrics_exclude.clone()),
        ProcessLabelConfig {
            include_pid: cfg.process_include_pid_label,
            include_command: cfg.process_include_command_label,
            include_state: cfg.process_include_state_label,
        },
        StreamCardinalityConfig {
            process_max_series: cfg.metric_cardinality.process_max_series,
            cgroup_max_series: cfg.metric_cardinality.cgroup_max_series,
        },
    );

    let running = Arc::new(AtomicBool::new(true));
    if !run_once {
        ctrlc::set_handler(make_stop_handler(Arc::clone(&running)))?;
    }

    let mut prev = PrevState::default();
    let mut replay_prev = PrevState::default();
    let mut archive = JsonArchiveWriter::from_config(&cfg.archive);
    archive.set_default_identity(&cfg.service_name, &cfg.instance_id);
    let mut tiered_replay = TieredReplayQueue::from_config(&cfg.tiered_replay)?;
    let mut export_state = ExportState::Pending;
    let mut offline_buffer = IntervalBuffer::new(cfg.offline_buffer_intervals);

    while running.load(Ordering::SeqCst) {
        let started_at = Instant::now();
        debug!("poll tick start");

        let snap = collector::collect_snapshot(cfg.include_process_metrics, cfg.host_type.clone())?;
        let raw = serde_json::json!(snap);
        archive.write_json_line(&raw);
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

        let mut flush_result = provider.force_flush();

        if flush_result.is_ok() && tiered_replay.has_pending() {
            match tiered_replay.drain_batch(cfg.tiered_replay.max_replay_per_tick) {
                Ok(batch) if !batch.is_empty() => {
                    for interval in &batch {
                        let replay_derived =
                            replay_prev.derive(&interval.snapshot, cfg.poll_interval);
                        instruments.record(
                            &interval.snapshot,
                            &replay_derived,
                            cfg.include_process_metrics,
                        );
                    }

                    let replay_flush = provider.force_flush();
                    if replay_flush.is_err() {
                        if let Err(err) = tiered_replay.requeue_front(batch) {
                            warn!(error = %err, "tiered replay requeue failed");
                        }
                        flush_result = replay_flush;
                    }
                }
                Ok(_) => {}
                Err(err) => warn!(error = %err, "tiered replay drain failed"),
            }
        }

        if flush_result.is_err() && tiered_replay.is_enabled() {
            if let Err(err) = tiered_replay.push(BufferedInterval {
                snapshot: snap.clone(),
            }) {
                warn!(error = %err, "tiered replay enqueue failed");
            }
        }

        log_flush_result(started_at, flush_result.is_ok());

        update_offline_buffer(&mut offline_buffer, flush_result.is_ok());

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

        let should_break_for_test = std::env::var("OJO_TEST_MAX_ITERATIONS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .is_some_and(|max| max <= 1);

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
            #[cfg(coverage)]
            let _ = sleep_for;
            #[cfg(not(coverage))]
            thread::sleep(sleep_for);
        }

        if should_break_for_test {
            running.store(false, Ordering::SeqCst);
        }
    }

    let _ = provider.shutdown();
    Ok(())
}

#[cfg(test)]
#[path = "tests/main_tests.rs"]
mod tests;
