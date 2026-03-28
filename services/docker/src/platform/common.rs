use crate::{DockerSample, DockerSnapshot};
use serde_json::Value;
use std::collections::BTreeMap;
use std::process::Child;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tracing::warn;

const CMD_TIMEOUT: Duration = Duration::from_secs(15);

pub(crate) fn collect_snapshot() -> DockerSnapshot {
    let summary = docker_ps_summary();
    let stats = docker_stats();
    let mut snapshot = DockerSnapshot {
        available: summary.available,
        total: summary.total,
        running: summary.running,
        stopped: summary.stopped,
        samples: Vec::new(),
    };
    if !snapshot.available {
        return snapshot;
    }
    for sample in &stats {
        let (name, image, state) =
            resolve_summary_entry(&summary.by_id, &sample.id).unwrap_or(("", "", ""));
        let enriched = enrich_sample(sample, name, image, state);
        snapshot.samples.push(enriched);
    }
    snapshot
}

fn enrich_sample(sample: &DockerSample, name: &str, image: &str, state: &str) -> DockerSample {
    let mut enriched = sample.clone();
    if enriched.name.is_empty() {
        enriched.name = name.trim_start_matches('/').to_string();
    } else {
        enriched.name = enriched.name.trim_start_matches('/').to_string();
    }
    if enriched.image.is_empty() {
        enriched.image = image.to_string();
    }
    if enriched.state.is_empty() {
        enriched.state = state.to_string();
    }
    enriched
}

fn resolve_summary_entry<'a>(
    by_id: &'a BTreeMap<String, (String, String, String)>,
    id: &str,
) -> Option<(&'a str, &'a str, &'a str)> {
    if let Some((name, image, state)) = by_id.get(id) {
        return Some((name.as_str(), image.as_str(), state.as_str()));
    }
    if id.len() >= 6 {
        return by_id.iter().find_map(|(candidate_id, tuple)| {
            if candidate_id.starts_with(id) || id.starts_with(candidate_id) {
                return Some((tuple.0.as_str(), tuple.1.as_str(), tuple.2.as_str()));
            }
            None
        });
    }
    None
}

#[derive(Clone, Debug, Default)]
struct DockerSummary {
    available: bool,
    total: u64,
    running: u64,
    stopped: u64,
    by_id: BTreeMap<String, (String, String, String)>,
}

fn docker_ps_summary() -> DockerSummary {
    let mut cmd = Command::new("docker");
    cmd.args(["ps", "-a", "--no-trunc", "--format", "{{json .}}"]);
    let maybe_output = run_with_timeout(cmd, CMD_TIMEOUT);
    let output = match maybe_output {
        Some(output) => output,
        None => return DockerSummary::default(),
    };
    if !output.status.success() {
        warn!(stderr = %String::from_utf8_lossy(&output.stderr), "docker ps failed");
        return DockerSummary::default();
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let mut summary = DockerSummary {
        available: true,
        ..DockerSummary::default()
    };
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let value = match serde_json::from_str::<Value>(line) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let id = value.get("ID").and_then(Value::as_str).unwrap_or_default();
        let name = value
            .get("Names")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim_start_matches('/')
            .to_string();
        let image = value
            .get("Image")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let state = value
            .get("State")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        summary.total += 1;
        if state.eq_ignore_ascii_case("running") {
            summary.running += 1;
        } else {
            summary.stopped += 1;
        }
        if !id.is_empty() {
            summary.by_id.insert(id.to_string(), (name, image, state));
        }
    }
    summary
}

fn docker_stats() -> Vec<DockerSample> {
    let mut cmd = Command::new("docker");
    cmd.args(["stats", "--no-stream", "--format", "{{json .}}"]);
    let maybe_output = run_with_timeout(cmd, CMD_TIMEOUT);
    let output = match maybe_output {
        Some(output) => output,
        None => return Vec::new(),
    };
    if !output.status.success() {
        warn!(stderr = %String::from_utf8_lossy(&output.stderr), "docker stats failed");
        return Vec::new();
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let mut samples = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let value = match serde_json::from_str::<Value>(line) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let id = value
            .get("ID")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let name = value
            .get("Name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim_start_matches('/')
            .to_string();
        let cpu_text = value
            .get("CPUPerc")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let mem_text = value
            .get("MemUsage")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let net_text = value
            .get("NetIO")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let block_text = value
            .get("BlockIO")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let (mem_usage, mem_limit) = crate::parse_pair_bytes(mem_text);
        let (net_rx, net_tx) = crate::parse_pair_bytes(net_text);
        let (block_read, block_write) = crate::parse_pair_bytes(block_text);
        samples.push(DockerSample {
            id,
            name,
            image: String::new(),
            state: String::new(),
            cpu_ratio: crate::parse_percent(cpu_text),
            mem_usage_bytes: mem_usage,
            mem_limit_bytes: mem_limit,
            net_rx_bytes: net_rx,
            net_tx_bytes: net_tx,
            block_read_bytes: block_read,
            block_write_bytes: block_write,
        });
    }
    samples
}

fn run_with_timeout(cmd: Command, timeout: Duration) -> Option<std::process::Output> {
    run_with_timeout_using_waiter(cmd, timeout, wait_for_child)
}

fn wait_for_child(child: &mut Child) -> std::io::Result<Option<std::process::ExitStatus>> {
    child.try_wait()
}

fn run_with_timeout_using_waiter<W>(
    mut cmd: Command,
    timeout: Duration,
    mut waiter: W,
) -> Option<std::process::Output>
where
    W: FnMut(&mut Child) -> std::io::Result<Option<std::process::ExitStatus>>,
{
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let child_result = cmd.spawn();
    let mut child = match child_result {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "failed to spawn command");
            return None;
        }
    };
    let start = Instant::now();
    loop {
        match waiter(&mut child) {
            Ok(Some(_)) => return child.wait_with_output().ok(),
            Ok(None) => {
                if start.elapsed() > timeout {
                    let _ = child.kill();
                    let _ = child.wait();
                    warn!("command timed out after {:?}", timeout);
                    return None;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => {
                warn!(error = %e, "error waiting for command");
                let _ = child.kill();
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests;
