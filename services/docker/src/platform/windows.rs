use crate::{parse_pair_bytes, parse_percent, DockerSample, DockerSnapshot};
use serde_json::Value;
use std::collections::BTreeMap;
use std::process::Command;

#[derive(Default)]
struct DockerSummary {
    available: bool,
    total: u64,
    running: u64,
    stopped: u64,
    by_id: BTreeMap<String, (String, String, String)>,
}

pub(crate) fn collect_snapshot() -> DockerSnapshot {
    let summary = docker_ps_summary();
    let stats = docker_stats();

    let mut samples = Vec::new();
    for stat in stats {
        let mut sample = stat;
        if let Some((name, image, state)) = resolve_summary_entry(&summary.by_id, &sample.id) {
            if sample.name.trim().is_empty() {
                sample.name = name.to_string();
            }
            if sample.image.trim().is_empty() {
                sample.image = image.to_string();
            }
            if sample.state.trim().is_empty() {
                sample.state = state.to_string();
            }
        }
        sample.name = sample.name.trim_start_matches('/').to_string();
        samples.push(sample);
    }

    DockerSnapshot {
        available: summary.available || !samples.is_empty(),
        total: summary.total,
        running: summary.running,
        stopped: summary.stopped,
        samples,
    }
}

fn resolve_summary_entry<'a>(
    by_id: &'a BTreeMap<String, (String, String, String)>,
    id: &str,
) -> Option<(&'a str, &'a str, &'a str)> {
    if let Some((name, image, state)) = by_id.get(id) {
        return Some((name.as_str(), image.as_str(), state.as_str()));
    }
    by_id.iter().find_map(|(candidate_id, tuple)| {
        if candidate_id.starts_with(id) || id.starts_with(candidate_id) {
            return Some((tuple.0.as_str(), tuple.1.as_str(), tuple.2.as_str()));
        }
        None
    })
}

fn docker_ps_summary() -> DockerSummary {
    let output = Command::new("docker")
        .args(["ps", "-a", "--format", "{{json .}}"])
        .output();

    let Ok(output) = output else {
        return DockerSummary::default();
    };
    if !output.status.success() {
        return DockerSummary::default();
    }
    let mut summary = DockerSummary {
        available: true,
        ..DockerSummary::default()
    };
    let text = String::from_utf8_lossy(&output.stdout);
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let id = value
            .get("ID")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let name = value
            .get("Names")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let image = value
            .get("Image")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let state = value
            .get("State")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();

        summary.total += 1;
        if state.eq_ignore_ascii_case("running") {
            summary.running += 1;
        } else {
            summary.stopped += 1;
        }
        if !id.is_empty() {
            summary.by_id.insert(id, (name, image, state));
        }
    }
    summary
}

fn docker_stats() -> Vec<DockerSample> {
    let output = Command::new("docker")
        .args(["stats", "--no-stream", "--format", "{{json .}}"])
        .output();

    let Ok(output) = output else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let mut rows = Vec::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let id = value
            .get("ID")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let mut sample = DockerSample {
            id,
            name: value
                .get("Name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            ..DockerSample::default()
        };
        sample.cpu_ratio = parse_percent(
            value
                .get("CPUPerc")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        );
        let (mem_usage, mem_limit) = parse_pair_bytes(
            value
                .get("MemUsage")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        );
        sample.mem_usage_bytes = mem_usage;
        sample.mem_limit_bytes = mem_limit;
        let (net_rx, net_tx) = parse_pair_bytes(
            value
                .get("NetIO")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        );
        sample.net_rx_bytes = net_rx;
        sample.net_tx_bytes = net_tx;
        let (blk_read, blk_write) = parse_pair_bytes(
            value
                .get("BlockIO")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        );
        sample.block_read_bytes = blk_read;
        sample.block_write_bytes = blk_write;
        rows.push(sample);
    }
    rows
}
