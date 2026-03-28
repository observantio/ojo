use crate::SystemdSnapshot;
use std::collections::BTreeMap;
use std::process::Output;

pub(crate) fn collect_snapshot() -> SystemdSnapshot {
    let manager = super::common::run_command_with_timeout(
        "systemctl",
        &["show", "--property=NJobs,NFailedUnits", "--value"],
    );
    let units = super::common::run_command_with_timeout(
        "systemctl",
        &[
            "list-units",
            "--type=service",
            "--all",
            "--no-legend",
            "--no-pager",
        ],
    );

    snapshot_from_outputs(manager, units)
}

fn snapshot_from_outputs(manager: Option<Output>, units: Option<Output>) -> SystemdSnapshot {
    let mut snap = SystemdSnapshot::default();
    let Some(units_output) = units else {
        return snap;
    };
    if !units_output.status.success() {
        return snap;
    }

    let units_text = String::from_utf8_lossy(&units_output.stdout);
    let unit_counts = parse_units_table(&units_text);
    if unit_counts.is_empty() {
        return snap;
    }

    let manager_values = match manager {
        Some(output) if output.status.success() => {
            let text = String::from_utf8_lossy(&output.stdout);
            parse_manager_values(&text)
        }
        _ => BTreeMap::new(),
    };

    snap.available = true;
    snap.units_total = unit_counts.values().copied().sum();
    snap.units_active = *unit_counts.get("active").unwrap_or(&0);
    snap.units_inactive = *unit_counts.get("inactive").unwrap_or(&0);
    snap.units_failed = *unit_counts.get("failed").unwrap_or(&0);
    snap.units_activating = *unit_counts.get("activating").unwrap_or(&0);
    snap.units_deactivating = *unit_counts.get("deactivating").unwrap_or(&0);
    snap.jobs_queued = *manager_values.get("njobs").unwrap_or(&0);
    snap.jobs_running = snap.jobs_queued;
    snap.failed_units_reported = *manager_values.get("nfailedunits").unwrap_or(&0);
    snap
}

fn parse_manager_values(text: &str) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    let lines = text
        .lines()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .collect::<Vec<_>>();
    if let Some(first) = lines.first() {
        if let Ok(v) = (*first).parse::<u64>() {
            out.insert("njobs".to_string(), v);
        }
    }
    if let Some(second) = lines.get(1) {
        if let Ok(v) = (*second).parse::<u64>() {
            out.insert("nfailedunits".to_string(), v);
        }
    }
    out
}

fn parse_units_table(text: &str) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    for line in text.lines() {
        let cols = line.split_whitespace().collect::<Vec<_>>();
        if cols.len() < 4 {
            continue;
        }
        let active = cols[2].to_ascii_lowercase();
        *out.entry(active).or_insert(0) += 1;
    }
    out
}

#[cfg(test)]
#[path = "../tests/platform_linux_tests.rs"]
mod tests;
