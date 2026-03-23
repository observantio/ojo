use crate::{NfsClientConfig, NfsClientSnapshot};
use std::fs;
use std::path::Path;
use std::process::Command;

pub(crate) fn collect_snapshot(cfg: &NfsClientConfig) -> NfsClientSnapshot {
    let mounts = collect_mounts_from_proc_mounts().unwrap_or(0);
    if let Some((calls, retrans, auth_refreshes)) = collect_rpc_stats_from_proc() {
        return NfsClientSnapshot {
            available: true,
            mounts,
            rpc_calls_total: calls,
            rpc_retransmissions_total: retrans,
            rpc_auth_refreshes_total: auth_refreshes,
        };
    }
    if let Some((calls, retrans, auth_refreshes)) = collect_rpc_stats_from_nfsstat(cfg) {
        return NfsClientSnapshot {
            available: true,
            mounts,
            rpc_calls_total: calls,
            rpc_retransmissions_total: retrans,
            rpc_auth_refreshes_total: auth_refreshes,
        };
    }
    NfsClientSnapshot::default()
}

fn collect_mounts_from_proc_mounts() -> Option<u64> {
    let contents = fs::read_to_string("/proc/mounts").ok()?;
    let count = contents
        .lines()
        .filter_map(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            fields.get(2).copied()
        })
        .filter(|fstype| *fstype == "nfs" || *fstype == "nfs4")
        .count() as u64;
    Some(count)
}

fn collect_rpc_stats_from_proc() -> Option<(u64, u64, u64)> {
    let path = Path::new("/proc/net/rpc/nfs");
    if !path.exists() {
        return None;
    }
    let contents = fs::read_to_string(path).ok()?;
    for line in contents.lines() {
        if !line.starts_with("rpc ") {
            continue;
        }
        let values = line.split_whitespace().skip(1).collect::<Vec<_>>();
        if values.len() < 3 {
            continue;
        }
        let calls = values[0].parse::<u64>().ok().unwrap_or(0);
        let retrans = values[1].parse::<u64>().ok().unwrap_or(0);
        let auth_refreshes = values[2].parse::<u64>().ok().unwrap_or(0);
        return Some((calls, retrans, auth_refreshes));
    }
    None
}

fn collect_rpc_stats_from_nfsstat(cfg: &NfsClientConfig) -> Option<(u64, u64, u64)> {
    let executable = cfg.executable.as_deref().unwrap_or("nfsstat");
    let output = Command::new(executable).args(["-c"]).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    for line in text.lines() {
        let lowered = line.to_ascii_lowercase();
        if !lowered.contains("calls") || !lowered.contains("retrans") {
            continue;
        }
        let values = line
            .split_whitespace()
            .filter_map(|token| token.parse::<u64>().ok())
            .collect::<Vec<_>>();
        if values.len() < 2 {
            continue;
        }
        let calls = values[0];
        let retrans = values[1];
        let auth_refreshes = values.get(2).copied().unwrap_or(0);
        return Some((calls, retrans, auth_refreshes));
    }
    None
}
