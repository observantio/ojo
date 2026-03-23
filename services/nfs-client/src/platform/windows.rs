use crate::{NfsClientConfig, NfsClientSnapshot};
use std::process::Command;

pub(crate) fn collect_snapshot(cfg: &NfsClientConfig) -> NfsClientSnapshot {
    if let Some((calls, retrans)) = collect_counters(cfg) {
        return NfsClientSnapshot {
            available: true,
            mounts: 0,
            rpc_calls_total: calls,
            rpc_retransmissions_total: retrans,
            rpc_auth_refreshes_total: 0,
        };
    }
    NfsClientSnapshot::default()
}

fn collect_counters(cfg: &NfsClientConfig) -> Option<(u64, u64)> {
    let executable = cfg.executable.as_deref().unwrap_or("powershell");
    let script = "$a=(Get-Counter '\\NFS Client(*)\\RPC Calls').CounterSamples | Select-Object -ExpandProperty CookedValue | Measure-Object -Sum; $b=(Get-Counter '\\NFS Client(*)\\RPC Retransmissions').CounterSamples | Select-Object -ExpandProperty CookedValue | Measure-Object -Sum; Write-Output ([math]::Round($a.Sum)); Write-Output ([math]::Round($b.Sum));";
    let output = Command::new(executable)
        .args(["-NoProfile", "-Command", script])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let values = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter_map(|line| line.parse::<u64>().ok())
        .collect::<Vec<_>>();
    if values.len() < 2 {
        return None;
    }
    Some((values[0], values[1]))
}
