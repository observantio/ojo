use crate::{NfsClientConfig, NfsClientSnapshot};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tracing::warn;

const CMD_TIMEOUT: Duration = Duration::from_secs(15);

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
    let executable = cfg
        .executable
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or("powershell");
    let script = "$a=(Get-Counter '\\NFS Client(*)\\RPC Calls').CounterSamples | Select-Object -ExpandProperty CookedValue | Measure-Object -Sum; $b=(Get-Counter '\\NFS Client(*)\\RPC Retransmissions').CounterSamples | Select-Object -ExpandProperty CookedValue | Measure-Object -Sum; Write-Output ([math]::Round($a.Sum)); Write-Output ([math]::Round($b.Sum));";
    let mut cmd = Command::new(executable);
    cmd.args(["-NoProfile", "-Command", script]);
    let output = run_with_timeout(cmd, CMD_TIMEOUT)?;
    if !output.status.success() {
        warn!(stderr = %String::from_utf8_lossy(&output.stderr), "powershell nfs command failed");
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let text = text.trim_start_matches('\u{FEFF}');
    let values: Vec<u64> = text
        .lines()
        .map(str::trim)
        .filter_map(|line| line.parse::<u64>().ok())
        .collect();
    if values.len() < 2 {
        return None;
    }
    Some((values[0], values[1]))
}

fn run_with_timeout(mut cmd: Command, timeout: Duration) -> Option<std::process::Output> {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "failed to spawn command");
            return None;
        }
    };
    let start = Instant::now();
    loop {
        match child.try_wait() {
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
