use crate::{NfsClientConfig, NfsClientSnapshot};
use std::fs;
use std::path::Path;
use std::process::Child;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tracing::warn;

const CMD_TIMEOUT: Duration = Duration::from_secs(15);

pub(crate) fn collect_snapshot(cfg: &NfsClientConfig) -> NfsClientSnapshot {
    let mounts = collect_mounts_from_proc_mounts().unwrap_or(0);
    snapshot_from_sources(
        mounts,
        collect_rpc_stats_from_proc(),
        collect_rpc_stats_from_nfsstat(cfg),
    )
}

fn snapshot_from_sources(
    mounts: u64,
    proc_stats: Option<(u64, u64, u64)>,
    nfsstat_stats: Option<(u64, u64, u64)>,
) -> NfsClientSnapshot {
    if let Some((calls, retrans, auth_refreshes)) = proc_stats {
        return NfsClientSnapshot {
            available: true,
            mounts,
            rpc_calls_total: calls,
            rpc_retransmissions_total: retrans,
            rpc_auth_refreshes_total: auth_refreshes,
        };
    }
    if let Some((calls, retrans, auth_refreshes)) = nfsstat_stats {
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
    collect_mounts_from_path(Path::new("/proc/mounts"))
}

fn collect_mounts_from_path(path: &Path) -> Option<u64> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(_) => return None,
    };
    Some(parse_nfs_mount_count(&contents))
}

fn collect_rpc_stats_from_proc() -> Option<(u64, u64, u64)> {
    collect_rpc_stats_from_proc_path(Path::new("/proc/net/rpc/nfs"))
}

fn collect_rpc_stats_from_proc_path(path: &Path) -> Option<(u64, u64, u64)> {
    if !path.exists() {
        return None;
    }
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(_) => return None,
    };
    parse_proc_nfs_rpc_stats(&contents)
}

fn parse_nfs_mount_count(contents: &str) -> u64 {
    let mut count = 0u64;
    for line in contents.lines() {
        let mut fields = line.split_whitespace();
        let _ = fields.next();
        let _ = fields.next();
        if let Some(fstype) = fields.next() {
            if fstype == "nfs" || fstype == "nfs4" {
                count += 1;
            }
        }
    }
    count
}

fn parse_proc_nfs_rpc_stats(contents: &str) -> Option<(u64, u64, u64)> {
    for line in contents.lines() {
        if !line.starts_with("rpc ") {
            continue;
        }
        let values = line.split_whitespace().skip(1).collect::<Vec<_>>();
        if values.len() < 3 {
            continue;
        }
        let calls = values[0].parse::<u64>().unwrap_or(0);
        let retrans = values[1].parse::<u64>().unwrap_or(0);
        let auth_refreshes = values[2].parse::<u64>().unwrap_or(0);
        return Some((calls, retrans, auth_refreshes));
    }
    None
}

#[allow(clippy::question_mark)]
fn collect_rpc_stats_from_nfsstat(cfg: &NfsClientConfig) -> Option<(u64, u64, u64)> {
    let executable = cfg
        .executable
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or("nfsstat");
    let mut cmd = Command::new(executable);
    cmd.args(["-c"]);
    let output = match run_with_timeout(cmd, CMD_TIMEOUT) {
        Some(output) => output,
        None => return None,
    };
    if !output.status.success() {
        warn!(stderr = %String::from_utf8_lossy(&output.stderr), "nfsstat command failed");
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    parse_nfsstat_client_output(&text)
}

fn parse_nfsstat_client_output(text: &str) -> Option<(u64, u64, u64)> {
    let lines: Vec<&str> = text.lines().collect();
    for (i, line) in lines.iter().enumerate() {
        let lowered = line.to_ascii_lowercase();
        if !lowered.contains("calls") || !lowered.contains("retrans") {
            continue;
        }
        let data_line = match lines.get(i + 1) {
            Some(line) => *line,
            None => return None,
        };
        let mut values = Vec::new();
        for token in data_line.split_whitespace() {
            if let Ok(parsed) = token.parse::<u64>() {
                values.push(parsed);
            }
        }
        if values.len() < 2 {
            continue;
        }
        return Some((values[0], values[1], values.get(2).copied().unwrap_or(0)));
    }
    None
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
#[path = "../tests/platform_linux_tests.rs"]
mod tests;
