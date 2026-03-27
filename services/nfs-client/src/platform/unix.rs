use crate::{NfsClientConfig, NfsClientSnapshot};
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tracing::warn;

const CMD_TIMEOUT: Duration = Duration::from_secs(15);

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
    Some(parse_nfs_mount_count(&contents))
}

fn collect_rpc_stats_from_proc() -> Option<(u64, u64, u64)> {
    let path = Path::new("/proc/net/rpc/nfs");
    if !path.exists() {
        return None;
    }
    let contents = fs::read_to_string(path).ok()?;
    parse_proc_nfs_rpc_stats(&contents)
}

fn parse_nfs_mount_count(contents: &str) -> u64 {
    contents
        .lines()
        .filter(|line| {
            line.split_whitespace()
                .nth(2)
                .is_some_and(|fstype| fstype == "nfs" || fstype == "nfs4")
        })
        .count() as u64
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

fn collect_rpc_stats_from_nfsstat(cfg: &NfsClientConfig) -> Option<(u64, u64, u64)> {
    let executable = cfg
        .executable
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or("nfsstat");
    let mut cmd = Command::new(executable);
    cmd.args(["-c"]);
    let output = run_with_timeout(cmd, CMD_TIMEOUT)?;
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
        let data_line = lines.get(i + 1)?;
        let values: Vec<u64> = data_line
            .split_whitespace()
            .filter_map(|token| token.parse::<u64>().ok())
            .collect();
        if values.len() < 2 {
            continue;
        }
        return Some((values[0], values[1], values.get(2).copied().unwrap_or(0)));
    }
    None
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

#[cfg(test)]
mod tests {
    use super::{
        collect_rpc_stats_from_nfsstat, parse_nfs_mount_count, parse_nfsstat_client_output,
        parse_proc_nfs_rpc_stats, run_with_timeout,
    };
    use crate::NfsClientConfig;
    use std::process::Command;
    use std::time::Duration;

    #[test]
    fn parse_nfsstat_client_output_reads_expected_values() {
        let text = "Client rpc stats:\ncalls retrans authrefrsh\n100 3 1\n";
        assert_eq!(parse_nfsstat_client_output(text), Some((100, 3, 1)));
        let two_cols = "calls retrans\n100 3\n";
        assert_eq!(parse_nfsstat_client_output(two_cols), Some((100, 3, 0)));
        assert_eq!(
            parse_nfsstat_client_output("calls retrans\nfoo bar\n"),
            None
        );
    }

    #[test]
    fn parse_helpers_cover_mount_and_proc_rpc_formats() {
        let mounts =
            "server:/x /mnt nfs4 rw 0 0\nserver:/y /home ext4 rw 0 0\nserver:/z /data nfs rw 0 0\n";
        assert_eq!(parse_nfs_mount_count(mounts), 2);

        let proc_stats = "net 0 0 0\nrpc 42 3 1\n";
        assert_eq!(parse_proc_nfs_rpc_stats(proc_stats), Some((42, 3, 1)));
        assert_eq!(parse_proc_nfs_rpc_stats("rpc 1 2\n"), None);
    }

    #[test]
    fn run_with_timeout_covers_success_and_timeout_paths() {
        let mut ok_cmd = Command::new("sh");
        ok_cmd.args(["-c", "printf 'ok'"]);
        let ok_output = run_with_timeout(ok_cmd, Duration::from_secs(1)).expect("expected output");
        assert!(ok_output.status.success());
        assert_eq!(String::from_utf8_lossy(&ok_output.stdout), "ok");

        let mut slow_cmd = Command::new("sh");
        slow_cmd.args(["-c", "sleep 1"]);
        assert_eq!(run_with_timeout(slow_cmd, Duration::from_millis(10)), None);
    }

    #[test]
    fn collect_rpc_stats_from_nfsstat_returns_none_on_failed_exit() {
        let cfg = NfsClientConfig {
            executable: Some("sh".to_string()),
        };
        assert_eq!(collect_rpc_stats_from_nfsstat(&cfg), None);
    }

    #[test]
    fn collect_rpc_stats_from_nfsstat_returns_none_when_spawn_fails() {
        let cfg = NfsClientConfig {
            executable: Some("/definitely/missing/nfsstat".to_string()),
        };
        assert_eq!(collect_rpc_stats_from_nfsstat(&cfg), None);
    }
}
