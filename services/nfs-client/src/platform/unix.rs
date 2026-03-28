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

fn run_with_timeout(cmd: Command, timeout: Duration) -> Option<std::process::Output> {
    run_with_timeout_using_waiter(cmd, timeout, |child| child.try_wait())
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
    let mut child = match cmd.spawn() {
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
mod tests {
    use super::{
        collect_mounts_from_path, collect_rpc_stats_from_nfsstat, collect_rpc_stats_from_proc_path,
        parse_nfs_mount_count, parse_nfsstat_client_output, parse_proc_nfs_rpc_stats,
        run_with_timeout, run_with_timeout_using_waiter, snapshot_from_sources,
    };
    use crate::NfsClientConfig;
    use std::fs;
    use std::process::Command;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn unique_temp_dir(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "ojo-nfs-unix-{name}-{}-{nanos}",
            std::process::id()
        ))
    }

    #[test]
    fn parse_nfsstat_client_output_reads_expected_values() {
        let text = "Client rpc stats:\ncalls retrans authrefrsh\n100 3 1\n";
        assert_eq!(parse_nfsstat_client_output(text), Some((100, 3, 1)));
        let two_cols = "calls retrans\n100 3\n";
        assert_eq!(parse_nfsstat_client_output(two_cols), Some((100, 3, 0)));
        assert_eq!(parse_nfsstat_client_output("calls retrans\n"), None);
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
        assert_eq!(parse_proc_nfs_rpc_stats("rpc 7 x 1\n"), Some((7, 0, 1)));

        let dir = unique_temp_dir("proc-rpc");
        fs::create_dir_all(&dir).expect("mkdir");
        let stats_file = dir.join("nfs-rpc");
        fs::write(&stats_file, "rpc 9 2 1\n").expect("write proc stats");
        assert_eq!(
            collect_rpc_stats_from_proc_path(&stats_file),
            Some((9, 2, 1))
        );
        fs::remove_file(&stats_file).expect("cleanup stats file");
        fs::remove_dir_all(&dir).expect("cleanup stats dir");

        let missing_file = unique_temp_dir("missing-proc-rpc").join("nfs-rpc");
        assert_eq!(collect_rpc_stats_from_proc_path(&missing_file), None);

        let proc_dir = unique_temp_dir("proc-rpc-dir");
        fs::create_dir_all(&proc_dir).expect("mkdir proc dir");
        assert_eq!(collect_rpc_stats_from_proc_path(&proc_dir), None);
        fs::remove_dir_all(&proc_dir).expect("cleanup proc dir");

        let mounts_dir = unique_temp_dir("mounts-dir");
        fs::create_dir_all(&mounts_dir).expect("mkdir mounts dir");
        assert_eq!(collect_mounts_from_path(&mounts_dir), None);
        fs::remove_dir_all(&mounts_dir).expect("cleanup mounts dir");
    }

    #[test]
    fn snapshot_from_sources_prefers_proc_then_nfsstat_then_default() {
        let from_proc = snapshot_from_sources(3, Some((11, 2, 1)), Some((22, 4, 2)));
        assert!(from_proc.available);
        assert_eq!(from_proc.mounts, 3);
        assert_eq!(from_proc.rpc_calls_total, 11);
        assert_eq!(from_proc.rpc_retransmissions_total, 2);
        assert_eq!(from_proc.rpc_auth_refreshes_total, 1);

        let from_nfsstat = snapshot_from_sources(5, None, Some((22, 4, 2)));
        assert!(from_nfsstat.available);
        assert_eq!(from_nfsstat.mounts, 5);
        assert_eq!(from_nfsstat.rpc_calls_total, 22);
        assert_eq!(from_nfsstat.rpc_retransmissions_total, 4);
        assert_eq!(from_nfsstat.rpc_auth_refreshes_total, 2);

        let unavailable = snapshot_from_sources(7, None, None);
        assert!(!unavailable.available);
        assert_eq!(unavailable.mounts, 0);
        assert_eq!(unavailable.rpc_calls_total, 0);
        assert_eq!(unavailable.rpc_retransmissions_total, 0);
        assert_eq!(unavailable.rpc_auth_refreshes_total, 0);
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

        let mut err_cmd = Command::new("sh");
        err_cmd.args(["-c", "printf 'ok'"]);
        let errored = run_with_timeout_using_waiter(err_cmd, Duration::from_secs(1), |_child| {
            Err(std::io::Error::other("forced wait error"))
        });
        assert_eq!(errored, None);
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

    #[test]
    fn collect_rpc_stats_from_nfsstat_parses_output_when_command_succeeds() {
        let dir = unique_temp_dir("nfsstat-success");
        fs::create_dir_all(&dir).expect("mkdir");
        let script = dir.join("fake-nfsstat.sh");
        fs::write(
            &script,
            "#!/bin/sh\nprintf 'Client rpc stats:\n'\nprintf 'calls retrans authrefrsh\n'\nprintf '123 4 2\n'\n",
        )
        .expect("write script");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&script).expect("metadata").permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&script, perms).expect("chmod");
        }

        let cfg = NfsClientConfig {
            executable: Some(script.to_string_lossy().to_string()),
        };
        assert_eq!(collect_rpc_stats_from_nfsstat(&cfg), Some((123, 4, 2)));

        fs::remove_file(&script).expect("cleanup script");
        fs::remove_dir_all(&dir).expect("cleanup dir");
    }
}
