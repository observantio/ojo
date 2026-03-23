use crate::{PostgresConfig, PostgresSnapshot};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tracing::warn;

const CMD_TIMEOUT: Duration = Duration::from_secs(15);

pub(super) fn collect_snapshot_impl(
    cfg: &PostgresConfig,
    default_executable: &str,
) -> PostgresSnapshot {
    let executable = if cfg.executable.trim().is_empty() {
        default_executable
    } else {
        cfg.executable.as_str()
    };
    let mut command = Command::new(executable);
    command.args(["-At", "-F", "\t"]);
    if let Some(uri) = &cfg.uri {
        command.args(["-d", uri]);
    }
    command.args([
        "-c",
        "SELECT
            (SELECT COUNT(*) FROM pg_stat_activity),
            COALESCE(SUM(xact_commit), 0),
            COALESCE(SUM(xact_rollback), 0),
            COALESCE(SUM(deadlocks), 0),
            COALESCE(SUM(blks_read), 0),
            COALESCE(SUM(blks_hit), 0)
        FROM pg_stat_database;",
    ]);

    let Some(output) = run_with_timeout(command, CMD_TIMEOUT) else {
        return PostgresSnapshot::default();
    };
    if !output.status.success() {
        warn!(stderr = %String::from_utf8_lossy(&output.stderr), "psql command failed");
        return PostgresSnapshot::default();
    }
    let line = String::from_utf8_lossy(&output.stdout)
        .lines()
        .find(|v| !v.trim().is_empty())
        .unwrap_or_default()
        .to_string();
    let values = line.split('\t').collect::<Vec<_>>();
    if values.len() < 6 {
        return PostgresSnapshot::default();
    }

    PostgresSnapshot {
        available: true,
        up: true,
        connections: parse_u64(values[0]),
        xact_commit_total: parse_u64(values[1]),
        xact_rollback_total: parse_u64(values[2]),
        deadlocks_total: parse_u64(values[3]),
        blks_read_total: parse_u64(values[4]),
        blks_hit_total: parse_u64(values[5]),
    }
}

fn parse_u64(value: &str) -> u64 {
    value.trim().parse::<u64>().unwrap_or(0)
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
