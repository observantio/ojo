use crate::{PostgresConfig, PostgresSnapshot};
use std::process::Child;
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

    let maybe_output = run_with_timeout(command, CMD_TIMEOUT);
    let output = match maybe_output {
        Some(output) => output,
        None => return PostgresSnapshot::default(),
    };
    if !output.status.success() {
        warn!(stderr = %String::from_utf8_lossy(&output.stderr), "psql command failed");
        return PostgresSnapshot::default();
    }

    let text = String::from_utf8_lossy(&output.stdout);
    parse_postgres_tsv_output(&text)
}

fn parse_postgres_tsv_output(text: &str) -> PostgresSnapshot {
    let mut line = String::new();
    for candidate in text.lines() {
        if candidate.trim().is_empty() {
            continue;
        }
        line = candidate.to_string();
        break;
    }
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
