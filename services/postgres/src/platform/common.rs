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

    let text = String::from_utf8_lossy(&output.stdout);
    parse_postgres_tsv_output(&text)
}

fn parse_postgres_tsv_output(text: &str) -> PostgresSnapshot {
    let line = text
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

#[cfg(test)]
mod tests {
    use super::{collect_snapshot_impl, parse_postgres_tsv_output, parse_u64, run_with_timeout};
    use crate::PostgresConfig;
    use std::process::Command;
    use std::time::Duration;

    #[test]
    fn parse_u64_handles_invalid_and_trimmed_input() {
        assert_eq!(parse_u64("bad"), 0);
        assert_eq!(parse_u64(" 99 "), 99);
    }

    #[test]
    fn collect_snapshot_impl_returns_default_when_command_cannot_spawn() {
        let cfg = PostgresConfig {
            executable: "/definitely/missing/psql".to_string(),
            ..PostgresConfig::default()
        };
        let snap = collect_snapshot_impl(&cfg, "psql");
        assert!(!snap.available);
        assert_eq!(snap.connections, 0);
    }

    #[test]
    fn parse_postgres_tsv_output_parses_expected_tsv_line() {
        let snap = parse_postgres_tsv_output("7\t100\t4\t2\t20\t50\n");

        assert!(snap.available);
        assert!(snap.up);
        assert_eq!(snap.connections, 7);
        assert_eq!(snap.xact_commit_total, 100);
        assert_eq!(snap.xact_rollback_total, 4);
        assert_eq!(snap.deadlocks_total, 2);
        assert_eq!(snap.blks_read_total, 20);
        assert_eq!(snap.blks_hit_total, 50);
    }

    #[test]
    fn parse_postgres_tsv_output_returns_default_for_short_lines() {
        let snap = parse_postgres_tsv_output("1\t2\n");
        assert!(!snap.available);
        assert_eq!(snap.connections, 0);
    }

    #[test]
    fn run_with_timeout_covers_success_and_timeout_paths() {
        let mut ok_cmd = Command::new("sh");
        ok_cmd.args(["-c", "printf 'ok'"]);
        let output = run_with_timeout(ok_cmd, Duration::from_secs(1)).expect("expected output");
        assert!(output.status.success());
        assert_eq!(String::from_utf8_lossy(&output.stdout), "ok");

        let mut slow_cmd = Command::new("sh");
        slow_cmd.args(["-c", "sleep 1"]);
        assert_eq!(run_with_timeout(slow_cmd, Duration::from_millis(10)), None);
    }
}
