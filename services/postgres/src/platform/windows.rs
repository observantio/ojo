use crate::{PostgresConfig, PostgresSnapshot};
use std::process::Command;

pub(crate) fn collect_snapshot(cfg: &PostgresConfig) -> PostgresSnapshot {
    collect_snapshot_impl(cfg, "psql.exe")
}

fn collect_snapshot_impl(cfg: &PostgresConfig, default_executable: &str) -> PostgresSnapshot {
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

    let output = command.output();
    let Ok(output) = output else {
        return PostgresSnapshot::default();
    };
    if !output.status.success() {
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
