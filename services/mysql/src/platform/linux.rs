use crate::{MysqlConfig, MysqlSnapshot};
use std::collections::BTreeMap;
use std::process::Command;

pub(crate) fn collect_snapshot(cfg: &MysqlConfig) -> MysqlSnapshot {
    collect_snapshot_impl(cfg, "mysql")
}

fn collect_snapshot_impl(cfg: &MysqlConfig, default_executable: &str) -> MysqlSnapshot {
    let executable = if cfg.executable.trim().is_empty() {
        default_executable
    } else {
        cfg.executable.as_str()
    };
    let mut command = Command::new(executable);
    command.args(["--batch", "--raw", "--skip-column-names"]);
    if let Some(uri) = &cfg.uri {
        command.args(["--uri", uri]);
    }
    command.args([
        "-e",
        "SHOW GLOBAL STATUS WHERE Variable_name IN ('Threads_connected','Threads_running','Queries','Slow_queries','Bytes_received','Bytes_sent')",
    ]);

    let output = command.output();
    let Ok(output) = output else {
        return MysqlSnapshot::default();
    };
    if !output.status.success() {
        return MysqlSnapshot::default();
    }

    let text = String::from_utf8_lossy(&output.stdout);
    let mut values = BTreeMap::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let mut parts = line.split('\t');
        let key = parts.next().unwrap_or_default().trim();
        let value = parts.next().unwrap_or_default().trim();
        if !key.is_empty() {
            values.insert(key.to_string(), value.to_string());
        }
    }
    if values.is_empty() {
        return MysqlSnapshot::default();
    }

    MysqlSnapshot {
        available: true,
        up: true,
        connections: parse_u64(values.get("Threads_connected")),
        threads_running: parse_u64(values.get("Threads_running")),
        queries_total: parse_u64(values.get("Queries")),
        slow_queries_total: parse_u64(values.get("Slow_queries")),
        bytes_received_total: parse_u64(values.get("Bytes_received")),
        bytes_sent_total: parse_u64(values.get("Bytes_sent")),
    }
}

fn parse_u64(value: Option<&String>) -> u64 {
    value
        .map(String::as_str)
        .unwrap_or_default()
        .trim()
        .parse::<u64>()
        .unwrap_or(0)
}
