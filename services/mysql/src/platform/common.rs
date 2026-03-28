use crate::{MysqlConfig, MysqlSnapshot};
use std::process::Child;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tracing::warn;

const CMD_TIMEOUT: Duration = Duration::from_secs(15);

pub(super) fn collect_snapshot_impl(cfg: &MysqlConfig, default_executable: &str) -> MysqlSnapshot {
    let executable = if cfg.executable.trim().is_empty() {
        default_executable
    } else {
        cfg.executable.as_str()
    };
    let mut command = Command::new(executable);
    command.args(["--batch", "--raw", "--skip-column-names"]);
    if let Some(host) = &cfg.host {
        command.args(["-h", host]);
    }
    if let Some(port) = cfg.port {
        command.args(["-P", &port.to_string()]);
    }
    if let Some(user) = &cfg.user {
        command.args(["-u", user]);
    }
    if let Some(password) = &cfg.password {
        command.arg(format!("-p{password}"));
    }
    if let Some(database) = &cfg.database {
        command.args(["-D", database]);
    }
    command.args([
        "-e",
        "SHOW GLOBAL STATUS WHERE Variable_name IN ('Threads_connected','Threads_running','Queries','Slow_queries','Bytes_received','Bytes_sent')",
    ]);

    let maybe_output = run_with_timeout(command, CMD_TIMEOUT);
    let output = match maybe_output {
        Some(output) => output,
        None => return MysqlSnapshot::default(),
    };
    if !output.status.success() {
        warn!(stderr = %String::from_utf8_lossy(&output.stderr), "mysql command failed");
        return MysqlSnapshot::default();
    }

    let text = String::from_utf8_lossy(&output.stdout);
    parse_mysql_status_output(&text)
}

fn parse_mysql_status_output(text: &str) -> MysqlSnapshot {
    let mut values = std::collections::BTreeMap::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
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
