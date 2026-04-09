use crate::{RedisConfig, RedisSnapshot};
use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::process::Child;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tracing::warn;

const CMD_TIMEOUT: Duration = Duration::from_secs(15);

pub(super) fn collect_snapshot_impl(cfg: &RedisConfig, default_executable: &str) -> RedisSnapshot {
    let executable = if cfg.executable.trim().is_empty() {
        default_executable
    } else {
        cfg.executable.as_str()
    };
    if executable == default_executable {
        if let Ok(info) = std::env::var("OJO_REDIS_INFO_STUB") {
            return parse_redis_info(&info);
        }
    }
    let mut command = Command::new(executable);
    if let Some(host) = &cfg.host {
        command.args(["-h", host]);
    }
    if let Some(port) = cfg.port {
        command.args(["-p", &port.to_string()]);
    }
    if let Some(username) = &cfg.username {
        command.args(["--user", username]);
    }
    if let Some(password) = &cfg.password {
        command.args(["-a", password]);
    }
    command.args(["INFO"]);

    let maybe_output = run_with_timeout(command, CMD_TIMEOUT);
    let output = match maybe_output {
        Some(output) => output,
        None => return RedisSnapshot::default(),
    };
    if !output.status.success() {
        warn!(stderr = %String::from_utf8_lossy(&output.stderr), "redis info command failed");
        return RedisSnapshot::default();
    }

    let text = String::from_utf8_lossy(&output.stdout);
    parse_redis_info(&text)
}

fn parse_redis_info(text: &str) -> RedisSnapshot {
    let mut map = BTreeMap::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((key, value)) = trimmed.split_once(':') else {
            continue;
        };
        map.insert(key.to_string(), value.to_string());
    }

    if map.is_empty() {
        return RedisSnapshot::default();
    }

    RedisSnapshot {
        available: true,
        up: true,
        connected_clients: parse_u64(map.get("connected_clients")),
        blocked_clients: parse_u64(map.get("blocked_clients")),
        memory_used_bytes: parse_u64(map.get("used_memory")),
        memory_max_bytes: parse_u64(map.get("maxmemory")),
        uptime_seconds: parse_u64(map.get("uptime_in_seconds")),
        commands_processed_total: parse_u64(map.get("total_commands_processed")),
        connections_received_total: parse_u64(map.get("total_connections_received")),
        keyspace_hits_total: parse_u64(map.get("keyspace_hits")),
        keyspace_misses_total: parse_u64(map.get("keyspace_misses")),
        expired_keys_total: parse_u64(map.get("expired_keys")),
        evicted_keys_total: parse_u64(map.get("evicted_keys")),
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
    let program = cmd.get_program().to_string_lossy().into_owned();
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let child_result = cmd.spawn();
    let mut child = match child_result {
        Ok(c) => c,
        Err(e) => {
            if e.kind() == ErrorKind::NotFound {
                let remediation = if cfg!(target_os = "windows") {
                    "Install `redis-cli.exe` and ensure it is on PATH, or set `redis.executable` in services/redis/redis.yaml (for example `C:\\Program Files\\Redis\\redis-cli.exe`)."
                } else {
                    "Install `redis-cli` (Ubuntu: `sudo apt install redis-tools`) or set `redis.executable` in services/redis/redis.yaml."
                };
                warn!(
                    command = %program,
                    %remediation,
                    "Redis command not found"
                );
            } else {
                warn!(error = %e, command = %program, "failed to spawn redis command");
            }
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
#[path = "../tests/platform_common_tests.rs"]
mod tests;
