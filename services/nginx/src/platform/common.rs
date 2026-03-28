use crate::{NginxConfig, NginxSnapshot};
use std::process::Child;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tracing::warn;

const CMD_TIMEOUT: Duration = Duration::from_secs(15);

pub(super) fn collect_snapshot_impl(cfg: &NginxConfig, default_executable: &str) -> NginxSnapshot {
    if let Ok(stub_status) = std::env::var("OJO_NGINX_STUB_STATUS") {
        return parse_stub_status(&stub_status);
    }

    let executable = if cfg.executable.trim().is_empty() {
        default_executable
    } else {
        cfg.executable.as_str()
    };
    let mut command = Command::new(executable);
    command.args([
        "--silent",
        "--show-error",
        "--max-time",
        "10",
        &cfg.status_url,
    ]);

    let maybe_output = run_with_timeout(command, CMD_TIMEOUT);
    let output = match maybe_output {
        Some(output) => output,
        None => return NginxSnapshot::default(),
    };
    if !output.status.success() {
        warn!(stderr = %String::from_utf8_lossy(&output.stderr), "nginx status command failed");
        return NginxSnapshot::default();
    }

    let text = String::from_utf8_lossy(&output.stdout);
    parse_stub_status(&text)
}

fn parse_stub_status(text: &str) -> NginxSnapshot {
    let lines = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    if lines.len() < 3 {
        return NginxSnapshot::default();
    }

    let active = parse_number_after_prefix(lines[0], "Active connections:").unwrap_or(0);
    let mut accepts_total = 0;
    let mut handled_total = 0;
    let mut requests_total = 0;
    for line in &lines {
        let numbers = line
            .split_whitespace()
            .filter_map(|token| token.parse::<u64>().ok())
            .collect::<Vec<_>>();
        if numbers.len() >= 3 {
            accepts_total = numbers[0];
            handled_total = numbers[1];
            requests_total = numbers[2];
            break;
        }
    }

    let mut reading = 0;
    let mut writing = 0;
    let mut waiting = 0;
    for line in &lines {
        if !line.to_ascii_lowercase().contains("reading") {
            continue;
        }
        let parts = line.split_whitespace().collect::<Vec<_>>();
        let mut index = 0;
        while index + 1 < parts.len() {
            let key = parts[index].trim_end_matches(':').to_ascii_lowercase();
            let value = parts[index + 1].parse::<u64>().unwrap_or(0);
            match key.as_str() {
                "reading" => reading = value,
                "writing" => writing = value,
                "waiting" => waiting = value,
                _ => {}
            }
            index += 2;
        }
    }

    if accepts_total == 0 && handled_total == 0 && requests_total == 0 && active == 0 {
        return NginxSnapshot::default();
    }

    NginxSnapshot {
        available: true,
        up: true,
        connections_active: active,
        connections_reading: reading,
        connections_writing: writing,
        connections_waiting: waiting,
        accepts_total,
        handled_total,
        requests_total,
    }
}

fn parse_number_after_prefix(line: &str, prefix: &str) -> Option<u64> {
    line.strip_prefix(prefix)?.trim().parse::<u64>().ok()
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
#[path = "../tests/platform_common_tests.rs"]
mod tests;
