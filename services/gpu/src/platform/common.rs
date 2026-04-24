use crate::{GpuSample, GpuSnapshot};
use std::process::Child;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tracing::warn;

const CMD_TIMEOUT: Duration = Duration::from_secs(15);

pub(crate) fn collect_snapshot() -> GpuSnapshot {
    match collect_nvidia_smi() {
        Some(samples) => GpuSnapshot {
            available: true,
            samples,
        },
        None => GpuSnapshot::default(),
    }
}

fn collect_nvidia_smi() -> Option<Vec<GpuSample>> {
    let mut cmd = Command::new("nvidia-smi");
    cmd.args([
        "--query-gpu=name,utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw,clocks_throttle_reasons.active",
        "--format=csv,noheader,nounits",
    ]);
    let output = run_with_timeout(cmd, CMD_TIMEOUT)?;
    if !output.status.success() {
        warn!(stderr = %String::from_utf8_lossy(&output.stderr), "nvidia-smi failed");
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let mut samples = Vec::new();
    let lines: Vec<&str> = text.lines().collect();
    for (index, line) in lines.into_iter().enumerate() {
        let parts = line.split(',').map(str::trim).collect::<Vec<_>>();
        if parts.len() < 7 {
            continue;
        }
        let util = parts[1].parse::<f64>().unwrap_or(0.0) / 100.0;
        let mem_used = parts[2].parse::<f64>().unwrap_or(0.0) * 1024.0 * 1024.0;
        let mem_total = parts[3].parse::<f64>().unwrap_or(0.0) * 1024.0 * 1024.0;
        let temp = parts[4].parse::<f64>().unwrap_or(0.0);
        let power = parts[5].parse::<f64>().unwrap_or(0.0);
        let throttled = {
            let raw = parts[6]
                .trim()
                .trim_start_matches("0x")
                .trim_start_matches("0X");
            u64::from_str_radix(raw, 16).unwrap_or(0) != 0
        };

        samples.push(GpuSample {
            index,
            name: parts[0].to_string(),
            util_ratio: util,
            memory_used_bytes: mem_used,
            memory_total_bytes: mem_total,
            temperature_celsius: temp,
            power_watts: power,
            throttled,
        });
    }
    Some(samples)
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
            warn!(command = %program, error = %e, "{}", spawn_failure_message(&program, &e));
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

fn spawn_failure_message(program: &str, error: &std::io::Error) -> String {
    if error.kind() == std::io::ErrorKind::NotFound {
        format!("{program} not found; GPU metrics are unavailable on this host")
    } else {
        format!("failed to spawn {program}")
    }
}

#[cfg(test)]
#[path = "../tests/platform_common_tests.rs"]
mod tests;
