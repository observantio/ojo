use crate::{GpuSample, GpuSnapshot};
use std::process::Child;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tracing::warn;

const CMD_TIMEOUT: Duration = Duration::from_secs(15);

pub(crate) fn collect_snapshot() -> GpuSnapshot {
    let maybe_samples = collect_nvidia_smi();
    if let Some(samples) = maybe_samples {
        return GpuSnapshot {
            available: true,
            samples,
        };
    }
    GpuSnapshot::default()
}

fn collect_nvidia_smi() -> Option<Vec<GpuSample>> {
    let mut cmd = Command::new("nvidia-smi");
    cmd.args([
        "--query-gpu=name,utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw,clocks_throttle_reasons.active",
        "--format=csv,noheader,nounits",
    ]);
    let output = run_with_timeout(cmd, CMD_TIMEOUT);
    let output = output?;
    if !output.status.success() {
        warn!(stderr = %String::from_utf8_lossy(&output.stderr), "nvidia-smi failed");
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let mut samples = Vec::new();
    for (index, line) in text.lines().enumerate() {
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
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let spawn_result = cmd.spawn();
    let mut child = match spawn_result {
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
