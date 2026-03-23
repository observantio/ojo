use crate::{GpuSample, GpuSnapshot};
use std::process::Command;

pub(crate) fn collect_snapshot() -> GpuSnapshot {
    if let Some(samples) = collect_nvidia_smi() {
        return GpuSnapshot {
            available: true,
            samples,
        };
    }
    GpuSnapshot::default()
}

fn collect_nvidia_smi() -> Option<Vec<GpuSample>> {
    let output = Command::new("nvidia-smi")
        .args([
            "--query-gpu=name,utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw,clocks_throttle_reasons.active",
            "--format=csv,noheader,nounits",
        ])
        .output()
        .ok()?;
    if !output.status.success() {
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
        let throttled = parts[6].eq_ignore_ascii_case("active");

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
