use crate::{SensorSample, SensorSnapshot};
use std::fs;
use std::path::{Path, PathBuf};

pub(crate) fn collect_snapshot() -> SensorSnapshot {
    let base = PathBuf::from("/sys/class/hwmon");
    let entries = fs::read_dir(&base);
    let Ok(entries) = entries else {
        return SensorSnapshot::default();
    };
    let mut snap = SensorSnapshot::default();
    for entry in entries.flatten() {
        let path = entry.path();
        let chip = fs::read_to_string(path.join("name"))
            .map(|v| v.trim().to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        let mut idx = 1usize;
        loop {
            let temp_path = path.join(format!("temp{idx}_input"));
            if !temp_path.exists() {
                break;
            }
            if let Some(value) = read_scaled(&temp_path, 1000.0) {
                snap.temperatures.push(SensorSample {
                    chip: chip.clone(),
                    kind: "temperature".to_string(),
                    label: format!("temp{idx}"),
                    value,
                });
                snap.available = true;
            }
            idx += 1;
        }

        let mut fan_idx = 1usize;
        loop {
            let fan_path = path.join(format!("fan{fan_idx}_input"));
            if !fan_path.exists() {
                break;
            }
            if let Some(value) = read_scaled(&fan_path, 1.0) {
                snap.fans.push(SensorSample {
                    chip: chip.clone(),
                    kind: "fan".to_string(),
                    label: format!("fan{fan_idx}"),
                    value,
                });
                snap.available = true;
            }
            fan_idx += 1;
        }

        let mut volt_idx = 0usize;
        loop {
            let volt_path = path.join(format!("in{volt_idx}_input"));
            if !volt_path.exists() {
                break;
            }
            if let Some(value) = read_scaled(&volt_path, 1000.0) {
                snap.voltages.push(SensorSample {
                    chip: chip.clone(),
                    kind: "voltage".to_string(),
                    label: format!("in{volt_idx}"),
                    value,
                });
                snap.available = true;
            }
            volt_idx += 1;
        }
    }
    snap
}

fn read_scaled(path: &Path, scale: f64) -> Option<f64> {
    let raw = fs::read_to_string(path).ok()?;
    let value = raw.trim().parse::<f64>().ok()?;
    Some(value / scale)
}
