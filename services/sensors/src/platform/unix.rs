use crate::{SensorSample, SensorSnapshot};
use std::fs;
use std::path::{Path, PathBuf};

pub(crate) fn collect_snapshot() -> SensorSnapshot {
    let base = PathBuf::from("/sys/class/hwmon");
    let Ok(entries) = fs::read_dir(&base) else {
        return SensorSnapshot::default();
    };
    let mut snap = SensorSnapshot::default();
    for entry in entries.flatten() {
        let path = entry.path();
        let chip = fs::read_to_string(path.join("name"))
            .map(|v| v.trim().to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        collect_temps(&path, &chip, &mut snap);
        collect_fans(&path, &chip, &mut snap);
        collect_voltages(&path, &chip, &mut snap);
    }
    snap
}

fn collect_temps(hwmon: &Path, chip: &str, snap: &mut SensorSnapshot) {
    let Ok(entries) = fs::read_dir(hwmon) else { return };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        let Some(rest) = name_str.strip_prefix("temp") else { continue };
        let Some(idx_str) = rest.strip_suffix("_input") else { continue };
        if idx_str.parse::<u32>().is_err() { continue }
        if let Some(value) = read_scaled(&entry.path(), 1000.0) {
            let label = read_label(hwmon, &format!("temp{idx_str}_label"))
                .unwrap_or_else(|| format!("temp{idx_str}"));
            snap.temperatures.push(SensorSample {
                chip: chip.to_string(),
                kind: "temperature".to_string(),
                label,
                value,
            });
            snap.available = true;
        }
    }
}

fn collect_fans(hwmon: &Path, chip: &str, snap: &mut SensorSnapshot) {
    let Ok(entries) = fs::read_dir(hwmon) else { return };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        let Some(rest) = name_str.strip_prefix("fan") else { continue };
        let Some(idx_str) = rest.strip_suffix("_input") else { continue };
        if idx_str.parse::<u32>().is_err() { continue }
        if let Some(value) = read_scaled(&entry.path(), 1.0) {
            let label = read_label(hwmon, &format!("fan{idx_str}_label"))
                .unwrap_or_else(|| format!("fan{idx_str}"));
            snap.fans.push(SensorSample {
                chip: chip.to_string(),
                kind: "fan".to_string(),
                label,
                value,
            });
            snap.available = true;
        }
    }
}

fn collect_voltages(hwmon: &Path, chip: &str, snap: &mut SensorSnapshot) {
    let Ok(entries) = fs::read_dir(hwmon) else { return };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        let Some(rest) = name_str.strip_prefix("in") else { continue };
        let Some(idx_str) = rest.strip_suffix("_input") else { continue };
        if idx_str.parse::<u32>().is_err() { continue }
        if let Some(value) = read_scaled(&entry.path(), 1000.0) {
            let label = read_label(hwmon, &format!("in{idx_str}_label"))
                .unwrap_or_else(|| format!("in{idx_str}"));
            snap.voltages.push(SensorSample {
                chip: chip.to_string(),
                kind: "voltage".to_string(),
                label,
                value,
            });
            snap.available = true;
        }
    }
}

fn read_label(hwmon: &Path, label_file: &str) -> Option<String> {
    let s = fs::read_to_string(hwmon.join(label_file)).ok()?;
    let trimmed = s.trim();
    if trimmed.is_empty() { None } else { Some(trimmed.to_string()) }
}

fn read_scaled(path: &Path, scale: f64) -> Option<f64> {
    let raw = fs::read_to_string(path).ok()?;
    let value = raw.trim().parse::<f64>().ok()?;
    let scaled = value / scale;
    if scaled.is_nan() || scaled.is_infinite() {
        return None;
    }
    Some(scaled)
}
