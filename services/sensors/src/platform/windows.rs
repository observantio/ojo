use crate::{SensorSample, SensorSnapshot};
use std::process::Command;

pub(crate) fn collect_snapshot() -> SensorSnapshot {
    let mut snap = SensorSnapshot::default();

    let temperatures = collect_temperatures();
    if !temperatures.is_empty() {
        snap.available = true;
        snap.temperatures = temperatures;
    }

    let fans = collect_fans();
    if !fans.is_empty() {
        snap.available = true;
        snap.fans = fans;
    }

    let voltages = collect_voltages();
    if !voltages.is_empty() {
        snap.available = true;
        snap.voltages = voltages;
    }

    snap
}

fn collect_temperatures() -> Vec<SensorSample> {
    // MSAcpi_ThermalZoneTemperature reports 1/10 Kelvin.
    let values = powershell_values(
        "Get-CimInstance -Namespace root/wmi -ClassName MSAcpi_ThermalZoneTemperature | ForEach-Object {$_.CurrentTemperature}",
    );
    values
        .iter()
        .enumerate()
        .filter_map(|(i, raw)| raw.parse::<f64>().ok().map(|v| (i, v)))
        .map(|(i, v)| SensorSample {
            chip: "acpi".to_string(),
            kind: "temperature".to_string(),
            label: format!("tz{i}"),
            value: (v / 10.0) - 273.15,
        })
        .collect()
}

fn collect_fans() -> Vec<SensorSample> {
    let values = powershell_values(
        "Get-CimInstance -ClassName Win32_Fan | ForEach-Object {$_.DesiredSpeed}",
    );
    values
        .iter()
        .enumerate()
        .filter_map(|(i, raw)| raw.parse::<f64>().ok().map(|v| (i, v)))
        .map(|(i, v)| SensorSample {
            chip: "win32".to_string(),
            kind: "fan".to_string(),
            label: format!("fan{i}"),
            value: v,
        })
        .collect()
}

fn collect_voltages() -> Vec<SensorSample> {
    let values = powershell_values(
        "Get-CimInstance -ClassName Win32_VoltageProbe | ForEach-Object {$_.CurrentReading}",
    );
    values
        .iter()
        .enumerate()
        .filter_map(|(i, raw)| raw.parse::<f64>().ok().map(|v| (i, v)))
        .map(|(i, v)| SensorSample {
            chip: "win32".to_string(),
            kind: "voltage".to_string(),
            label: format!("volt{i}"),
            value: v,
        })
        .collect()
}

fn powershell_values(script: &str) -> Vec<String> {
    let output = Command::new("powershell")
        .args(["-NoProfile", "-Command", script])
        .output();
    let Ok(output) = output else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}
