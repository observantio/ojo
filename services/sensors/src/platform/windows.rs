use crate::{SensorSample, SensorSnapshot};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tracing::warn;

const CMD_TIMEOUT: Duration = Duration::from_secs(15);

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
    let values = powershell_values(
        "Get-CimInstance -Namespace root/wmi -ClassName MSAcpi_ThermalZoneTemperature | ForEach-Object {$_.CurrentTemperature}",
    );
    values
        .iter()
        .enumerate()
        .filter_map(|(i, raw)| {
            raw.parse::<f64>().ok().and_then(|v| {
                let celsius = (v / 10.0) - 273.15;
                if celsius < -40.0 || celsius > 150.0 {
                    None
                } else {
                    Some((i, celsius))
                }
            })
        })
        .map(|(i, celsius)| SensorSample {
            chip: "acpi".to_string(),
            kind: "temperature".to_string(),
            label: format!("tz{i}"),
            value: celsius,
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
    let mut cmd = Command::new("powershell");
    cmd.args(["-NoProfile", "-Command", script]);
    let Some(output) = run_with_timeout(cmd, CMD_TIMEOUT) else {
        return Vec::new();
    };
    if !output.status.success() {
        warn!(stderr = %String::from_utf8_lossy(&output.stderr), "powershell command failed");
        return Vec::new();
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn run_with_timeout(mut cmd: Command, timeout: Duration) -> Option<std::process::Output> {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "failed to spawn command");
            return None;
        }
    };
    let start = Instant::now();
    loop {
        match child.try_wait() {
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
