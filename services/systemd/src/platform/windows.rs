use crate::SystemdSnapshot;

pub(crate) fn collect_snapshot() -> SystemdSnapshot {
    let output = super::common::run_command_with_timeout(
        "powershell",
        &[
            "-NoProfile",
            "-Command",
            "$s=Get-Service;\nWrite-Output \"total=$($s.Count)\";\nWrite-Output \"active=$(($s | Where-Object {$_.Status -eq 'Running'}).Count)\";\nWrite-Output \"inactive=$(($s | Where-Object {$_.Status -eq 'Stopped'}).Count)\";\nWrite-Output \"failed=0\";\nWrite-Output \"activating=$(($s | Where-Object {$_.Status -eq 'StartPending'}).Count)\";\nWrite-Output \"deactivating=$(($s | Where-Object {$_.Status -eq 'StopPending'}).Count)\";",
        ],
    );

    let Some(output) = output else {
        return SystemdSnapshot::default();
    };
    if !output.status.success() {
        return SystemdSnapshot::default();
    }

    let text = String::from_utf8_lossy(&output.stdout);
    let map = super::common::parse_key_value_lines(&text);
    if map.is_empty() {
        return SystemdSnapshot::default();
    }

    SystemdSnapshot {
        available: true,
        units_total: *map.get("total").unwrap_or(&0),
        units_active: *map.get("active").unwrap_or(&0),
        units_inactive: *map.get("inactive").unwrap_or(&0),
        units_failed: *map.get("failed").unwrap_or(&0),
        units_activating: *map.get("activating").unwrap_or(&0),
        units_deactivating: *map.get("deactivating").unwrap_or(&0),
        units_reloading: 0,
        units_not_found: 0,
        units_maintenance: 0,
        jobs_queued: 0,
        jobs_running: 0,
        failed_units_reported: *map.get("failed").unwrap_or(&0),
    }
}
