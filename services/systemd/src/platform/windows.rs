use crate::SystemdSnapshot;

pub(crate) fn collect_snapshot() -> SystemdSnapshot {
    let output = super::common::run_command_with_timeout(
        "powershell",
        &[
            "-NoProfile",
            "-Command",
            "$s=@(Get-CimInstance Win32_Service -ErrorAction Stop);\n\
             $total=$s.Count;\n\
             $running=@($s | Where-Object { $_.State -eq 'Running' }).Count;\n\
             $stopped=@($s | Where-Object { $_.State -eq 'Stopped' }).Count;\n\
             $paused=@($s | Where-Object { $_.State -eq 'Paused' }).Count;\n\
             $startPending=@($s | Where-Object { $_.State -eq 'Start Pending' }).Count;\n\
             $continuePending=@($s | Where-Object { $_.State -eq 'Continue Pending' }).Count;\n\
             $stopPending=@($s | Where-Object { $_.State -eq 'Stop Pending' }).Count;\n\
             $pausePending=@($s | Where-Object { $_.State -eq 'Pause Pending' }).Count;\n\
             $failed=@($s | Where-Object {\n\
                 $_.State -eq 'Stopped' -and\n\
                 $_.ExitCode -ne 0 -and\n\
                 $_.StartMode -ne 'Disabled'\n\
             }).Count;\n\
             $active=$running;\n\
             $inactive=$stopped + $paused;\n\
             $activating=$startPending + $continuePending;\n\
             $deactivating=$stopPending + $pausePending;\n\
             $maintenance=$paused;\n\
             $jobsRunning=$activating + $deactivating;\n\
             Write-Output \"total=$total\";\n\
             Write-Output \"active=$active\";\n\
             Write-Output \"inactive=$inactive\";\n\
             Write-Output \"failed=$failed\";\n\
             Write-Output \"activating=$activating\";\n\
             Write-Output \"deactivating=$deactivating\";\n\
             Write-Output \"maintenance=$maintenance\";\n\
             Write-Output \"jobs_running=$jobsRunning\";",
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
        units_maintenance: *map.get("maintenance").unwrap_or(&0),
        jobs_queued: 0,
        jobs_running: *map.get("jobs_running").unwrap_or(&0),
        failed_units_reported: *map.get("failed").unwrap_or(&0),
    }
}
