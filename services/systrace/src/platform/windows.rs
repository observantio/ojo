use crate::SystraceSnapshot;

pub(crate) fn collect_snapshot() -> SystraceSnapshot {
    let output = super::common::run_command_with_timeout(
        "powershell",
        &[
            "-NoProfile",
            "-Command",
            "$ErrorActionPreference='SilentlyContinue';\n$ctx=(Get-Counter '\\System\\Context Switches/sec').CounterSamples | Select-Object -ExpandProperty CookedValue | Measure-Object -Average;\n$intr=(Get-Counter '\\Processor(_Total)\\Interrupts/sec').CounterSamples | Select-Object -ExpandProperty CookedValue | Measure-Object -Average;\n$dpc=(Get-Counter '\\Processor(_Total)\\DPC Rate').CounterSamples | Select-Object -ExpandProperty CookedValue | Measure-Object -Average;\n$sysc=(Get-Counter '\\System\\System Calls/sec').CounterSamples | Select-Object -ExpandProperty CookedValue | Measure-Object -Average;\n$procs=(Get-Counter '\\System\\Processes').CounterSamples | Select-Object -ExpandProperty CookedValue | Measure-Object -Average;\n$threads=(Get-Counter '\\System\\Threads').CounterSamples | Select-Object -ExpandProperty CookedValue | Measure-Object -Average;\n$queue=(Get-Counter '\\System\\Processor Queue Length').CounterSamples | Select-Object -ExpandProperty CookedValue | Measure-Object -Average;\n$lines=(logman query -ets 2>$null);\n$total=($lines | Where-Object { $_ -and $_ -match '^\\S' }).Count;\n$running=($lines | Select-String -Pattern 'Running').Count;\nWrite-Output 'available=1';\nWrite-Output \"etw_sessions_total=$total\";\nWrite-Output \"etw_sessions_running=$running\";\nWrite-Output \"context_switches_per_sec=$([math]::Round($ctx.Average,2))\";\nWrite-Output \"interrupts_per_sec=$([math]::Round($intr.Average,2))\";\nWrite-Output \"dpcs_per_sec=$([math]::Round($dpc.Average,2))\";\nWrite-Output \"system_calls_per_sec=$([math]::Round($sysc.Average,2))\";\nWrite-Output \"processes_total=$([math]::Round($procs.Average,0))\";\nWrite-Output \"threads_total=$([math]::Round($threads.Average,0))\";\nWrite-Output \"run_queue_depth=$([math]::Round($queue.Average,2))\";",
        ],
    );

    let Some(output) = output else {
        return SystraceSnapshot::default();
    };
    if !output.status.success() {
        return SystraceSnapshot::default();
    }

    let text = String::from_utf8_lossy(&output.stdout);
    let map = super::common::parse_key_value_lines(&text);
    if map.is_empty() {
        return SystraceSnapshot::default();
    }

    SystraceSnapshot {
        available: map.get("available").copied().unwrap_or(0.0) >= 1.0,
        tracefs_available: false,
        etw_available: true,
        tracing_on: map.get("etw_sessions_running").copied().unwrap_or(0.0) > 0.0,
        current_tracer: "etw".to_string(),
        tracers_available: 1,
        events_total: 0,
        events_enabled: 0,
        buffer_total_kb: 0,
        etw_sessions_total: map.get("etw_sessions_total").copied().unwrap_or(0.0) as u64,
        etw_sessions_running: map.get("etw_sessions_running").copied().unwrap_or(0.0) as u64,
        etw_providers_total: count_etw_providers(),
        event_categories_total: 0,
        trace_sample_lines_total: 0,
        trace_sample: Vec::new(),
        context_switches_per_sec: map.get("context_switches_per_sec").copied().unwrap_or(0.0),
        interrupts_per_sec: map.get("interrupts_per_sec").copied().unwrap_or(0.0),
        system_calls_per_sec: map.get("system_calls_per_sec").copied().unwrap_or(0.0),
        system_calls_source: "windows_perf_counter".to_string(),
        system_calls_source_code: 10,
        system_calls_coverage_ratio: 1.0,
        dpcs_per_sec: map.get("dpcs_per_sec").copied().unwrap_or(0.0),
        process_forks_per_sec: 0.0,
        run_queue_depth: map.get("run_queue_depth").copied().unwrap_or(0.0),
        processes_total: map.get("processes_total").copied().unwrap_or(0.0) as u64,
        threads_total: map.get("threads_total").copied().unwrap_or(0.0) as u64,
        kernel_stack_samples_total: 0,
        user_stack_samples_total: 0,
        collection_errors: 0,
        enabled_events_inventory_total: 0,
        enabled_events_inventory_sample: Vec::new(),
        high_value_categories_targeted: 15,
        high_value_categories_enabled: 0,
        trace_stream_lines_captured_total: 0,
        trace_stream_continuity: false,
        trace_dropped_events_total: 0,
        trace_overrun_events_total: 0,
        syscall_enter_enabled: false,
        syscall_exit_enabled: false,
        privileged_mode: true,
        ebpf_available: false,
        uprobes_available: false,
        usdt_available: false,
        symbolizer_available: true,
        archive_writer_healthy: false,
        archive_events_total: 0,
        archive_bytes_total: 0,
        runtime_probes_configured_total: 0,
        validation_datasets_total: 0,
        validation_last_success: false,
    }
}

fn count_etw_providers() -> u64 {
    if let Some(output) = super::common::run_command_with_timeout("wevtutil", &["enum-providers"]) {
        if output.status.success() {
            let text = String::from_utf8_lossy(&output.stdout);
            let count = text.lines().filter(|line| !line.trim().is_empty()).count();
            if count > 0 {
                return count as u64;
            }
        }
    }

    if let Some(output) = super::common::run_command_with_timeout("logman", &["query", "providers"])
    {
        if output.status.success() {
            let text = String::from_utf8_lossy(&output.stdout);
            return text.lines().filter(|line| !line.trim().is_empty()).count() as u64;
        }
    }

    0
}

#[cfg(test)]
#[path = "../tests/platform_windows_tests.rs"]
mod tests;
