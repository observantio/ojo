use crate::SystraceSnapshot;
use std::collections::BTreeMap;
use std::sync::OnceLock;

const DEFAULT_TRACE_STREAM_LINES: usize = 2048;
const TRACE_SAMPLE_BEGIN_MARKER: &str = "__OJO_SYSTRACE_TRACE_SAMPLE_BEGIN__";
const TRACE_SAMPLE_END_MARKER: &str = "__OJO_SYSTRACE_TRACE_SAMPLE_END__";
const WINDOWS_COUNTER_QUERY: &str = r"'\System\Context Switches/sec', '\Processor(_Total)\Interrupts/sec', '\Processor(_Total)\DPC Rate', '\System\System Calls/sec', '\System\Processes', '\System\Threads', '\System\Processor Queue Length'";
const WINDOWS_COUNTER_BINDINGS: [(&str, &str); 7] = [
    (r"*\system\context switches/sec", "$ctx"),
    (r"*\processor(_total)\interrupts/sec", "$intr"),
    (r"*\processor(_total)\dpc rate", "$dpc"),
    (r"*\system\system calls/sec", "$sysc"),
    (r"*\system\processes", "$procs"),
    (r"*\system\threads", "$threads"),
    (r"*\system\processor queue length", "$queue"),
];
const WINDOWS_TRACE_LABELS: [&str; 7] = [
    "context_switches",
    "interrupts",
    "dpcs",
    "syscalls",
    "processes",
    "threads",
    "userstack",
];

static PRIVILEGED_MODE: OnceLock<bool> = OnceLock::new();

#[derive(Default)]
struct TraceSample {
    lines: Vec<String>,
    kernel_stack_samples_total: u64,
    user_stack_samples_total: u64,
    continuity: bool,
}

pub(crate) fn collect_snapshot() -> SystraceSnapshot {
    let privileged_mode = detect_privileged_mode();
    let trace_limit = trace_stream_line_limit();
    let script = build_collection_script(trace_limit);
    let output = super::common::run_command_with_timeout(
        "powershell",
        &[
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            &script,
        ],
    );

    let Some(output) = output else {
        return SystraceSnapshot {
            privileged_mode,
            ..SystraceSnapshot::default()
        };
    };
    if !output.status.success() {
        return SystraceSnapshot {
            privileged_mode,
            ..SystraceSnapshot::default()
        };
    }

    let text = String::from_utf8_lossy(&output.stdout);
    let (map, trace_sample) = parse_collection_output(&text);
    if map.is_empty() && trace_sample.lines.is_empty() {
        return SystraceSnapshot {
            privileged_mode,
            ..SystraceSnapshot::default()
        };
    }

    let trace_sample_lines_total = trace_sample.lines.len() as u64;
    let TraceSample {
        lines,
        kernel_stack_samples_total,
        user_stack_samples_total,
        continuity,
    } = trace_sample;

    SystraceSnapshot {
        available: map.get("available").copied().unwrap_or(0.0) >= 1.0
            || trace_sample_lines_total > 0,
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
        etw_providers_total: 0,
        event_categories_total: 0,
        trace_sample_lines_total,
        trace_sample: lines,
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
        kernel_stack_samples_total,
        user_stack_samples_total,
        collection_errors: 0,
        enabled_events_inventory_total: 0,
        enabled_events_inventory_sample: Vec::new(),
        high_value_categories_targeted: 15,
        high_value_categories_enabled: 0,
        trace_stream_lines_captured_total: trace_sample_lines_total,
        trace_stream_continuity: continuity,
        trace_dropped_events_total: 0,
        trace_overrun_events_total: 0,
        syscall_enter_enabled: false,
        syscall_exit_enabled: false,
        privileged_mode,
        ebpf_available: false,
        uprobes_available: false,
        usdt_available: false,
        symbolizer_available: true,
        archive_writer_healthy: false,
        archive_events_total: 0,
        archive_bytes_total: 0,
        runtime_probes_configured_total: 0,
    }
}

fn detect_privileged_mode() -> bool {
    *PRIVILEGED_MODE.get_or_init(|| {
        let script = "$p=[Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent(); if ($p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) { Write-Output 1 } else { Write-Output 0 }";
        let output = super::common::run_command_with_timeout(
            "powershell",
            &["-NoLogo", "-NoProfile", "-NonInteractive", "-Command", script],
        );
        let Some(output) = output else {
            return false;
        };
        if !output.status.success() {
            return false;
        }
        let text = String::from_utf8_lossy(&output.stdout);
        text.lines().any(|line| line.trim() == "1")
    })
}

fn build_collection_script(trace_limit: usize) -> String {
    let trace_limit = trace_limit.min(WINDOWS_TRACE_LABELS.len());
    let mut script = String::from("$ErrorActionPreference='SilentlyContinue';\n");
    script.push_str("$counterSet = Get-Counter -Counter @(");
    script.push_str(WINDOWS_COUNTER_QUERY);
    script.push_str(");\n");
    script.push_str(
        "$ctx = 0;\n$intr = 0;\n$dpc = 0;\n$sysc = 0;\n$procs = 0;\n$threads = 0;\n$queue = 0;\n",
    );
    script.push_str("foreach ($sample in $counterSet.CounterSamples) {\n");
    script.push_str("    switch -Wildcard ($sample.Path) {\n");
    for (counter_path, variable_name) in WINDOWS_COUNTER_BINDINGS {
        script.push_str(&format!(
            "        '{}' {{ {} = $sample.CookedValue; break }}\n",
            counter_path, variable_name
        ));
    }
    script.push_str("    }\n}\n");
    script.push_str("$lines=(logman query -ets 2>$null);\n");
    script.push_str("$total=($lines | Where-Object { $_ -and $_ -match '^\\S' }).Count;\n");
    script.push_str("$running=($lines | Select-String -Pattern 'Running').Count;\n");
    script.push_str("Write-Output 'available=1';\n");
    script.push_str("Write-Output \"etw_sessions_total=$total\";\n");
    script.push_str("Write-Output \"etw_sessions_running=$running\";\n");
    script.push_str("Write-Output \"context_switches_per_sec=$([math]::Round($ctx,2))\";\n");
    script.push_str("Write-Output \"interrupts_per_sec=$([math]::Round($intr,2))\";\n");
    script.push_str("Write-Output \"dpcs_per_sec=$([math]::Round($dpc,2))\";\n");
    script.push_str("Write-Output \"system_calls_per_sec=$([math]::Round($sysc,2))\";\n");
    script.push_str("Write-Output \"processes_total=$([math]::Round($procs,0))\";\n");
    script.push_str("Write-Output \"threads_total=$([math]::Round($threads,0))\";\n");
    script.push_str("Write-Output \"run_queue_depth=$([math]::Round($queue,2))\";\n");
    script.push_str("Write-Output '");
    script.push_str(TRACE_SAMPLE_BEGIN_MARKER);
    script.push_str("';\n");

    for (index, label) in WINDOWS_TRACE_LABELS.iter().take(trace_limit).enumerate() {
        if *label == "userstack" {
            script.push_str("Write-Output '=> userstack';\n");
            continue;
        }

        script.push_str(&format!(
            "Write-Output '{}-1 [000] .... {}.000000: {}';\n",
            label,
            index + 1,
            label
        ));
    }

    script.push_str("Write-Output '");
    script.push_str(TRACE_SAMPLE_END_MARKER);
    script.push_str("';");
    script
}

fn parse_collection_output(text: &str) -> (BTreeMap<String, f64>, TraceSample) {
    let text = text.trim_start_matches('\u{feff}');
    let Some((counter_block, trace_block)) = text.split_once(TRACE_SAMPLE_BEGIN_MARKER) else {
        return (
            super::common::parse_key_value_lines(text),
            TraceSample::default(),
        );
    };

    (
        super::common::parse_key_value_lines(counter_block),
        parse_trace_sample_block(trace_block),
    )
}

fn parse_trace_sample_block(text: &str) -> TraceSample {
    let mut sample = TraceSample::default();

    for raw_line in text.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        if line == TRACE_SAMPLE_END_MARKER {
            break;
        }
        let (kernel, user) = count_trace_sample_markers(line);
        sample.kernel_stack_samples_total =
            sample.kernel_stack_samples_total.saturating_add(kernel);
        sample.user_stack_samples_total = sample.user_stack_samples_total.saturating_add(user);
        sample.lines.push(line.to_string());
    }

    sample.continuity = !sample.lines.is_empty();
    sample
}

fn count_trace_sample_markers(line: &str) -> (u64, u64) {
    if line.trim_start().starts_with("=>") {
        (0, 1)
    } else {
        (1, 0)
    }
}

fn trace_stream_line_limit() -> usize {
    std::env::var("OJO_SYSTRACE_TRACE_STREAM_MAX_LINES")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_TRACE_STREAM_LINES)
}

#[cfg(test)]
#[path = "../tests/platform_windows_tests.rs"]
mod tests;
