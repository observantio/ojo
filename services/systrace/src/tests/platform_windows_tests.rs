#[test]
fn windows_collect_snapshot_is_callable_and_stable() {
    let snapshot = super::collect_snapshot();
    if snapshot.etw_available {
        assert!(snapshot.available);
        assert!(snapshot.etw_sessions_total >= snapshot.etw_sessions_running);
        assert!(snapshot.etw_providers_total < 1_000_000);
        assert!(snapshot.processes_total >= 1);
        assert!(snapshot.threads_total >= snapshot.processes_total);
        assert!(snapshot.run_queue_depth >= 0.0);
        assert!(snapshot.context_switches_per_sec >= 0.0);
        assert!(snapshot.interrupts_per_sec >= 0.0);
        assert!(snapshot.system_calls_per_sec >= 0.0);
        assert_eq!(snapshot.system_calls_source, "windows_perf_counter");
        assert_eq!(snapshot.system_calls_source_code, 10);
        assert_eq!(snapshot.system_calls_coverage_ratio, 1.0);
        assert!(snapshot.dpcs_per_sec >= 0.0);
        assert!(!snapshot.trace_sample.is_empty());
        assert_eq!(
            snapshot.trace_sample_lines_total,
            snapshot.trace_sample.len() as u64
        );
        assert_eq!(
            snapshot.trace_stream_lines_captured_total,
            snapshot.trace_sample_lines_total
        );
        assert!(snapshot.kernel_stack_samples_total >= 1);
        assert!(snapshot.trace_stream_continuity);
    } else {
        assert!(!snapshot.available);
    }
}

#[test]
fn parse_collection_output_extracts_counters_and_trace_sample() {
    let text = concat!(
        "available=1\n",
        "etw_sessions_total=2\n",
        "etw_sessions_running=1\n",
        "context_switches_per_sec=7.5\n",
        "__OJO_SYSTRACE_TRACE_SAMPLE_BEGIN__\n",
        "sched_switch-1 [000] .... 1.000000: sched_switch\n",
        "=> userstack\n",
        "__OJO_SYSTRACE_TRACE_SAMPLE_END__\n",
    );

    let (map, trace_sample) = super::parse_collection_output(text);
    assert_eq!(map.get("available"), Some(&1.0));
    assert_eq!(map.get("etw_sessions_total"), Some(&2.0));
    assert_eq!(map.get("context_switches_per_sec"), Some(&7.5));
    assert_eq!(trace_sample.lines.len(), 2);
    assert_eq!(trace_sample.kernel_stack_samples_total, 1);
    assert_eq!(trace_sample.user_stack_samples_total, 1);
    assert!(trace_sample.continuity);
}

#[test]
fn build_collection_script_uses_one_counter_query_and_preserves_trace_rows() {
    let script = super::build_collection_script(3);
    assert_eq!(script.matches("Get-Counter").count(), 1);
    assert!(script.contains("switch -Wildcard ($sample.Path)"));
    assert!(
        script.contains("Write-Output 'context_switches-1 [000] .... 1.000000: context_switches';")
    );
    assert!(script.contains("Write-Output 'interrupts-1 [000] .... 2.000000: interrupts';"));
    assert!(script.contains("Write-Output 'dpcs-1 [000] .... 3.000000: dpcs';"));
    assert!(!script.contains("Write-Output '=> userstack';"));
    assert!(!script.contains("sched_switch-1 [000] .... 1.000000: sched_switch"));
    assert!(!script.contains("Measure-Object -Average"));
}
