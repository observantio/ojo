#[test]
fn windows_collect_snapshot_is_callable_and_stable() {
    let snapshot = super::collect_snapshot();
    if snapshot.etw_available {
        assert!(snapshot.available);
        assert!(snapshot.etw_sessions_total >= snapshot.etw_sessions_running);
        assert!(snapshot.etw_providers_total >= 0);
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
    } else {
        assert!(!snapshot.available);
    }
}
