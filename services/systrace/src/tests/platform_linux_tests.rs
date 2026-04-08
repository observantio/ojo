#[test]
fn linux_collect_snapshot_is_callable_and_stable() {
    let snapshot = super::collect_snapshot();
    if snapshot.tracefs_available {
        assert!(snapshot.available);
        assert!(snapshot.events_total >= snapshot.events_enabled);
        assert!(snapshot.event_categories_total <= snapshot.events_total);
        assert!(snapshot.run_queue_depth >= 0.0);
        assert!(snapshot.processes_total >= 1);
        assert!(snapshot.threads_total >= snapshot.processes_total);
        assert!(snapshot.context_switches_per_sec >= 0.0);
        assert!(snapshot.interrupts_per_sec >= 0.0);
        assert!(snapshot.system_calls_per_sec >= 0.0);
        assert!(snapshot.system_calls_coverage_ratio >= 0.0);
        assert!(snapshot.system_calls_coverage_ratio <= 1.0);
        assert!(snapshot.process_forks_per_sec >= 0.0);
    } else {
        assert!(!snapshot.available);
    }
}

#[test]
fn parse_vmstat_syscalls_prefers_nr_syscalls() {
    let vmstat = "nr_syscalls 100\nsyscalls 80\n";
    let parsed = super::parse_vmstat_syscalls_total(vmstat);
    assert_eq!(parsed, Some((100, super::SyscallSource::VmstatNrSyscalls)));
}

#[test]
fn parse_vmstat_syscalls_falls_back_to_syscalls_key() {
    let vmstat = "pgfault 1\nsyscalls 42\n";
    let parsed = super::parse_vmstat_syscalls_total(vmstat);
    assert_eq!(parsed, Some((42, super::SyscallSource::VmstatSyscalls)));
}

#[test]
fn parse_proc_stat_content_extracts_syscalls_when_present() {
    let stat = "ctxt 10\nintr 20\nprocesses 3\nprocs_running 2\nsyscalls 55\n";
    let counters = super::parse_proc_stat_content(stat);
    assert_eq!(counters.context_switches_total, 10);
    assert_eq!(counters.interrupts_total, 20);
    assert_eq!(counters.forks_total, 3);
    assert_eq!(counters.run_queue_depth, 2.0);
    assert_eq!(counters.syscalls_total, 55);
    assert_eq!(
        counters.syscall_source,
        super::SyscallSource::ProcStatSyscalls
    );
}

#[test]
fn parse_proc_io_syscalls_sums_read_and_write_counts() {
    let content = "rchar: 1\nwchar: 2\nsyscr: 9\nsyscw: 4\n";
    let total = super::parse_proc_io_syscalls(content);
    assert_eq!(total, Some(13));
}

#[test]
fn count_stack_sample_markers_detects_kernel_and_user_hints() {
    let (kernel, user) = super::count_stack_sample_markers("foo kernel_stack bar user_stack");
    assert_eq!(kernel, 1);
    assert_eq!(user, 1);

    let (kernel_only, user_only) = super::count_stack_sample_markers(" => schedule");
    assert_eq!(kernel_only, 1);
    assert_eq!(user_only, 0);
}

#[test]
fn bootstrap_rates_scale_from_uptime() {
    let counters = super::KernelCounters {
        context_switches_total: 100,
        interrupts_total: 80,
        forks_total: 20,
        syscalls_total: 60,
        ..Default::default()
    };
    let rates = super::bootstrap_rates(counters, 10.0);
    assert_eq!(rates.context_switches_per_sec, 10.0);
    assert_eq!(rates.interrupts_per_sec, 8.0);
    assert_eq!(rates.process_forks_per_sec, 2.0);
    assert_eq!(rates.system_calls_per_sec, 6.0);
}

#[test]
fn bootstrap_rates_handle_non_positive_uptime() {
    let counters = super::KernelCounters {
        context_switches_total: 100,
        interrupts_total: 80,
        forks_total: 20,
        syscalls_total: 60,
        ..Default::default()
    };
    let rates = super::bootstrap_rates(counters, 0.0);
    assert_eq!(rates.context_switches_per_sec, 0.0);
    assert_eq!(rates.interrupts_per_sec, 0.0);
    assert_eq!(rates.process_forks_per_sec, 0.0);
    assert_eq!(rates.system_calls_per_sec, 0.0);
}
