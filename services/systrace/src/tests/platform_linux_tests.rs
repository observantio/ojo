use std::fs;
use std::path::Path;

fn unique_temp_dir(name: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "ojo-systrace-linux-tests-{name}-{}-{nanos}",
        std::process::id()
    ))
}

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

#[test]
fn delta_per_sec_handles_forward_backward_and_zero_elapsed() {
    assert_eq!(super::delta_per_sec(10, 20, 2.0), 5.0);
    assert_eq!(super::delta_per_sec(20, 10, 2.0), 0.0);
    assert_eq!(super::delta_per_sec(10, 20, 0.0), 0.0);
}

#[test]
fn read_helpers_cover_trim_bool_u64_and_whitespace_count() {
    let dir = unique_temp_dir("read-helpers");
    fs::create_dir_all(&dir).expect("create temp dir");
    let text_path = dir.join("value.txt");
    fs::write(&text_path, "  42  \n").expect("write text");
    assert_eq!(
        super::read_trimmed(Path::new(&text_path)),
        Some("42".to_string())
    );
    assert_eq!(super::read_u64_file(Path::new(&text_path)), Some(42));

    fs::write(&text_path, "on\n").expect("write bool");
    assert_eq!(super::read_bool_file(Path::new(&text_path)), Some(true));
    fs::write(&text_path, "0\n").expect("write bool false");
    assert_eq!(super::read_bool_file(Path::new(&text_path)), Some(false));

    fs::write(&text_path, "a b c\n").expect("write count");
    assert_eq!(super::read_whitespace_count(Path::new(&text_path)), 3);

    fs::remove_dir_all(&dir).expect("cleanup");
}

#[test]
fn sample_trace_pipe_parses_lines_and_continuity() {
    let dir = unique_temp_dir("trace-pipe");
    fs::create_dir_all(&dir).expect("create temp dir");
    let pipe = dir.join("trace_pipe");
    fs::write(
        &pipe,
        "task [000] .... 2.0: a\n=> symbol\ntask [000] .... 1.0: b\n",
    )
    .expect("write trace pipe");

    let sample = super::sample_trace_pipe(&dir, 16);
    assert_eq!(sample.lines.len(), 3);
    assert!(sample.kernel_stack_samples_total >= 1);
    assert!(!sample.continuity);

    fs::remove_dir_all(&dir).expect("cleanup");
}

#[test]
fn collect_event_scan_counts_enabled_categories_and_samples() {
    let dir = unique_temp_dir("event-scan");
    let events_root = dir.join("events");
    fs::create_dir_all(events_root.join("raw_syscalls/sys_enter")).expect("enter dir");
    fs::create_dir_all(events_root.join("raw_syscalls/sys_exit")).expect("exit dir");
    fs::create_dir_all(events_root.join("sched/sched_switch")).expect("sched dir");
    fs::write(events_root.join("raw_syscalls/sys_enter/enable"), "1\n").expect("enter on");
    fs::write(events_root.join("raw_syscalls/sys_exit/enable"), "1\n").expect("exit on");
    fs::write(events_root.join("sched/sched_switch/enable"), "0\n").expect("sched off");

    let scan = super::collect_event_scan(&events_root);
    assert!(scan.categories_total >= 2);
    assert!(scan.events_total >= 3);
    assert_eq!(scan.events_enabled, 2);
    assert!(scan.inventory.syscall_enter_enabled);
    assert!(scan.inventory.syscall_exit_enabled);
    assert!(scan.inventory.enabled_events_total >= 2);
    assert!(!scan.inventory.enabled_events_sample.is_empty());

    fs::remove_dir_all(&dir).expect("cleanup");
}

#[test]
fn collect_trace_loss_stats_sums_dropped_and_overrun() {
    let dir = unique_temp_dir("loss-stats");
    let per_cpu = dir.join("per_cpu");
    fs::create_dir_all(per_cpu.join("cpu0")).expect("cpu0 dir");
    fs::create_dir_all(per_cpu.join("cpu1")).expect("cpu1 dir");
    fs::write(per_cpu.join("cpu0/stats"), "dropped 3\noverrun 2\n").expect("stats0");
    fs::write(
        per_cpu.join("cpu1/stats"),
        "entries 10\nlost 4\noverrun 1\n",
    )
    .expect("stats1");

    let stats = super::collect_trace_loss_stats(&dir);
    assert_eq!(stats.dropped_events_total, 7);
    assert_eq!(stats.overrun_events_total, 3);

    fs::remove_dir_all(&dir).expect("cleanup");
}

#[test]
fn syscall_source_metadata_covers_all_variants() {
    let cases = vec![
        (super::SyscallSource::Unavailable, "unavailable", 0, 0.0),
        (
            super::SyscallSource::VmstatNrSyscalls,
            "linux_vmstat_nr_syscalls",
            1,
            1.0,
        ),
        (
            super::SyscallSource::VmstatSyscalls,
            "linux_vmstat_syscalls",
            2,
            1.0,
        ),
        (
            super::SyscallSource::ProcStatSyscalls,
            "linux_proc_stat_syscalls",
            3,
            1.0,
        ),
        (
            super::SyscallSource::ProcIoSyscrSyscw,
            "linux_proc_io_syscr_syscw",
            4,
            0.35,
        ),
    ];

    for (source, name, code, ratio) in cases {
        assert_eq!(source.as_str(), name);
        assert_eq!(source.code(), code);
        assert!((source.coverage_ratio() - ratio).abs() < f64::EPSILON);
    }
}

#[test]
fn path_and_env_helpers_cover_missing_branches() {
    let old_path = std::env::var_os("PATH");
    std::env::remove_var("PATH");
    assert!(!super::binary_exists_in_path("definitely-not-present"));

    let dir = unique_temp_dir("bin-check");
    fs::create_dir_all(&dir).expect("create dir");
    fs::write(dir.join("fakebin"), b"x").expect("write fakebin");
    std::env::set_var("PATH", dir.to_string_lossy().to_string());
    assert!(super::binary_exists_in_path("fakebin"));
    assert!(!super::binary_exists_in_path("missing"));

    std::env::remove_var("OJO_SYSTRACE_TRACE_STREAM_MAX_LINES");
    assert_eq!(super::trace_stream_line_limit(), 2048);
    std::env::set_var("OJO_SYSTRACE_TRACE_STREAM_MAX_LINES", "0");
    assert_eq!(super::trace_stream_line_limit(), 2048);
    std::env::set_var("OJO_SYSTRACE_TRACE_STREAM_MAX_LINES", "17");
    assert_eq!(super::trace_stream_line_limit(), 17);
    std::env::set_var("OJO_SYSTRACE_TRACE_STREAM_MAX_LINES", "12.bad");
    assert_eq!(super::trace_stream_line_limit(), 2048);

    if let Some(p) = old_path {
        std::env::set_var("PATH", p);
    } else {
        std::env::remove_var("PATH");
    }
    std::env::remove_var("OJO_SYSTRACE_TRACE_STREAM_MAX_LINES");
    fs::remove_dir_all(&dir).expect("cleanup");
}

#[test]
fn tracefs_config_and_write_helpers_cover_disabled_and_missing_paths() {
    std::env::set_var("OJO_SYSTRACE_AUTO_CONFIG", "off");
    let dir = unique_temp_dir("tracefs-disabled");
    fs::create_dir_all(&dir).expect("create dir");
    assert_eq!(super::maybe_configure_tracefs(&dir), 0);
    std::env::remove_var("OJO_SYSTRACE_AUTO_CONFIG");

    let missing = dir.join("missing.txt");
    assert_eq!(super::write_trimmed(missing, "x"), 0);

    let existing = dir.join("existing.txt");
    fs::write(&existing, "old").expect("seed");
    assert_eq!(super::write_trimmed(existing.clone(), "new"), 0);
    assert_eq!(fs::read_to_string(existing).expect("read"), "new");

    fs::remove_dir_all(&dir).expect("cleanup");
}

#[test]
fn parse_trace_line_seconds_token_rejects_non_numeric_dotted_tokens() {
    assert_eq!(super::parse_trace_line_seconds_token("abc.12: event"), None);
    assert_eq!(super::parse_trace_line_seconds_token("12.a3: event"), None);
}

#[test]
fn collect_event_scan_handles_missing_root_and_sample_breaks_on_invalid_utf8() {
    let missing = unique_temp_dir("no-events");
    let scan = super::collect_event_scan(&missing);
    assert!(scan.errors >= 1);

    let dir = unique_temp_dir("trace-invalid-utf8");
    fs::create_dir_all(&dir).expect("create dir");
    let pipe = dir.join("trace_pipe");
    fs::write(&pipe, [0xff, 0xfe, b'\n']).expect("write invalid utf8");

    let sample = super::sample_trace_pipe(&dir, 8);
    assert!(sample.lines.is_empty());

    fs::remove_dir_all(&dir).expect("cleanup");
}
