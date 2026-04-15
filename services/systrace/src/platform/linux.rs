use crate::SystraceSnapshot;
use std::fs::{self, OpenOptions};
use std::io::ErrorKind;
use std::io::{BufRead, BufReader};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

const DEFAULT_BUFFER_SIZE_KB: u64 = 16384;
const DEFAULT_TRACE_STREAM_LINES: usize = 2048;
const BASELINE_EVENTS: &[&str] = &[
    "sched/sched_switch",
    "sched/sched_wakeup",
    "sched/sched_wakeup_new",
    "sched/sched_migrate_task",
    "sched/sched_process_fork",
    "sched/sched_process_exec",
    "sched/sched_process_exit",
    "raw_syscalls/sys_enter",
    "raw_syscalls/sys_exit",
    "irq/irq_handler_entry",
    "irq/irq_handler_exit",
    "softirq/softirq_entry",
    "softirq/softirq_exit",
    "timer/timer_start",
    "timer/timer_expire_entry",
    "timer/timer_expire_exit",
    "exceptions/page_fault_user",
    "exceptions/page_fault_kernel",
    "block/block_rq_issue",
    "block/block_rq_complete",
    "net/netif_receive_skb",
    "net/net_dev_queue",
    "syscalls/sys_enter_openat",
    "syscalls/sys_exit_openat",
    "syscalls/sys_enter_read",
    "syscalls/sys_exit_read",
    "syscalls/sys_enter_write",
    "syscalls/sys_exit_write",
    "syscalls/sys_enter_connect",
    "syscalls/sys_exit_connect",
    "syscalls/sys_enter_accept4",
    "syscalls/sys_exit_accept4",
    "syscalls/sys_enter_epoll_wait",
    "syscalls/sys_exit_epoll_wait",
    "workqueue/workqueue_execute_start",
    "workqueue/workqueue_execute_end",
    "sched/sched_stat_runtime",
    "sched/sched_stat_wait",
    "signal/signal_generate",
    "signal/signal_deliver",
    "cgroup/cgroup_attach_task",
    "mm_vmscan/mm_vmscan_direct_reclaim_begin",
    "mm_vmscan/mm_vmscan_direct_reclaim_end",
    "tcp/tcp_retransmit_skb",
    "tcp/tcp_receive_reset",
    "futex/futex_wake",
    "futex/futex_wait",
];

const HIGH_VALUE_EVENT_CATEGORIES: &[&str] = &[
    "sched",
    "syscalls",
    "raw_syscalls",
    "irq",
    "softirq",
    "block",
    "workqueue",
    "timer",
    "mm",
    "exceptions",
    "net",
    "tcp",
    "signal",
    "cgroup",
    "task",
];

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum SyscallSource {
    #[default]
    Unavailable,
    VmstatNrSyscalls,
    VmstatSyscalls,
    ProcStatSyscalls,
    ProcIoSyscrSyscw,
}

impl SyscallSource {
    fn as_str(self) -> &'static str {
        match self {
            SyscallSource::Unavailable => "unavailable",
            SyscallSource::VmstatNrSyscalls => "linux_vmstat_nr_syscalls",
            SyscallSource::VmstatSyscalls => "linux_vmstat_syscalls",
            SyscallSource::ProcStatSyscalls => "linux_proc_stat_syscalls",
            SyscallSource::ProcIoSyscrSyscw => "linux_proc_io_syscr_syscw",
        }
    }

    fn code(self) -> u64 {
        match self {
            SyscallSource::Unavailable => 0,
            SyscallSource::VmstatNrSyscalls => 1,
            SyscallSource::VmstatSyscalls => 2,
            SyscallSource::ProcStatSyscalls => 3,
            SyscallSource::ProcIoSyscrSyscw => 4,
        }
    }

    fn coverage_ratio(self) -> f64 {
        match self {
            SyscallSource::Unavailable => 0.0,
            SyscallSource::VmstatNrSyscalls => 1.0,
            SyscallSource::VmstatSyscalls => 1.0,
            SyscallSource::ProcStatSyscalls => 1.0,
            // Approximate fallback that captures process I/O syscalls only.
            SyscallSource::ProcIoSyscrSyscw => 0.35,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct KernelCounters {
    context_switches_total: u64,
    interrupts_total: u64,
    forks_total: u64,
    syscalls_total: u64,
    run_queue_depth: f64,
    syscall_source: SyscallSource,
}

#[derive(Clone, Copy, Debug)]
struct RateState {
    at: Instant,
    counters: KernelCounters,
}

static RATE_STATE: OnceLock<Mutex<Option<RateState>>> = OnceLock::new();

#[derive(Default)]
struct TraceSample {
    lines: Vec<String>,
    kernel_stack_samples_total: u64,
    user_stack_samples_total: u64,
    continuity: bool,
}

#[derive(Default)]
struct EventInventory {
    enabled_events_total: u64,
    enabled_events_sample: Vec<String>,
    syscall_enter_enabled: bool,
    syscall_exit_enabled: bool,
    high_value_categories_targeted: u64,
    high_value_categories_enabled: u64,
}

#[derive(Default)]
struct TraceLossStats {
    dropped_events_total: u64,
    overrun_events_total: u64,
}

#[derive(Default)]
struct EventScan {
    events_total: u64,
    events_enabled: u64,
    categories_total: u64,
    errors: u64,
    inventory: EventInventory,
}

pub(crate) fn collect_snapshot() -> SystraceSnapshot {
    let mut snap = SystraceSnapshot::default();
    let Some(tracefs_root) = detect_tracefs_root() else {
        return snap;
    };

    let setup_errors = maybe_configure_tracefs(&tracefs_root);

    snap.available = true;
    snap.privileged_mode = is_root();
    snap.ebpf_available = detect_ebpf_available();
    snap.uprobes_available = tracefs_root.join("uprobe_events").exists();
    snap.usdt_available = detect_usdt_available();
    snap.symbolizer_available = detect_symbolizer_available();
    snap.tracefs_available = true;
    snap.tracing_on = read_bool_file(&tracefs_root.join("tracing_on")).unwrap_or(false);
    snap.current_tracer = read_trimmed(&tracefs_root.join("current_tracer")).unwrap_or_default();
    snap.tracers_available = read_whitespace_count(&tracefs_root.join("available_tracers"));
    snap.buffer_total_kb = read_u64_file(&tracefs_root.join("buffer_total_size_kb")).unwrap_or(0);

    let event_scan = collect_event_scan(&tracefs_root.join("events"));
    snap.events_total = event_scan.events_total;
    snap.events_enabled = event_scan.events_enabled;
    snap.event_categories_total = event_scan.categories_total;
    snap.enabled_events_inventory_total = event_scan.inventory.enabled_events_total;
    snap.enabled_events_inventory_sample = event_scan.inventory.enabled_events_sample;
    snap.syscall_enter_enabled = event_scan.inventory.syscall_enter_enabled;
    snap.syscall_exit_enabled = event_scan.inventory.syscall_exit_enabled;
    snap.high_value_categories_targeted = event_scan.inventory.high_value_categories_targeted;
    snap.high_value_categories_enabled = event_scan.inventory.high_value_categories_enabled;

    let trace_sample = sample_trace_pipe(&tracefs_root, trace_stream_line_limit());
    snap.trace_sample_lines_total = trace_sample.lines.len() as u64;
    snap.trace_stream_lines_captured_total = trace_sample.lines.len() as u64;
    snap.trace_stream_continuity = trace_sample.continuity;
    snap.trace_sample = trace_sample.lines;
    snap.kernel_stack_samples_total = trace_sample.kernel_stack_samples_total;
    snap.user_stack_samples_total = trace_sample.user_stack_samples_total;

    let loss = collect_trace_loss_stats(&tracefs_root);
    snap.trace_dropped_events_total = loss.dropped_events_total;
    snap.trace_overrun_events_total = loss.overrun_events_total;

    let counters = read_kernel_counters();
    let rates = derive_rates(counters);
    snap.context_switches_per_sec = rates.context_switches_per_sec;
    snap.interrupts_per_sec = rates.interrupts_per_sec;
    snap.process_forks_per_sec = rates.process_forks_per_sec;
    snap.system_calls_per_sec = rates.system_calls_per_sec;
    snap.system_calls_source = counters.syscall_source.as_str().to_string();
    snap.system_calls_source_code = counters.syscall_source.code();
    snap.system_calls_coverage_ratio = counters.syscall_source.coverage_ratio();
    snap.run_queue_depth = counters.run_queue_depth;
    let (processes_total, threads_total) = count_process_and_thread_inventory();
    snap.processes_total = processes_total;
    snap.threads_total = threads_total;
    snap.dpcs_per_sec = 0.0;
    snap.collection_errors = event_scan.errors.saturating_add(setup_errors);
    snap
}

fn is_root() -> bool {
    // SAFETY: geteuid has no side effects and does not require invariants beyond libc linkage.
    unsafe { libc::geteuid() == 0 }
}

fn detect_ebpf_available() -> bool {
    Path::new("/sys/fs/bpf").exists() && binary_exists_in_path("bpftool")
}

fn detect_usdt_available() -> bool {
    // bpftrace/perf provide practical user-space probe attachment points.
    binary_exists_in_path("bpftrace") || binary_exists_in_path("perf")
}

fn detect_symbolizer_available() -> bool {
    binary_exists_in_path("addr2line") || binary_exists_in_path("llvm-symbolizer")
}

fn binary_exists_in_path(name: &str) -> bool {
    let Some(path_var) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&path_var).any(|dir| {
        let candidate = dir.join(name);
        candidate.is_file()
    })
}

fn trace_stream_line_limit() -> usize {
    std::env::var("OJO_SYSTRACE_TRACE_STREAM_MAX_LINES")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_TRACE_STREAM_LINES)
}

fn maybe_configure_tracefs(tracefs_root: &Path) -> u64 {
    if !std::env::var("OJO_SYSTRACE_AUTO_CONFIG")
        .map(|v| {
            !matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "off"
            )
        })
        .unwrap_or(true)
    {
        return 0;
    }

    static TRACE_SETUP_DONE: OnceLock<u64> = OnceLock::new();
    *TRACE_SETUP_DONE.get_or_init(|| configure_tracefs_baseline(tracefs_root))
}

fn configure_tracefs_baseline(tracefs_root: &Path) -> u64 {
    let mut errors = 0u64;

    errors = errors.saturating_add(write_trimmed(tracefs_root.join("tracing_on"), "0"));

    errors = errors.saturating_add(write_trimmed(
        tracefs_root.join("buffer_size_kb"),
        &DEFAULT_BUFFER_SIZE_KB.to_string(),
    ));

    errors = errors.saturating_add(write_trimmed(tracefs_root.join("events/enable"), "0"));

    for event in BASELINE_EVENTS {
        let path = tracefs_root.join("events").join(event).join("enable");
        errors = errors.saturating_add(write_trimmed(path, "1"));
    }

    // These options improve stack visibility in trace_pipe lines when supported by the kernel.
    errors = errors.saturating_add(write_trimmed(tracefs_root.join("options/stacktrace"), "1"));
    errors = errors.saturating_add(write_trimmed(
        tracefs_root.join("options/userstacktrace"),
        "1",
    ));

    errors = errors.saturating_add(write_trimmed(tracefs_root.join("tracing_on"), "1"));
    errors
}

fn write_trimmed(path: PathBuf, value: &str) -> u64 {
    if !path.exists() {
        return 0;
    }
    fs::write(path, value).map(|_| 0).unwrap_or(1)
}

#[derive(Clone, Copy, Debug, Default)]
struct DerivedRates {
    context_switches_per_sec: f64,
    interrupts_per_sec: f64,
    process_forks_per_sec: f64,
    system_calls_per_sec: f64,
}

fn derive_rates(counters: KernelCounters) -> DerivedRates {
    let state = RATE_STATE.get_or_init(|| Mutex::new(None));
    let now = Instant::now();
    let mut guard = match state.lock() {
        Ok(g) => g,
        Err(_) => return DerivedRates::default(),
    };

    let rates = if let Some(prev) = *guard {
        let elapsed = now.duration_since(prev.at).as_secs_f64();
        if elapsed > 0.0 {
            DerivedRates {
                context_switches_per_sec: delta_per_sec(
                    prev.counters.context_switches_total,
                    counters.context_switches_total,
                    elapsed,
                ),
                interrupts_per_sec: delta_per_sec(
                    prev.counters.interrupts_total,
                    counters.interrupts_total,
                    elapsed,
                ),
                process_forks_per_sec: delta_per_sec(
                    prev.counters.forks_total,
                    counters.forks_total,
                    elapsed,
                ),
                system_calls_per_sec: delta_per_sec(
                    prev.counters.syscalls_total,
                    counters.syscalls_total,
                    elapsed,
                ),
            }
        } else {
            DerivedRates::default()
        }
    } else {
        bootstrap_rates(counters, read_uptime_secs().unwrap_or(0.0))
    };

    *guard = Some(RateState { at: now, counters });
    rates
}

fn bootstrap_rates(counters: KernelCounters, uptime_secs: f64) -> DerivedRates {
    if uptime_secs <= 0.0 {
        return DerivedRates::default();
    }
    DerivedRates {
        context_switches_per_sec: (counters.context_switches_total as f64) / uptime_secs,
        interrupts_per_sec: (counters.interrupts_total as f64) / uptime_secs,
        process_forks_per_sec: (counters.forks_total as f64) / uptime_secs,
        system_calls_per_sec: (counters.syscalls_total as f64) / uptime_secs,
    }
}

fn read_uptime_secs() -> Option<f64> {
    let raw = fs::read_to_string("/proc/uptime").ok()?;
    let first = raw.split_whitespace().next()?;
    first.parse::<f64>().ok()
}

fn delta_per_sec(prev: u64, curr: u64, elapsed: f64) -> f64 {
    if curr < prev || elapsed <= 0.0 {
        return 0.0;
    }
    (curr.saturating_sub(prev) as f64) / elapsed
}

fn read_kernel_counters() -> KernelCounters {
    let mut counters = KernelCounters::default();

    if let Ok(stat) = fs::read_to_string("/proc/stat") {
        counters = parse_proc_stat_content(&stat);
    }

    if let Ok(vmstat) = fs::read_to_string("/proc/vmstat") {
        if let Some((syscalls_total, source)) = parse_vmstat_syscalls_total(&vmstat) {
            counters.syscalls_total = syscalls_total;
            counters.syscall_source = source;
        }
    }

    if matches!(counters.syscall_source, SyscallSource::Unavailable) {
        if let Some(io_syscalls_total) = aggregate_proc_io_syscalls_total() {
            counters.syscalls_total = io_syscalls_total;
            counters.syscall_source = SyscallSource::ProcIoSyscrSyscw;
        }
    }

    counters
}

fn parse_proc_stat_content(stat: &str) -> KernelCounters {
    let mut counters = KernelCounters::default();
    for line in stat.lines() {
        let mut parts = line.split_whitespace();
        let Some(key) = parts.next() else {
            continue;
        };
        match key {
            "ctxt" => {
                counters.context_switches_total = parts
                    .next()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0);
            }
            "intr" => {
                counters.interrupts_total = parts
                    .next()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0);
            }
            "processes" => {
                counters.forks_total = parts
                    .next()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0);
            }
            "procs_running" => {
                counters.run_queue_depth = parts
                    .next()
                    .and_then(|v| v.parse::<f64>().ok())
                    .unwrap_or(0.0);
            }
            "syscalls" => {
                counters.syscalls_total = parts
                    .next()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0);
                counters.syscall_source = SyscallSource::ProcStatSyscalls;
            }
            _ => {}
        }
    }
    counters
}

fn parse_vmstat_syscalls_total(vmstat: &str) -> Option<(u64, SyscallSource)> {
    let mut nr_syscalls = None;
    let mut syscalls = None;
    for line in vmstat.lines() {
        let mut parts = line.split_whitespace();
        let Some(key) = parts.next() else {
            continue;
        };
        let value = parts.next().and_then(|v| v.parse::<u64>().ok());
        match key {
            "nr_syscalls" => nr_syscalls = value,
            "syscalls" => syscalls = value,
            _ => {}
        }
    }

    if let Some(value) = nr_syscalls {
        return Some((value, SyscallSource::VmstatNrSyscalls));
    }
    syscalls.map(|value| (value, SyscallSource::VmstatSyscalls))
}

fn parse_proc_io_syscalls(content: &str) -> Option<u64> {
    let mut syscr = None;
    let mut syscw = None;
    for line in content.lines() {
        let mut parts = line.split_whitespace();
        let Some(key) = parts.next() else {
            continue;
        };
        let value = parts.next().and_then(|v| v.parse::<u64>().ok());
        match key {
            "syscr:" => syscr = value,
            "syscw:" => syscw = value,
            _ => {}
        }
    }

    match (syscr, syscw) {
        (None, None) => None,
        _ => Some(syscr.unwrap_or(0).saturating_add(syscw.unwrap_or(0))),
    }
}

fn aggregate_proc_io_syscalls_total() -> Option<u64> {
    let entries = fs::read_dir("/proc").ok()?;
    let mut total = 0u64;
    let mut observed = false;

    for entry in entries.flatten() {
        let name = entry.file_name();
        let pid_text = name.to_string_lossy();
        if !pid_text.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        let path = entry.path().join("io");
        let Ok(io_text) = fs::read_to_string(path) else {
            continue;
        };
        if let Some(pid_total) = parse_proc_io_syscalls(&io_text) {
            total = total.saturating_add(pid_total);
            observed = true;
        }
    }

    if observed {
        Some(total)
    } else {
        None
    }
}

fn count_process_and_thread_inventory() -> (u64, u64) {
    let mut process_count = 0u64;
    let mut thread_count = 0u64;

    let entries = match fs::read_dir("/proc") {
        Ok(entries) => entries,
        Err(_) => return (0, 0),
    };

    for entry in entries.flatten() {
        let name = entry.file_name();
        let pid_text = name.to_string_lossy();
        if !pid_text.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        process_count = process_count.saturating_add(1);
        let tasks_dir = entry.path().join("task");
        if let Ok(tasks) = fs::read_dir(tasks_dir) {
            let count = tasks.flatten().count() as u64;
            thread_count = thread_count.saturating_add(count.max(1));
        } else {
            thread_count = thread_count.saturating_add(1);
        }
    }

    (process_count, thread_count)
}

fn detect_tracefs_root() -> Option<PathBuf> {
    for candidate in ["/sys/kernel/tracing", "/sys/kernel/debug/tracing"] {
        let path = Path::new(candidate);
        if path.exists() {
            return Some(path.to_path_buf());
        }
    }

    if let Some(root) = detect_tracefs_from_mounts() {
        return Some(root);
    }

    None
}

fn detect_tracefs_from_mounts() -> Option<PathBuf> {
    let mounts = fs::read_to_string("/proc/mounts").ok()?;
    for line in mounts.lines() {
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() < 3 {
            continue;
        }
        let fstype = fields[2];
        if fstype == "tracefs" {
            return Some(PathBuf::from(fields[1]));
        }
    }
    None
}

fn read_trimmed(path: &Path) -> Option<String> {
    fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn read_bool_file(path: &Path) -> Option<bool> {
    let raw = read_trimmed(path)?;
    Some(matches!(raw.as_str(), "1" | "true" | "on"))
}

fn read_u64_file(path: &Path) -> Option<u64> {
    let raw = read_trimmed(path)?;
    raw.parse::<u64>().ok()
}

fn read_whitespace_count(path: &Path) -> u64 {
    read_trimmed(path)
        .map(|raw| raw.split_whitespace().count() as u64)
        .unwrap_or(0)
}

fn count_stack_sample_markers(line: &str) -> (u64, u64) {
    let text = line.to_ascii_lowercase();
    let kernel = u64::from(
        text.contains("kernel_stack") || text.contains("stacktrace") || text.contains("=>"),
    );
    let user = u64::from(
        text.contains("user_stack") || text.contains("userspace stack") || text.contains("ustack"),
    );
    (kernel, user)
}

fn sample_trace_pipe(tracefs_root: &Path, max_lines: usize) -> TraceSample {
    let path = tracefs_root.join("trace_pipe");
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(&path);
    let file = match file {
        Ok(f) => f,
        Err(_) => return TraceSample::default(),
    };

    let reader = BufReader::new(file);
    let mut sample = TraceSample::default();
    let mut last_ts = None;
    let mut continuity = true;
    for line in reader.lines().take(max_lines) {
        match line {
            Ok(text) if !text.trim().is_empty() => {
                if let Some(ts) = parse_trace_line_seconds_token(&text) {
                    if let Some(prev) = last_ts {
                        if ts < prev {
                            continuity = false;
                        }
                    }
                    last_ts = Some(ts);
                }
                let (kernel, user) = count_stack_sample_markers(&text);
                sample.kernel_stack_samples_total =
                    sample.kernel_stack_samples_total.saturating_add(kernel);
                sample.user_stack_samples_total =
                    sample.user_stack_samples_total.saturating_add(user);
                sample.lines.push(text);
            }
            _ => break,
        }
    }
    sample.continuity = continuity;
    sample
}

fn parse_trace_line_seconds_token(line: &str) -> Option<f64> {
    line.split_whitespace().find_map(|part| {
        let candidate = part.trim_end_matches(':');
        if !candidate.contains('.') {
            return None;
        }
        if !candidate.chars().any(|ch| ch.is_ascii_digit()) {
            return None;
        }
        if !candidate.chars().all(|ch| ch.is_ascii_digit() || ch == '.') {
            return None;
        }
        candidate.parse::<f64>().ok()
    })
}

fn collect_event_scan(events_root: &Path) -> EventScan {
    let mut scan = EventScan {
        inventory: EventInventory {
            high_value_categories_targeted: HIGH_VALUE_EVENT_CATEGORIES.len() as u64,
            ..EventInventory::default()
        },
        ..EventScan::default()
    };

    let categories_dir = match fs::read_dir(events_root) {
        Ok(it) => it,
        Err(err) => {
            if !matches!(err.kind(), ErrorKind::PermissionDenied) {
                scan.errors = scan.errors.saturating_add(1);
            }
            return scan;
        }
    };

    let mut seen_high_value = std::collections::BTreeSet::new();
    let mut enabled_sample = Vec::new();

    for category in categories_dir.flatten() {
        let category_path = category.path();
        if !category_path.is_dir() {
            continue;
        }
        scan.categories_total = scan.categories_total.saturating_add(1);
        let category_name = category.file_name().to_string_lossy().to_string();

        let events = match fs::read_dir(&category_path) {
            Ok(it) => it,
            Err(err) => {
                if !matches!(err.kind(), ErrorKind::PermissionDenied) {
                    scan.errors = scan.errors.saturating_add(1);
                }
                continue;
            }
        };

        for event in events.flatten() {
            let event_path = event.path();
            if !event_path.is_dir() {
                continue;
            }
            scan.events_total = scan.events_total.saturating_add(1);
            match fs::read_to_string(event_path.join("enable")) {
                Ok(value) => {
                    let enabled = matches!(value.trim(), "1" | "Y" | "y");
                    if !enabled {
                        continue;
                    }
                    scan.events_enabled = scan.events_enabled.saturating_add(1);
                    scan.inventory.enabled_events_total =
                        scan.inventory.enabled_events_total.saturating_add(1);

                    let event_name = event.file_name().to_string_lossy().to_string();
                    if enabled_sample.len() < 512 {
                        enabled_sample.push(format!("{category_name}/{event_name}"));
                    }
                    if category_name == "raw_syscalls" || category_name == "syscalls" {
                        if event_name.contains("sys_enter") {
                            scan.inventory.syscall_enter_enabled = true;
                        }
                        if event_name.contains("sys_exit") {
                            scan.inventory.syscall_exit_enabled = true;
                        }
                    }
                    if HIGH_VALUE_EVENT_CATEGORIES.contains(&category_name.as_str()) {
                        seen_high_value.insert(category_name.clone());
                    }
                }
                Err(err) => {
                    if !matches!(
                        err.kind(),
                        ErrorKind::PermissionDenied | ErrorKind::NotFound
                    ) {
                        scan.errors = scan.errors.saturating_add(1);
                    }
                }
            }
        }
    }

    scan.inventory.high_value_categories_enabled = seen_high_value.len() as u64;
    scan.inventory.enabled_events_sample = enabled_sample;
    scan
}

fn collect_trace_loss_stats(tracefs_root: &Path) -> TraceLossStats {
    let mut stats = TraceLossStats::default();
    let per_cpu_root = tracefs_root.join("per_cpu");
    let entries = match fs::read_dir(per_cpu_root) {
        Ok(it) => it,
        Err(_) => return stats,
    };

    for cpu in entries.flatten() {
        let cpu_path = cpu.path();
        if !cpu_path.is_dir() {
            continue;
        }
        let stats_file = cpu_path.join("stats");
        let Ok(content) = fs::read_to_string(stats_file) else {
            continue;
        };
        let mut overrun_seen = 0u64;
        for line in content.lines() {
            let mut parts = line.split_whitespace();
            let Some(key) = parts.next() else {
                continue;
            };
            let value = parts
                .next()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
            if key.contains("dropped") || key.contains("lost") {
                stats.dropped_events_total = stats.dropped_events_total.saturating_add(value);
            }
            if key.contains("overrun") {
                overrun_seen = overrun_seen.saturating_add(value);
            }
        }
        stats.overrun_events_total = stats.overrun_events_total.saturating_add(overrun_seen);
    }

    stats
}

#[cfg(test)]
#[path = "../tests/platform_linux_tests.rs"]
mod tests;
