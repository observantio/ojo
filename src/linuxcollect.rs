use crate::model::{
    CpuInfoSnapshot, CpuTimes, CpuTimesSeconds, DiskSnapshot, LoadSnapshot, MemorySnapshot,
    MountSnapshot, NetDevSnapshot, ProcessSnapshot, Snapshot, SoftnetCpuSnapshot,
    SwapDeviceSnapshot, SystemSnapshot,
};
use anyhow::Result;
use procfs::{process::all_processes, Current, CurrentSI};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::CString;
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

const PROCESS_FD_SCAN_LIMIT: usize = 1024;
const CGROUP_MAX_DIRS: usize = 4096;
const CGROUP_MAX_DEPTH: usize = 24;

const DEFAULT_INCLUDE_PSEUDO_FS: bool = false;
const DEFAULT_INCLUDE_VIRTUAL_NET: bool = false;

#[derive(Default)]
struct ReadCache {
    files: HashMap<PathBuf, Option<String>>,
}

impl ReadCache {
    fn read_raw(&mut self, path: impl AsRef<Path>) -> Option<&str> {
        let path = path.as_ref().to_path_buf();
        if self.files.contains_key(&path) {
            return self.files.get(&path).and_then(|v| v.as_deref());
        }

        let value = fs::read_to_string(&path).ok();
        self.files.insert(path.clone(), value);
        self.files.get(&path).and_then(|v| v.as_deref())
    }

    fn read_trimmed(&mut self, path: impl AsRef<Path>) -> Option<&str> {
        self.read_raw(path)
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
    }

    fn read_u64(&mut self, path: impl AsRef<Path>) -> Option<u64> {
        self.read_trimmed(path)?.parse().ok()
    }

    fn read_i64_first(&mut self, path: impl AsRef<Path>) -> Option<i64> {
        self.read_raw(path)?.split_whitespace().next()?.parse().ok()
    }

    fn read_bool_num(&mut self, path: impl AsRef<Path>) -> Option<bool> {
        match self.read_trimmed(path)? {
            "0" => Some(false),
            "1" => Some(true),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
enum CgroupMode {
    None,
    V1,
    V2,
    Hybrid,
}

impl CgroupMode {
    fn as_str(self) -> &'static str {
        match self {
            CgroupMode::None => "unsupported",
            CgroupMode::V1 => "v1",
            CgroupMode::V2 => "v2",
            CgroupMode::Hybrid => "hybrid",
        }
    }
}

struct ProcessCollectionMeta {
    fd_scan_enabled: bool,
}

struct LinuxSupportInputs<'a> {
    cgroup_mode: CgroupMode,
    psi_supported: bool,
    psi_irq_supported: bool,
    schedstat_supported: bool,
    process_meta: Option<&'a ProcessCollectionMeta>,
    in_container: bool,
    system_ok: bool,
    memory_ok: bool,
    process_ok: bool,
}

fn linux_support_state(inputs: LinuxSupportInputs<'_>) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    out.insert(
        "process.network.usage".to_string(),
        "unsupported_without_ebpf_or_etw".to_string(),
    );
    out.insert(
        "system.linux.cgroup.mode".to_string(),
        inputs.cgroup_mode.as_str().to_string(),
    );
    out.insert(
        "system.linux.pressure".to_string(),
        if inputs.psi_supported {
            "available".to_string()
        } else {
            "unsupported_or_disabled".to_string()
        },
    );
    out.insert(
        "system.linux.pressure.irq".to_string(),
        if inputs.psi_irq_supported {
            "available_when_kernel_exports_irq_psi".to_string()
        } else {
            "not_available_on_this_kernel".to_string()
        },
    );
    out.insert(
        "system.linux.schedstat".to_string(),
        if inputs.schedstat_supported {
            "best_effort_kernel_version_tolerant".to_string()
        } else {
            "unsupported_or_unreadable".to_string()
        },
    );
    out.insert(
        "system.linux.runqueue.depth".to_string(),
        "heuristic_estimate_from_loadavg_with_schedstat_weighting_when_available".to_string(),
    );
    out.insert(
        "system.linux.namespace.scope".to_string(),
        if inputs.in_container {
            "container_or_restricted_namespace_heuristic".to_string()
        } else {
            "host_or_host_like_namespace_heuristic".to_string()
        },
    );
    out.insert(
        "system.linux.namespace.scope.confidence".to_string(),
        "heuristic_signal_not_ground_truth".to_string(),
    );
    out.insert(
        "schema.key_format".to_string(),
        "mixed_dot_and_pipe_legacy".to_string(),
    );
    out.insert(
        "snapshot.field.cgroup".to_string(),
        "contains_v1_v2_metrics_when_available".to_string(),
    );
    out.insert(
        "snapshot.field.cgroup_v2".to_string(),
        "deprecated_alias_for_snapshot.field.cgroup".to_string(),
    );
    out.insert(
        "system.disk.sectors".to_string(),
        "reported_as_sectors_with_device_logical_block_size_for_conversion".to_string(),
    );
    out.insert(
        "snapshot.core.system".to_string(),
        if inputs.system_ok {
            "collected".to_string()
        } else {
            "fallback_default_due_to_collection_failure".to_string()
        },
    );
    out.insert(
        "snapshot.core.memory".to_string(),
        if inputs.memory_ok {
            "collected".to_string()
        } else {
            "fallback_default_due_to_collection_failure".to_string()
        },
    );
    out.insert(
        "snapshot.processes".to_string(),
        if inputs.process_ok {
            "collected_or_disabled_by_config".to_string()
        } else {
            "fallback_empty_due_to_collection_failure".to_string()
        },
    );

    if let Some(meta) = inputs.process_meta {
        out.insert(
            "process.open_file_descriptors.collection".to_string(),
            if meta.fd_scan_enabled {
                "counted_from_proc_pid_fd".to_string()
            } else {
                "disabled_for_scale_fd_count_omitted".to_string()
            },
        );
    }

    out
}

fn is_likely_containerized() -> bool {
    if Path::new("/.dockerenv").exists() || Path::new("/run/.containerenv").exists() {
        return true;
    }

    let Ok(cgroup) = fs::read_to_string("/proc/1/cgroup") else {
        return false;
    };

    cgroup.lines().any(|line| {
        line.contains("docker")
            || line.contains("kubepods")
            || line.contains("containerd")
            || line.contains("podman")
    })
}

fn parse_u64_with_max_flag(value: &str) -> (Option<u64>, bool) {
    if value == "max" {
        (None, true)
    } else {
        (value.parse::<u64>().ok(), false)
    }
}

fn env_bool(name: &str, default_value: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| {
            let n = v.trim().to_ascii_lowercase();
            matches!(n.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(default_value)
}

fn include_pseudo_filesystems() -> bool {
    env_bool("OJO_LINUX_INCLUDE_PSEUDO_FS", DEFAULT_INCLUDE_PSEUDO_FS)
}

fn include_virtual_interfaces() -> bool {
    env_bool(
        "OJO_LINUX_INCLUDE_VIRTUAL_INTERFACES",
        DEFAULT_INCLUDE_VIRTUAL_NET,
    )
}

fn include_raw_cgroup_paths() -> bool {
    env_bool("OJO_LINUX_INCLUDE_RAW_CGROUP_PATHS", false)
}

fn is_pseudo_filesystem(fs_type: &str) -> bool {
    matches!(
        fs_type,
        "proc"
            | "sysfs"
            | "tmpfs"
            | "devtmpfs"
            | "cgroup"
            | "cgroup2"
            | "securityfs"
            | "pstore"
            | "debugfs"
            | "tracefs"
            | "configfs"
            | "overlay"
            | "nsfs"
            | "autofs"
            | "squashfs"
            | "rpc_pipefs"
            | "fusectl"
            | "mqueue"
            | "hugetlbfs"
    )
}

fn is_noise_interface(name: &str) -> bool {
    [
        "veth", "br-", "docker", "cni", "flannel", "virbr", "vnet", "tun", "tap",
    ]
    .iter()
    .any(|prefix| name.starts_with(prefix))
}

fn normalize_cgroup_scope(scope: &str) -> String {
    if include_raw_cgroup_paths() {
        return scope.to_string();
    }

    let mut out = Vec::new();
    for part in scope.split('/') {
        if part.is_empty() {
            continue;
        }
        if out.len() >= 4 {
            out.push("...".to_string());
            break;
        }

        let looks_dynamic = part.len() > 24
            || part.chars().all(|c| c.is_ascii_hexdigit())
            || part.chars().filter(|c| *c == '-').count() >= 3
            || part.chars().all(|c| c.is_ascii_digit());

        if looks_dynamic {
            out.push("{id}".to_string());
        } else {
            out.push(part.to_string());
        }
    }

    if out.is_empty() {
        "root".to_string()
    } else {
        out.join("/")
    }
}

fn unescape_mount_field(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = String::with_capacity(raw.len());
    let mut i = 0usize;

    while i < bytes.len() {
        if bytes[i] == b'\\'
            && i + 3 < bytes.len()
            && bytes[i + 1].is_ascii_digit()
            && bytes[i + 2].is_ascii_digit()
            && bytes[i + 3].is_ascii_digit()
        {
            let octal = &raw[i + 1..i + 4];
            if let Ok(v) = u8::from_str_radix(octal, 8) {
                out.push(v as char);
                i += 4;
                continue;
            }
        }

        out.push(bytes[i] as char);
        i += 1;
    }

    out
}

fn read_primary_interfaces() -> HashSet<String> {
    let mut out = HashSet::new();

    if let Ok(contents) = fs::read_to_string("/proc/net/route") {
        for line in contents.lines().skip(1) {
            let cols = line.split_whitespace().collect::<Vec<_>>();
            if cols.len() < 4 {
                continue;
            }

            let iface = cols[0];
            let destination = cols[1];
            let flags = u32::from_str_radix(cols[3], 16).unwrap_or(0);
            let up = flags & 0x1 != 0;
            if destination == "00000000" && up {
                out.insert(iface.to_string());
            }
        }
    }

    if let Ok(contents) = fs::read_to_string("/proc/net/ipv6_route") {
        for line in contents.lines() {
            let cols = line.split_whitespace().collect::<Vec<_>>();
            if cols.len() < 10 {
                continue;
            }

            let dst = cols[0];
            let dst_prefix_len = cols[1];
            let iface = cols[9];
            if dst == "00000000000000000000000000000000" && dst_prefix_len == "00000000" {
                out.insert(iface.to_string());
            }
        }
    }

    out
}

fn linux_metric_classification() -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();

    out.insert("system.cpu.time".to_string(), "counter".to_string());
    out.insert(
        "system.cpu.utilization".to_string(),
        "gauge_derived_ratio".to_string(),
    );

    out.insert(
        "system.cpu.load_average.1m".to_string(),
        "gauge".to_string(),
    );
    out.insert(
        "system.cpu.load_average.5m".to_string(),
        "gauge".to_string(),
    );
    out.insert(
        "system.cpu.load_average.15m".to_string(),
        "gauge".to_string(),
    );
    out.insert(
        "system.linux.load.runnable".to_string(),
        "gauge".to_string(),
    );
    out.insert(
        "system.linux.load.entities".to_string(),
        "gauge".to_string(),
    );
    out.insert(
        "system.linux.load.latest_pid".to_string(),
        "gauge".to_string(),
    );

    out.insert("system.memory.*".to_string(), "gauge".to_string());
    out.insert("system.swap.*".to_string(), "gauge".to_string());

    out.insert(
        "system.linux.pressure".to_string(),
        "gauge_ratio".to_string(),
    );
    out.insert(
        "system.linux.pressure.stall_time".to_string(),
        "counter".to_string(),
    );

    out.insert("system.context_switches".to_string(), "counter".to_string());
    out.insert("system.linux.interrupts".to_string(), "counter".to_string());
    out.insert("system.linux.softirqs".to_string(), "counter".to_string());

    out.insert("system.linux.vmstat".to_string(), "gauge".to_string());

    out.insert("system.disk.io".to_string(), "counter".to_string());
    out.insert("system.disk.operations".to_string(), "counter".to_string());
    out.insert(
        "system.disk.operation_time".to_string(),
        "counter".to_string(),
    );
    out.insert("system.disk.io_time".to_string(), "counter".to_string());
    out.insert(
        "system.disk.pending_operations".to_string(),
        "gauge".to_string(),
    );
    out.insert(
        "system.disk.utilization".to_string(),
        "gauge_derived_ratio".to_string(),
    );
    out.insert("system.disk.queue_depth".to_string(), "gauge".to_string());
    out.insert(
        "system.disk.*_per_sec".to_string(),
        "gauge_derived".to_string(),
    );
    out.insert("system.disk.*".to_string(), "gauge".to_string());

    out.insert("system.network.io".to_string(), "counter".to_string());
    out.insert("system.network.packets".to_string(), "counter".to_string());
    out.insert("system.network.errors".to_string(), "counter".to_string());
    out.insert("system.network.dropped".to_string(), "counter".to_string());
    out.insert(
        "system.network.*_per_sec".to_string(),
        "gauge_derived".to_string(),
    );
    out.insert("system.network.*".to_string(), "gauge".to_string());

    out.insert("system.linux.net.snmp".to_string(), "counter".to_string());
    out.insert("system.linux.netstat".to_string(), "counter".to_string());

    out.insert("system.linux.schedstat".to_string(), "counter".to_string());
    out.insert(
        "system.linux.runqueue.depth".to_string(),
        "gauge_approximation".to_string(),
    );

    out.insert("system.linux.slab".to_string(), "gauge".to_string());
    out.insert("system.linux.cgroup".to_string(), "gauge".to_string());

    out.insert("system.filesystem".to_string(), "gauge".to_string());

    out.insert("process.cpu.time".to_string(), "counter".to_string());
    out.insert("process.disk.io".to_string(), "counter".to_string());
    out.insert("process.io.chars".to_string(), "counter".to_string());
    out.insert("process.io.syscalls".to_string(), "counter".to_string());
    out.insert(
        "process.context_switches".to_string(),
        "counter".to_string(),
    );
    out.insert("process.paging.faults".to_string(), "counter".to_string());
    out.insert("process.memory.usage".to_string(), "gauge".to_string());
    out.insert(
        "process.open_file_descriptors".to_string(),
        "gauge".to_string(),
    );
    out.insert("process.oom_score".to_string(), "gauge".to_string());
    out.insert("process.cpu.last_id".to_string(), "gauge".to_string());
    out.insert("process.start_time".to_string(), "gauge".to_string());
    out.insert("process.linux.start_time".to_string(), "gauge".to_string());
    out.insert("process.linux.scheduler".to_string(), "gauge".to_string());

    out.insert("system.mounts.*".to_string(), "state".to_string());
    out.insert("system.cpuinfo.*".to_string(), "inventory".to_string());
    out.insert("system.zoneinfo.*".to_string(), "gauge".to_string());
    out.insert("system.buddyinfo.*".to_string(), "gauge".to_string());

    out
}

fn u64_to_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

fn key_dot2(a: &str, b: &str) -> String {
    let mut key = String::with_capacity(a.len() + b.len() + 1);
    key.push_str(a);
    key.push('.');
    key.push_str(b);
    key
}

fn key_dot3(a: &str, b: &str, c: &str) -> String {
    let mut key = String::with_capacity(a.len() + b.len() + c.len() + 2);
    key.push_str(a);
    key.push('.');
    key.push_str(b);
    key.push('.');
    key.push_str(c);
    key
}

fn key_pipe2(a: &str, b: usize) -> String {
    let b = b.to_string();
    let mut key = String::with_capacity(a.len() + b.len() + 1);
    key.push_str(a);
    key.push('|');
    key.push_str(&b);
    key
}

fn key_pipe3(a: &str, b: &str, c: impl ToString) -> String {
    let c = c.to_string();
    let mut key = String::with_capacity(a.len() + b.len() + c.len() + 2);
    key.push_str(a);
    key.push('|');
    key.push_str(b);
    key.push('|');
    key.push_str(&c);
    key
}

fn key_pipe4(a: &str, b: &str, c: &str, d: impl ToString) -> String {
    let d = d.to_string();
    let mut key = String::with_capacity(a.len() + b.len() + c.len() + d.len() + 3);
    key.push_str(a);
    key.push('|');
    key.push_str(b);
    key.push('|');
    key.push_str(c);
    key.push('|');
    key.push_str(&d);
    key
}

fn read_cpu_frequency_mhz(cache: &mut ReadCache, cpu: usize) -> Option<f64> {
    let base = Path::new("/sys/devices/system/cpu").join(format!("cpu{cpu}/cpufreq"));
    let khz = cache
        .read_u64(base.join("scaling_cur_freq"))
        .or_else(|| cache.read_u64(base.join("cpuinfo_cur_freq")))
        .or_else(|| cache.read_u64(base.join("base_frequency")))?;
    Some(khz as f64 / 1000.0)
}

fn page_size_bytes() -> u64 {
    procfs::page_size().max(1)
}

pub fn collect_snapshot(include_process_metrics: bool) -> Result<Snapshot> {
    let mut cache = ReadCache::default();

    let (processes, process_count_hint, process_meta, process_ok, process_err) =
        if include_process_metrics {
            match collect_processes(&mut cache) {
                Ok((processes, meta)) => {
                    let count = processes.len() as u64;
                    (processes, Some(count), Some(meta), true, None)
                }
                Err(e) => (
                    Vec::new(),
                    None,
                    Some(ProcessCollectionMeta {
                        fd_scan_enabled: false,
                    }),
                    false,
                    Some(e.to_string()),
                ),
            }
        } else {
            (Vec::new(), None, None, true, None)
        };

    let mounts = collect_mounts().unwrap_or_default();
    let filesystem = collect_filesystem_stats(&mounts);
    let (system, system_ok, system_err) = match collect_system(&mut cache, process_count_hint) {
        Ok(v) => (v, true, None),
        Err(e) => (SystemSnapshot::default(), false, Some(e.to_string())),
    };
    let load = collect_load().ok();
    let runnable = load.as_ref().map(|v| v.runnable as f64).unwrap_or(0.0);
    let (schedstat, runqueue_depth) =
        collect_schedstat_and_runqueue(system.per_cpu.len(), runnable)
            .unwrap_or_else(|_| (BTreeMap::new(), BTreeMap::new()));

    let pressure = collect_pressure().unwrap_or_default();
    let pressure_totals_us = collect_pressure_totals().unwrap_or_default();
    let cgroup = collect_cgroup().unwrap_or_else(|_| (BTreeMap::new(), CgroupMode::None));
    let cgroup_metrics = cgroup.0;
    let cgroup_mode = cgroup.1;
    let (memory, memory_ok, memory_err) = match collect_memory() {
        Ok(v) => (v, true, None),
        Err(e) => (MemorySnapshot::default(), false, Some(e.to_string())),
    };

    let psi_supported = !pressure.is_empty() || !pressure_totals_us.is_empty();
    let psi_irq_supported = pressure.keys().any(|k| k.starts_with("irq."))
        || pressure_totals_us.keys().any(|k| k.starts_with("irq."));

    let mut support_state = linux_support_state(LinuxSupportInputs {
        cgroup_mode,
        psi_supported,
        psi_irq_supported,
        schedstat_supported: !schedstat.is_empty(),
        process_meta: process_meta.as_ref(),
        in_container: is_likely_containerized(),
        system_ok,
        memory_ok,
        process_ok,
    });

    if let Some(err) = system_err {
        support_state.insert("snapshot.core.system.error".to_string(), err);
    }
    if let Some(err) = memory_err {
        support_state.insert("snapshot.core.memory.error".to_string(), err);
    }
    if let Some(err) = process_err {
        support_state.insert("snapshot.processes.error".to_string(), err);
    }

    Ok(Snapshot {
        system,
        memory,
        load,
        pressure,
        pressure_totals_us,
        vmstat: procfs::vmstat().unwrap_or_default().into_iter().collect(),
        interrupts: collect_interrupts().unwrap_or_default(),
        softirqs: collect_softirqs().unwrap_or_default(),
        net_snmp: collect_net_snmp().unwrap_or_default(),
        net_stat: collect_netstat().unwrap_or_default(),
        sockets: collect_sockets().unwrap_or_default(),
        schedstat,
        runqueue_depth,
        slabinfo: collect_slabinfo().unwrap_or_default(),
        filesystem,
        cgroup: cgroup_metrics,
        softnet: collect_softnet().unwrap_or_default(),
        swaps: collect_swaps().unwrap_or_default(),
        mounts,
        cpuinfo: collect_cpuinfo(&mut cache).unwrap_or_default(),
        zoneinfo: collect_zoneinfo().unwrap_or_default(),
        buddyinfo: collect_buddyinfo().unwrap_or_default(),
        disks: collect_disks(&mut cache).unwrap_or_default(),
        net: collect_net(&mut cache).unwrap_or_default(),
        processes,
        support_state,
        metric_classification: linux_metric_classification(),
        windows: None,
    })
}

fn collect_netstat() -> Result<BTreeMap<String, u64>> {
    let Ok(contents) = fs::read_to_string("/proc/net/netstat") else {
        return Ok(BTreeMap::new());
    };
    let mut out = BTreeMap::new();
    let mut pending: Option<(String, Vec<String>)> = None;

    for line in contents.lines() {
        let mut parts = line.split_whitespace();
        let Some(raw_prefix) = parts.next() else {
            continue;
        };
        let prefix = raw_prefix.trim_end_matches(':').to_string();
        let cols = parts.map(str::to_string).collect::<Vec<_>>();

        if let Some((pending_prefix, headers)) = pending.take() {
            if pending_prefix == prefix {
                for (header, value) in headers.iter().zip(cols.iter()) {
                    if let Ok(parsed) = value.parse::<u64>() {
                        out.insert(key_dot2(&prefix, header), parsed);
                    }
                }
            } else {
                pending = Some((prefix, cols));
            }
        } else {
            pending = Some((prefix, cols));
        }
    }

    Ok(out)
}

fn distribute_runnable(
    cpu_count: usize,
    runnable: f64,
    waiting_ns_by_cpu: &BTreeMap<usize, u64>,
) -> BTreeMap<String, f64> {
    let mut runqueue_depth = BTreeMap::new();
    let cpu_count = cpu_count.max(1);
    let wait_sum = waiting_ns_by_cpu.values().copied().sum::<u64>() as f64;

    runqueue_depth.insert("global_estimated_runnable".to_string(), runnable.max(0.0));

    for cpu in 0..cpu_count {
        let waiting_ns = waiting_ns_by_cpu.get(&cpu).copied().unwrap_or(0);
        let weight = if wait_sum > 0.0 {
            waiting_ns as f64 / wait_sum
        } else {
            1.0 / cpu_count as f64
        };
        runqueue_depth.insert(key_pipe2("cpu", cpu), (runnable * weight).max(0.0));
    }

    runqueue_depth
}

fn collect_schedstat_and_runqueue(
    cpu_count: usize,
    runnable: f64,
) -> Result<(BTreeMap<String, u64>, BTreeMap<String, f64>)> {
    let Ok(contents) = fs::read_to_string("/proc/schedstat") else {
        return Ok((
            BTreeMap::new(),
            distribute_runnable(cpu_count, runnable, &BTreeMap::new()),
        ));
    };
    let mut out = BTreeMap::new();
    let mut waiting_ns_by_cpu: BTreeMap<usize, u64> = BTreeMap::new();
    let mut version: Option<u64> = None;

    for line in contents.lines() {
        let cols = line.split_whitespace().collect::<Vec<_>>();
        if cols.len() >= 2 && cols[0] == "version" {
            version = cols[1].parse::<u64>().ok();
            if let Some(v) = version {
                out.insert("version|value".to_string(), v);
            }
            break;
        }
    }

    if let Some(v) = version {
        if v < 15 {
            return Ok((
                out,
                distribute_runnable(cpu_count, runnable, &BTreeMap::new()),
            ));
        }
    }

    for line in contents.lines() {
        let cols = line.split_whitespace().collect::<Vec<_>>();
        if cols.len() >= 2 && cols[0] == "version" {
            continue;
        }
        if cols.len() < 4 {
            continue;
        }

        let cpu = if let Some(raw) = cols[0].strip_prefix("cpu") {
            if raw.is_empty() {
                continue;
            }
            raw.parse::<usize>().ok()
        } else {
            None
        };

        let Some(cpu) = cpu else { continue };

        let n = cols.len();
        let running_ns = cols[n - 3].parse::<u64>().unwrap_or(0);
        let waiting_ns = cols[n - 2].parse::<u64>().unwrap_or(0);
        let timeslices = cols[n - 1].parse::<u64>().unwrap_or(0);

        out.insert(key_pipe3("cpu", "running_ns", cpu), running_ns);
        out.insert(key_pipe3("cpu", "waiting_ns", cpu), waiting_ns);
        out.insert(key_pipe3("cpu", "timeslices", cpu), timeslices);
        waiting_ns_by_cpu.insert(cpu, waiting_ns);
    }

    let runqueue_depth = distribute_runnable(cpu_count, runnable, &waiting_ns_by_cpu);

    Ok((out, runqueue_depth))
}

fn collect_slabinfo() -> Result<BTreeMap<String, u64>> {
    let Ok(contents) = fs::read_to_string("/proc/slabinfo") else {
        return Ok(collect_slabinfo_sysfs());
    };
    let mut out = BTreeMap::new();

    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let cols = trimmed.split_whitespace().collect::<Vec<_>>();
        if cols.len() < 6 {
            continue;
        }

        let name = cols[0];
        let active_objs = cols[1].parse::<u64>().unwrap_or(0);
        let num_objs = cols[2].parse::<u64>().unwrap_or(0);
        let objsize = cols[3].parse::<u64>().unwrap_or(0);
        let objperslab = cols[4].parse::<u64>().unwrap_or(0);
        let pagesperslab = cols[5].parse::<u64>().unwrap_or(0);

        out.insert(key_pipe3(name, "active_objs", "value"), active_objs);
        out.insert(key_pipe3(name, "num_objs", "value"), num_objs);
        out.insert(key_pipe3(name, "objsize", "bytes"), objsize);
        out.insert(key_pipe3(name, "objperslab", "value"), objperslab);
        out.insert(key_pipe3(name, "pagesperslab", "value"), pagesperslab);

        if let Some(pos) = cols.iter().position(|c| *c == ":") {
            if cols.len() >= pos + 4 {
                let active_slabs = cols[pos + 1].parse::<u64>().unwrap_or(0);
                let num_slabs = cols[pos + 2].parse::<u64>().unwrap_or(0);
                out.insert(key_pipe3(name, "active_slabs", "value"), active_slabs);
                out.insert(key_pipe3(name, "num_slabs", "value"), num_slabs);
            }
        }
    }

    if out.is_empty() {
        Ok(collect_slabinfo_sysfs())
    } else {
        Ok(out)
    }
}

fn collect_slabinfo_sysfs() -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    let Ok(entries) = fs::read_dir("/sys/kernel/slab") else {
        return out;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|v| v.to_str()) else {
            continue;
        };

        let read_u64 = |file: &str| -> Option<u64> {
            let raw = fs::read_to_string(path.join(file)).ok()?;
            raw.split_whitespace().next()?.parse::<u64>().ok()
        };

        if let Some(value) = read_u64("objects") {
            out.insert(key_pipe3(name, "active_objs", "value"), value);
        }
        if let Some(value) = read_u64("total_objects") {
            out.insert(key_pipe3(name, "num_objs", "value"), value);
        }
        if let Some(value) = read_u64("object_size") {
            out.insert(key_pipe3(name, "objsize", "bytes"), value);
        }
        if let Some(value) = read_u64("objects_per_slab") {
            out.insert(key_pipe3(name, "objperslab", "value"), value);
        }
        if let Some(value) = read_u64("slabs") {
            out.insert(key_pipe3(name, "num_slabs", "value"), value);
        }
        if let Some(value) = read_u64("partial") {
            out.insert(key_pipe3(name, "partial_slabs", "value"), value);
        }
    }

    out
}

fn collect_filesystem_stats(mounts: &[MountSnapshot]) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();

    for mount in mounts {
        let mountpoint = Path::new(&mount.mountpoint);
        let Ok(path_c) = CString::new(mountpoint.as_os_str().as_bytes()) else {
            continue;
        };

        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::statvfs(path_c.as_ptr(), &mut stat) };
        if rc != 0 {
            continue;
        }

        let frsize = if stat.f_frsize > 0 {
            stat.f_frsize
        } else {
            stat.f_bsize
        } as u64;

        let total_bytes = (stat.f_blocks as u64).saturating_mul(frsize);
        let free_bytes = (stat.f_bfree as u64).saturating_mul(frsize);
        let avail_bytes = (stat.f_bavail as u64).saturating_mul(frsize);
        let used_bytes = total_bytes.saturating_sub(free_bytes);
        let used_bytes_user_visible = total_bytes.saturating_sub(avail_bytes);
        let reserved_bytes = free_bytes.saturating_sub(avail_bytes);

        out.insert(
            key_pipe3(&mount.mountpoint, "total_bytes", "value"),
            total_bytes,
        );
        out.insert(
            key_pipe3(&mount.mountpoint, "used_bytes", "value"),
            used_bytes,
        );
        out.insert(
            key_pipe3(&mount.mountpoint, "used_bytes_user_visible", "value"),
            used_bytes_user_visible,
        );
        out.insert(
            key_pipe3(&mount.mountpoint, "reserved_bytes", "value"),
            reserved_bytes,
        );
        out.insert(
            key_pipe3(&mount.mountpoint, "free_bytes", "value"),
            free_bytes,
        );
        out.insert(
            key_pipe3(&mount.mountpoint, "avail_bytes", "value"),
            avail_bytes,
        );
        out.insert(
            key_pipe3(&mount.mountpoint, "blocks", "value"),
            stat.f_blocks as u64,
        );
        out.insert(
            key_pipe3(&mount.mountpoint, "bfree", "value"),
            stat.f_bfree as u64,
        );
        out.insert(
            key_pipe3(&mount.mountpoint, "bavail", "value"),
            stat.f_bavail as u64,
        );
        out.insert(
            key_pipe3(&mount.mountpoint, "files", "value"),
            stat.f_files as u64,
        );
        out.insert(
            key_pipe3(&mount.mountpoint, "ffree", "value"),
            stat.f_ffree as u64,
        );
    }

    out
}

fn collect_cgroup() -> Result<(BTreeMap<String, u64>, CgroupMode)> {
    let root = Path::new("/sys/fs/cgroup");
    let mounts = fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
    let mut v1_mounts = Vec::new();
    let mut v2_mounts = Vec::new();

    for line in mounts.lines() {
        let Some((left, right)) = line.split_once(" - ") else {
            continue;
        };
        let left_cols = left.split_whitespace().collect::<Vec<_>>();
        let right_cols = right.split_whitespace().collect::<Vec<_>>();
        if left_cols.len() < 5 || right_cols.is_empty() {
            continue;
        }
        let mountpoint = PathBuf::from(unescape_mount_field(left_cols[4]));
        match right_cols[0] {
            "cgroup2" => v2_mounts.push(mountpoint),
            "cgroup" => v1_mounts.push(mountpoint),
            _ => {}
        }
    }

    if !root.exists() && v1_mounts.is_empty() && v2_mounts.is_empty() {
        return Ok((BTreeMap::new(), CgroupMode::None));
    }

    let mut out = BTreeMap::new();
    let has_v2 = root.join("cgroup.controllers").exists() || !v2_mounts.is_empty();
    let has_v1 = !v1_mounts.is_empty()
        || fs::read_dir(root)
            .ok()
            .map(|entries| {
                entries.flatten().any(|entry| {
                    let path = entry.path();
                    path.is_dir()
                        && path
                            .file_name()
                            .and_then(|v| v.to_str())
                            .map(|name| {
                                matches!(name, "cpu" | "cpuacct" | "memory" | "blkio" | "pids")
                            })
                            .unwrap_or(false)
                })
            })
            .unwrap_or(false);

    if has_v2 {
        if v2_mounts.is_empty() {
            collect_cgroup_v2_tree(root, &mut out);
        } else {
            for mount in v2_mounts {
                collect_cgroup_v2_tree(&mount, &mut out);
            }
        }
    }
    if has_v1 {
        if v1_mounts.is_empty() {
            collect_cgroup_v1_tree(root, "unknown", &mut out);
        } else {
            for mount in v1_mounts {
                let controller = mount
                    .file_name()
                    .and_then(|v| v.to_str())
                    .unwrap_or("unknown");
                collect_cgroup_v1_tree(&mount, controller, &mut out);
            }
        }
    }

    let mode = match (has_v1, has_v2) {
        (true, true) => CgroupMode::Hybrid,
        (true, false) => CgroupMode::V1,
        (false, true) => CgroupMode::V2,
        (false, false) => CgroupMode::None,
    };

    Ok((out, mode))
}

fn collect_cgroup_v2_tree(root: &Path, out: &mut BTreeMap<String, u64>) {
    let mut stack = vec![(root.to_path_buf(), 0usize)];
    let mut visited = 0usize;

    while let Some((dir, depth)) = stack.pop() {
        visited += 1;
        if visited > CGROUP_MAX_DIRS {
            break;
        }
        if depth > CGROUP_MAX_DEPTH {
            continue;
        }

        let rel_raw = dir
            .strip_prefix(root)
            .ok()
            .and_then(|p| p.to_str())
            .filter(|p| !p.is_empty())
            .map(|p| format!("v2/{p}"))
            .unwrap_or_else(|| "v2/root".to_string());
        let rel = normalize_cgroup_scope(&rel_raw);

        collect_cgroup_v2_dir(&dir, &rel, out);

        if let Ok(entries) = fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push((path, depth + 1));
                }
            }
        }
    }
}

fn collect_cgroup_v1_tree(root: &Path, controller_raw: &str, out: &mut BTreeMap<String, u64>) {
    let controller = controller_raw.replace(',', "+");
    let mut stack = vec![(root.to_path_buf(), 0usize)];
    let mut visited = 0usize;
    while let Some((dir, depth)) = stack.pop() {
        visited += 1;
        if visited > CGROUP_MAX_DIRS {
            break;
        }
        if depth > CGROUP_MAX_DEPTH {
            continue;
        }

        let rel_raw = dir
            .strip_prefix(root)
            .ok()
            .and_then(|p| p.to_str())
            .filter(|p| !p.is_empty())
            .map(|p| format!("v1/{controller}/{p}"))
            .unwrap_or_else(|| format!("v1/{controller}/root"));
        let rel = normalize_cgroup_scope(&rel_raw);

        collect_cgroup_v1_dir(&dir, &rel, out);

        if let Ok(children) = fs::read_dir(&dir) {
            for child in children.flatten() {
                let path = child.path();
                if path.is_dir() {
                    stack.push((path, depth + 1));
                }
            }
        }
    }
}

fn collect_cgroup_v2_dir(path: &Path, scope: &str, out: &mut BTreeMap<String, u64>) {
    for file in [
        "memory.current",
        "memory.swap.current",
        "memory.swap.max",
        "pids.current",
        "pids.max",
        "cpu.weight",
    ] {
        let file_path = path.join(file);
        let text = fs::read_to_string(&file_path).ok();
        let (parsed, is_max) = text
            .as_deref()
            .map(|v| parse_u64_with_max_flag(v.trim()))
            .unwrap_or((None, false));
        if let Some(value) = parsed {
            out.insert(key_pipe3(scope, file, "value"), value);
        }
        if is_max {
            out.insert(key_pipe3(scope, file, "is_max"), 1);
        }
    }

    if let Ok(cpu_max) = fs::read_to_string(path.join("cpu.max")) {
        let cols = cpu_max.split_whitespace().collect::<Vec<_>>();
        if cols.len() >= 2 {
            let (quota, quota_is_max) = parse_u64_with_max_flag(cols[0]);
            if let Some(value) = quota {
                out.insert(key_pipe3(scope, "cpu.max.quota", "value"), value);
            }
            if quota_is_max {
                out.insert(key_pipe3(scope, "cpu.max.quota", "is_max"), 1);
            }

            let (period, period_is_max) = parse_u64_with_max_flag(cols[1]);
            if let Some(value) = period {
                out.insert(key_pipe3(scope, "cpu.max.period", "value"), value);
            }
            if period_is_max {
                out.insert(key_pipe3(scope, "cpu.max.period", "is_max"), 1);
            }
        }
    }

    if let Ok(cpu_stat) = fs::read_to_string(path.join("cpu.stat")) {
        for line in cpu_stat.lines() {
            let cols = line.split_whitespace().collect::<Vec<_>>();
            if cols.len() != 2 {
                continue;
            }
            let (parsed, is_max) = parse_u64_with_max_flag(cols[1]);
            if let Some(value) = parsed {
                out.insert(key_pipe3(scope, "cpu.stat", cols[0]), value);
            }
            if is_max {
                out.insert(key_pipe4(scope, "cpu.stat", cols[0], "is_max"), 1);
            }
        }
    }

    if let Ok(memory_stat) = fs::read_to_string(path.join("memory.stat")) {
        for line in memory_stat.lines() {
            let cols = line.split_whitespace().collect::<Vec<_>>();
            if cols.len() != 2 {
                continue;
            }
            let (parsed, is_max) = parse_u64_with_max_flag(cols[1]);
            if let Some(value) = parsed {
                out.insert(key_pipe3(scope, "memory.stat", cols[0]), value);
            }
            if is_max {
                out.insert(key_pipe4(scope, "memory.stat", cols[0], "is_max"), 1);
            }
        }
    }

    if let Ok(io_stat) = fs::read_to_string(path.join("io.stat")) {
        for line in io_stat.lines() {
            let cols = line.split_whitespace().collect::<Vec<_>>();
            if cols.len() < 2 {
                continue;
            }
            let dev = cols[0];
            for kv in &cols[1..] {
                let Some((k, v)) = kv.split_once('=') else {
                    continue;
                };
                let (parsed, is_max) = parse_u64_with_max_flag(v);
                if let Some(value) = parsed {
                    out.insert(key_pipe4(scope, "io.stat", dev, k), value);
                }
                if is_max {
                    out.insert(key_pipe4(scope, "io.stat", dev, format!("{k}.is_max")), 1);
                }
            }
        }
    }
}

fn collect_cgroup_v1_dir(path: &Path, scope: &str, out: &mut BTreeMap<String, u64>) {
    for file in [
        "memory.usage_in_bytes",
        "memory.limit_in_bytes",
        "memory.memsw.usage_in_bytes",
        "memory.memsw.limit_in_bytes",
        "memory.kmem.usage_in_bytes",
        "pids.current",
        "pids.max",
        "cpu.shares",
        "cpu.cfs_quota_us",
        "cpu.cfs_period_us",
        "cpuacct.usage",
    ] {
        let file_path = path.join(file);
        let text = fs::read_to_string(&file_path).ok();
        let (parsed, is_max) = text
            .as_deref()
            .map(|v| parse_u64_with_max_flag(v.trim()))
            .unwrap_or((None, false));
        if let Some(value) = parsed {
            out.insert(key_pipe3(scope, file, "value"), value);
        }
        if is_max {
            out.insert(key_pipe3(scope, file, "is_max"), 1);
        }
    }

    if let Ok(contents) = fs::read_to_string(path.join("blkio.throttle.io_service_bytes")) {
        for line in contents.lines() {
            let cols = line.split_whitespace().collect::<Vec<_>>();
            if cols.len() < 3 {
                continue;
            }
            let dev = cols[0];
            let op = cols[1].to_ascii_lowercase();
            let (parsed, is_max) = parse_u64_with_max_flag(cols[2]);
            if let Some(value) = parsed {
                out.insert(
                    key_pipe4(scope, "blkio.throttle.io_service_bytes", dev, op.as_str()),
                    value,
                );
            }
            if is_max {
                out.insert(
                    key_pipe4(
                        scope,
                        "blkio.throttle.io_service_bytes",
                        dev,
                        format!("{op}.is_max"),
                    ),
                    1,
                );
            }
        }
    }
}

fn read_proc_first_value(cache: &mut ReadCache, path: impl AsRef<Path>) -> Option<String> {
    cache
        .read_raw(path)?
        .split_whitespace()
        .next()
        .map(str::to_string)
}

fn read_proc_u64(cache: &mut ReadCache, path: impl AsRef<Path>) -> Option<u64> {
    read_proc_first_value(cache, path)?.parse().ok()
}

#[derive(Default)]
struct ProcessStatusFields {
    fd_table_size: Option<u64>,
    vm_size_kib: Option<u64>,
    vm_rss_kib: Option<u64>,
    vm_data_kib: Option<u64>,
    vm_stack_kib: Option<u64>,
    vm_exe_kib: Option<u64>,
    vm_lib_kib: Option<u64>,
    vm_swap_kib: Option<u64>,
    vm_pte_kib: Option<u64>,
    vm_hwm_kib: Option<u64>,
    voluntary_ctxt_switches: Option<u64>,
    nonvoluntary_ctxt_switches: Option<u64>,
}

fn parse_status_value_kib(raw: &str) -> Option<u64> {
    raw.split_whitespace().next()?.parse().ok()
}

fn parse_process_status_fields(contents: &str) -> ProcessStatusFields {
    let mut out = ProcessStatusFields::default();

    for line in contents.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();

        match key {
            "FDSize" => out.fd_table_size = value.parse().ok(),
            "VmSize" => out.vm_size_kib = parse_status_value_kib(value),
            "VmRSS" => out.vm_rss_kib = parse_status_value_kib(value),
            "VmData" => out.vm_data_kib = parse_status_value_kib(value),
            "VmStk" => out.vm_stack_kib = parse_status_value_kib(value),
            "VmExe" => out.vm_exe_kib = parse_status_value_kib(value),
            "VmLib" => out.vm_lib_kib = parse_status_value_kib(value),
            "VmSwap" => out.vm_swap_kib = parse_status_value_kib(value),
            "VmPTE" => out.vm_pte_kib = parse_status_value_kib(value),
            "VmHWM" => out.vm_hwm_kib = parse_status_value_kib(value),
            "voluntary_ctxt_switches" => out.voluntary_ctxt_switches = value.parse().ok(),
            "nonvoluntary_ctxt_switches" => out.nonvoluntary_ctxt_switches = value.parse().ok(),
            _ => {}
        }
    }

    out
}

fn collect_pressure() -> Result<BTreeMap<String, f64>> {
    let mut out = BTreeMap::new();

    for resource in ["cpu", "memory", "io", "irq"] {
        let path = Path::new("/proc/pressure").join(resource);
        let Ok(contents) = fs::read_to_string(path) else {
            continue;
        };

        for line in contents.lines() {
            let mut parts = line.split_whitespace();
            let Some(scope) = parts.next() else { continue };

            for field in parts {
                let Some((name, value)) = field.split_once('=') else {
                    continue;
                };
                if name == "total" {
                    continue;
                }
                if let Ok(parsed) = value.parse::<f64>() {
                    out.insert(key_dot3(resource, scope, name), parsed / 100.0);
                }
            }
        }
    }

    Ok(out)
}

fn collect_pressure_totals() -> Result<BTreeMap<String, u64>> {
    let mut out = BTreeMap::new();

    for resource in ["cpu", "memory", "io", "irq"] {
        let path = Path::new("/proc/pressure").join(resource);
        let Ok(contents) = fs::read_to_string(path) else {
            continue;
        };

        for line in contents.lines() {
            let mut parts = line.split_whitespace();
            let Some(scope) = parts.next() else { continue };

            for field in parts {
                let Some((name, value)) = field.split_once('=') else {
                    continue;
                };
                if name != "total" {
                    continue;
                }
                if let Ok(parsed) = value.parse::<u64>() {
                    out.insert(key_dot2(resource, scope), parsed);
                }
            }
        }
    }

    Ok(out)
}

fn collect_proc_stat_totals() -> Result<(u64, u64)> {
    let contents = fs::read_to_string("/proc/stat")?;
    let mut interrupts_total = 0;
    let mut softirqs_total = 0;

    for line in contents.lines() {
        let mut parts = line.split_whitespace();
        match parts.next() {
            Some("intr") => {
                interrupts_total = parts.next().and_then(|v| v.parse().ok()).unwrap_or(0);
            }
            Some("softirq") => {
                softirqs_total = parts.next().and_then(|v| v.parse().ok()).unwrap_or(0);
            }
            _ => {}
        }
    }

    Ok((interrupts_total, softirqs_total))
}

fn collect_uptime_secs() -> Result<f64> {
    let value = fs::read_to_string("/proc/uptime")?;
    Ok(value
        .split_whitespace()
        .next()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.0))
}

fn collect_net_snmp() -> Result<BTreeMap<String, u64>> {
    let contents = fs::read_to_string("/proc/net/snmp")?;
    let mut out = BTreeMap::new();
    let mut pending: Option<(String, Vec<String>)> = None;

    for line in contents.lines() {
        let mut parts = line.split_whitespace();
        let Some(raw_prefix) = parts.next() else {
            continue;
        };
        let prefix = raw_prefix.trim_end_matches(':').to_string();
        let cols = parts.map(str::to_string).collect::<Vec<_>>();

        if let Some((pending_prefix, headers)) = pending.take() {
            if pending_prefix == prefix {
                for (header, value) in headers.iter().zip(cols.iter()) {
                    if let Ok(parsed) = value.parse::<u64>() {
                        out.insert(key_dot2(&prefix, header), parsed);
                    }
                }
            } else {
                pending = Some((prefix, cols));
            }
        } else {
            pending = Some((prefix, cols));
        }
    }

    Ok(out)
}

fn collect_sockets() -> Result<BTreeMap<String, u64>> {
    fn parse_sockstat(path: &str, family: &str, out: &mut BTreeMap<String, u64>) -> Result<()> {
        let contents = fs::read_to_string(path)?;
        for line in contents.lines() {
            let mut parts = line.split_whitespace();
            let Some(proto_raw) = parts.next() else {
                continue;
            };
            let proto = proto_raw.trim_end_matches(':').to_ascii_lowercase();
            let cols = parts.collect::<Vec<_>>();
            let mut i = 0usize;

            while i + 1 < cols.len() {
                let key = cols[i].to_ascii_lowercase();
                if let Ok(value) = cols[i + 1].parse::<u64>() {
                    out.insert(key_dot3(family, &proto, &key), value);
                }
                i += 2;
            }
        }
        Ok(())
    }

    let mut out = BTreeMap::new();
    parse_sockstat("/proc/net/sockstat", "v4", &mut out)?;
    if Path::new("/proc/net/sockstat6").exists() {
        parse_sockstat("/proc/net/sockstat6", "v6", &mut out)?;
    }
    Ok(out)
}

fn collect_interrupts() -> Result<BTreeMap<String, u64>> {
    let contents = fs::read_to_string("/proc/interrupts")?;
    let mut out = BTreeMap::new();
    let mut cpus = 0usize;

    for (idx, line) in contents.lines().enumerate() {
        if idx == 0 {
            cpus = line
                .split_whitespace()
                .filter(|part| part.starts_with("CPU"))
                .count();
            continue;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let Some((irq_raw, rest)) = trimmed.split_once(':') else {
            continue;
        };
        let irq = irq_raw.trim();
        let cols = rest.split_whitespace().collect::<Vec<_>>();

        for (cpu, value) in cols.iter().take(cpus.min(cols.len())).enumerate() {
            if let Ok(value) = value.parse::<u64>() {
                out.insert(key_pipe2(irq, cpu), value);
            }
        }
    }

    Ok(out)
}

fn collect_softirqs() -> Result<BTreeMap<String, u64>> {
    let contents = fs::read_to_string("/proc/softirqs")?;
    let mut out = BTreeMap::new();
    let mut cpus = 0usize;

    for (idx, line) in contents.lines().enumerate() {
        if idx == 0 {
            cpus = line
                .split_whitespace()
                .filter(|part| part.starts_with("CPU"))
                .count();
            continue;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let Some((kind_raw, rest)) = trimmed.split_once(':') else {
            continue;
        };
        let kind = kind_raw.trim();
        let cols = rest.split_whitespace().collect::<Vec<_>>();

        for (cpu, value) in cols.iter().take(cpus.min(cols.len())).enumerate() {
            if let Ok(value) = value.parse::<u64>() {
                out.insert(key_pipe2(kind, cpu), value);
            }
        }
    }

    Ok(out)
}

fn collect_softnet() -> Result<Vec<SoftnetCpuSnapshot>> {
    let contents = fs::read_to_string("/proc/net/softnet_stat")?;
    Ok(contents
        .lines()
        .enumerate()
        .filter_map(|(cpu, line)| {
            let cols = line.split_whitespace().collect::<Vec<_>>();
            if cols.len() < 3 {
                return None;
            }

            Some(SoftnetCpuSnapshot {
                cpu,
                processed: u64::from_str_radix(cols[0], 16).unwrap_or(0),
                dropped: u64::from_str_radix(cols[1], 16).unwrap_or(0),
                time_squeezed: u64::from_str_radix(cols[2], 16).unwrap_or(0),
                cpu_collision: cols.get(8).and_then(|v| u64::from_str_radix(v, 16).ok()),
                received_rps: cols.get(9).and_then(|v| u64::from_str_radix(v, 16).ok()),
                flow_limit_count: cols.get(10).and_then(|v| u64::from_str_radix(v, 16).ok()),
            })
        })
        .collect())
}

fn collect_swaps() -> Result<Vec<SwapDeviceSnapshot>> {
    let contents = fs::read_to_string("/proc/swaps")?;
    let mut out = Vec::new();

    for (idx, line) in contents.lines().enumerate() {
        if idx == 0 {
            continue;
        }

        let cols = line.split_whitespace().collect::<Vec<_>>();
        if cols.len() < 5 {
            continue;
        }

        let size_kib = cols[2].parse::<u64>().unwrap_or(0);
        let used_kib = cols[3].parse::<u64>().unwrap_or(0);
        let priority = cols[4].parse::<i64>().unwrap_or(0);

        out.push(SwapDeviceSnapshot {
            device: cols[0].to_string(),
            swap_type: cols[1].to_string(),
            size_bytes: size_kib.saturating_mul(1024),
            used_bytes: used_kib.saturating_mul(1024),
            priority,
        });
    }

    Ok(out)
}

fn collect_mounts() -> Result<Vec<MountSnapshot>> {
    let contents = fs::read_to_string("/proc/mounts")?;
    let mut out = Vec::new();

    for line in contents.lines() {
        let cols = line.split_whitespace().collect::<Vec<_>>();
        if cols.len() < 4 {
            continue;
        }

        let fs_type = cols[2].to_string();
        if !include_pseudo_filesystems() && is_pseudo_filesystem(&fs_type) {
            continue;
        }

        out.push(MountSnapshot {
            device: unescape_mount_field(cols[0]),
            mountpoint: unescape_mount_field(cols[1]),
            fs_type,
            read_only: cols[3].split(',').any(|option| option == "ro"),
        });
    }

    Ok(out)
}

fn collect_cpuinfo(cache: &mut ReadCache) -> Result<Vec<CpuInfoSnapshot>> {
    let contents = fs::read_to_string("/proc/cpuinfo")?;
    let mut out = Vec::new();
    let mut current = CpuInfoSnapshot::default();
    let mut seen = false;

    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            if seen {
                out.push(current);
                current = CpuInfoSnapshot::default();
                seen = false;
            }
            continue;
        }

        let Some((key, value)) = trimmed.split_once(':') else {
            continue;
        };
        let key = key.trim();
        let value = value.trim();
        seen = true;

        match key {
            "processor" => current.cpu = value.parse::<usize>().unwrap_or(0),
            "vendor_id" | "CPU implementer" | "Hardware" => {
                current.vendor_id = Some(value.to_string())
            }
            "model name" | "Processor" | "model" | "cpu" => {
                if current.model_name.is_none() {
                    current.model_name = Some(value.to_string());
                }
            }
            "cpu MHz" => current.mhz = value.parse::<f64>().ok(),
            "cache size" | "L2 cache" => {
                let size_kib = value
                    .split_whitespace()
                    .next()
                    .and_then(|v| v.parse::<u64>().ok());
                if current.cache_size_bytes.is_none() {
                    current.cache_size_bytes = size_kib.map(|v| v.saturating_mul(1024));
                }
            }
            _ => {}
        }
    }

    if seen {
        out.push(current);
    }

    for cpu in &mut out {
        if cpu.mhz.is_none() {
            cpu.mhz = read_cpu_frequency_mhz(cache, cpu.cpu);
        }
    }

    Ok(out)
}

fn collect_zoneinfo() -> Result<BTreeMap<String, u64>> {
    let contents = fs::read_to_string("/proc/zoneinfo")?;
    let mut out = BTreeMap::new();
    let mut current_node = String::new();
    let mut current_zone = String::new();

    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Some(rest) = trimmed.strip_prefix("Node ") {
            if let Some((node_part, zone_part)) = rest.split_once(", zone") {
                current_node = node_part.trim().to_string();
                current_zone = zone_part.trim().to_string();
            }
            continue;
        }

        if current_node.is_empty() || current_zone.is_empty() {
            continue;
        }

        let cols = trimmed.split_whitespace().collect::<Vec<_>>();
        if cols.len() != 2 {
            continue;
        }

        if let Ok(value) = cols[1].parse::<u64>() {
            out.insert(key_pipe3(&current_node, &current_zone, cols[0]), value);
        }
    }

    Ok(out)
}

fn collect_buddyinfo() -> Result<BTreeMap<String, u64>> {
    let contents = fs::read_to_string("/proc/buddyinfo")?;
    let mut out = BTreeMap::new();

    for line in contents.lines() {
        let cols = line.split_whitespace().collect::<Vec<_>>();
        if cols.len() < 5 || cols[0] != "Node" {
            continue;
        }

        let node = cols[1].trim_end_matches(',');
        let zone = cols[3].trim_end_matches(',');

        for (order, value) in cols[4..].iter().enumerate() {
            if let Ok(parsed) = value.parse::<u64>() {
                out.insert(key_pipe3(node, zone, order), parsed);
            }
        }
    }

    Ok(out)
}

fn cpu_times_from_stat(cpu: &procfs::CpuTime) -> CpuTimes {
    CpuTimes {
        user: cpu.user,
        nice: cpu.nice,
        system: cpu.system,
        idle: cpu.idle,
        iowait: cpu.iowait.unwrap_or(0),
        irq: cpu.irq.unwrap_or(0),
        softirq: cpu.softirq.unwrap_or(0),
        steal: cpu.steal.unwrap_or(0),
        guest: cpu.guest.unwrap_or(0),
        guest_nice: cpu.guest_nice.unwrap_or(0),
    }
}

fn cpu_times_seconds_from_stat(cpu: &procfs::CpuTime, hz: f64) -> CpuTimesSeconds {
    CpuTimesSeconds {
        user: cpu.user as f64 / hz,
        nice: cpu.nice as f64 / hz,
        system: cpu.system as f64 / hz,
        idle: cpu.idle as f64 / hz,
        iowait: cpu.iowait.unwrap_or(0) as f64 / hz,
        irq: cpu.irq.unwrap_or(0) as f64 / hz,
        softirq: cpu.softirq.unwrap_or(0) as f64 / hz,
        steal: cpu.steal.unwrap_or(0) as f64 / hz,
        guest: cpu.guest.unwrap_or(0) as f64 / hz,
        guest_nice: cpu.guest_nice.unwrap_or(0) as f64 / hz,
    }
}

fn collect_system(cache: &mut ReadCache, process_count: Option<u64>) -> Result<SystemSnapshot> {
    let stat = procfs::KernelStats::current()?;
    let (interrupts_total, softirqs_total) = collect_proc_stat_totals()?;
    let uptime_secs = collect_uptime_secs()?;
    let hz = procfs::ticks_per_second().max(1) as f64;

    let per_cpu = stat.cpu_time.iter().map(cpu_times_from_stat).collect();
    let cpu_total = cpu_times_from_stat(&stat.total);
    let cpu_total_seconds = cpu_times_seconds_from_stat(&stat.total, hz);
    let per_cpu_seconds = stat
        .cpu_time
        .iter()
        .map(|cpu| cpu_times_seconds_from_stat(cpu, hz))
        .collect();

    Ok(SystemSnapshot {
        is_windows: false,
        ticks_per_second: procfs::ticks_per_second(),
        cpu_cycle_utilization: None,
        boot_time_epoch_secs: stat.btime,
        uptime_secs,
        context_switches: stat.ctxt,
        forks_since_boot: Some(stat.processes),
        interrupts_total,
        softirqs_total,
        process_count: process_count
            .unwrap_or_else(|| all_processes().map(|p| p.count() as u64).unwrap_or(0)),
        pid_max: read_proc_u64(cache, "/proc/sys/kernel/pid_max"),
        entropy_available_bits: read_proc_u64(cache, "/proc/sys/kernel/random/entropy_avail"),
        entropy_pool_size_bits: read_proc_u64(cache, "/proc/sys/kernel/random/poolsize"),
        procs_running: stat.procs_running.unwrap_or(0),
        procs_blocked: stat.procs_blocked.unwrap_or(0),
        cpu_total,
        cpu_total_seconds,
        per_cpu,
        per_cpu_seconds,
    })
}

fn collect_memory() -> Result<MemorySnapshot> {
    let mem = procfs::Meminfo::current()?;
    Ok(MemorySnapshot {
        mem_total_bytes: mem.mem_total,
        mem_free_bytes: mem.mem_free,
        mem_available_bytes: mem.mem_available.unwrap_or(0),
        buffers_bytes: Some(mem.buffers),
        cached_bytes: mem.cached,
        active_bytes: Some(mem.active),
        inactive_bytes: Some(mem.inactive),
        anon_pages_bytes: Some(mem.anon_pages.unwrap_or(0)),
        mapped_bytes: Some(mem.mapped),
        shmem_bytes: Some(mem.shmem.unwrap_or(0)),
        swap_total_bytes: mem.swap_total,
        swap_free_bytes: mem.swap_free,
        swap_cached_bytes: Some(mem.swap_cached),
        dirty_bytes: Some(mem.dirty),
        writeback_bytes: Some(mem.writeback),
        slab_bytes: Some(mem.slab),
        sreclaimable_bytes: Some(mem.s_reclaimable.unwrap_or(0)),
        sunreclaim_bytes: Some(mem.s_unreclaim.unwrap_or(0)),
        page_tables_bytes: Some(mem.page_tables.unwrap_or(0)),
        committed_as_bytes: mem.committed_as,
        commit_limit_bytes: mem.commit_limit.unwrap_or(0),
        kernel_stack_bytes: Some(mem.kernel_stack.unwrap_or(0)),
        hugepages_total: Some(mem.hugepages_total.unwrap_or(0)),
        hugepages_free: Some(mem.hugepages_free.unwrap_or(0)),
        hugepage_size_bytes: Some(mem.hugepagesize.unwrap_or(0)),
        anon_hugepages_bytes: Some(mem.anon_hugepages.unwrap_or(0)),
    })
}

fn collect_load() -> Result<LoadSnapshot> {
    let load = procfs::LoadAverage::current()?;
    Ok(LoadSnapshot {
        one: load.one as f64,
        five: load.five as f64,
        fifteen: load.fifteen as f64,
        runnable: load.cur,
        entities: load.max,
        latest_pid: load.latest_pid,
    })
}

fn disk_queue_dir(name: &str) -> Option<PathBuf> {
    let class_queue = Path::new("/sys/class/block").join(name).join("queue");
    if class_queue.exists() {
        return Some(class_queue);
    }

    let block_queue = Path::new("/sys/block").join(name).join("queue");
    if block_queue.exists() {
        return Some(block_queue);
    }

    None
}

fn collect_disks(cache: &mut ReadCache) -> Result<Vec<DiskSnapshot>> {
    let stats = procfs::DiskStats::current()?;
    Ok(stats
        .0
        .into_iter()
        .map(|d| {
            let base = disk_queue_dir(&d.name);
            let logical_block_size = base
                .as_ref()
                .and_then(|p| cache.read_u64(p.join("logical_block_size")))
                .or_else(|| {
                    base.as_ref()
                        .and_then(|p| cache.read_u64(p.join("hw_sector_size")))
                })
                .or(Some(512));

            DiskSnapshot {
                name: d.name,
                has_counters: true,
                reads: d.reads,
                writes: d.writes,
                sectors_read: d.sectors_read,
                sectors_written: d.sectors_written,
                time_reading_ms: d.time_reading,
                time_writing_ms: d.time_writing,
                in_progress: d.in_progress,
                time_in_progress_ms: d.time_in_progress,
                weighted_time_in_progress_ms: d.weighted_time_in_progress,
                logical_block_size,
                physical_block_size: base
                    .as_ref()
                    .and_then(|p| cache.read_u64(p.join("physical_block_size"))),
                rotational: base
                    .as_ref()
                    .and_then(|p| cache.read_bool_num(p.join("rotational"))),
            }
        })
        .collect())
}

fn collect_net(cache: &mut ReadCache) -> Result<Vec<NetDevSnapshot>> {
    let devs = fs::read_to_string("/proc/net/dev")?;
    let mut out = Vec::new();
    let primary = read_primary_interfaces();
    let include_virtual = include_virtual_interfaces();

    for line in devs.lines().skip(2) {
        let mut parts = line.split(':');
        let name = parts.next().unwrap_or("").trim().to_string();
        let data = parts
            .next()
            .unwrap_or("")
            .split_whitespace()
            .collect::<Vec<_>>();

        if data.len() < 16 || name.is_empty() {
            continue;
        }

        let sys = Path::new("/sys/class/net").join(&name);
        let is_loopback = name == "lo";
        let is_virtual = Path::new("/sys/devices/virtual/net").join(&name).exists();
        let is_physical = !is_loopback && !is_virtual;
        let is_primary = primary.contains(&name);

        if !include_virtual && is_noise_interface(&name) && !is_primary {
            continue;
        }

        let stable_id = cache
            .read_trimmed(sys.join("address"))
            .filter(|mac| *mac != "00:00:00:00:00:00")
            .map(|mac| format!("mac:{mac}"))
            .or_else(|| {
                cache
                    .read_trimmed(sys.join("ifindex"))
                    .map(|v| format!("ifindex:{v}"))
            });

        let speed_mbps = cache.read_u64(sys.join("speed")).and_then(|v| {
            if v == 0 || v == u64::MAX || v == u32::MAX as u64 {
                None
            } else {
                Some(v)
            }
        });

        out.push(NetDevSnapshot {
            name,
            stable_id,
            interface_index: cache.read_u64(sys.join("ifindex")).map(|v| v as u32),
            interface_luid: None,
            is_virtual: Some(is_virtual),
            is_loopback: Some(is_loopback),
            is_physical: Some(is_physical),
            is_primary: Some(is_primary),
            mtu: cache.read_u64(sys.join("mtu")),
            speed_mbps,
            tx_queue_len: cache.read_u64(sys.join("tx_queue_len")),
            carrier_up: cache.read_bool_num(sys.join("carrier")),
            rx_bytes: data[0].parse().unwrap_or(0),
            rx_packets: data[1].parse().unwrap_or(0),
            rx_errs: data[2].parse().unwrap_or(0),
            rx_drop: data[3].parse().unwrap_or(0),
            rx_fifo: data[4].parse().unwrap_or(0),
            rx_frame: data[5].parse().unwrap_or(0),
            rx_compressed: data[6].parse().unwrap_or(0),
            rx_multicast: data[7].parse().unwrap_or(0),
            tx_bytes: data[8].parse().unwrap_or(0),
            tx_packets: data[9].parse().unwrap_or(0),
            tx_errs: data[10].parse().unwrap_or(0),
            tx_drop: data[11].parse().unwrap_or(0),
            tx_fifo: data[12].parse().unwrap_or(0),
            tx_colls: data[13].parse().unwrap_or(0),
            tx_carrier: data[14].parse().unwrap_or(0),
            tx_compressed: data[15].parse().unwrap_or(0),
        });
    }

    Ok(out)
}

fn count_dir_entries(path: &Path) -> Option<u64> {
    let mut count = 0u64;
    let dir = fs::read_dir(path).ok()?;
    for entry in dir {
        if entry.is_ok() {
            count += 1;
        }
    }
    Some(count)
}

fn estimate_process_count_from_proc() -> Option<usize> {
    let entries = fs::read_dir("/proc").ok()?;
    let mut count = 0usize;
    for entry in entries.flatten() {
        let name = entry.file_name();
        if name
            .to_str()
            .map(|v| v.as_bytes().iter().all(|b| b.is_ascii_digit()))
            .unwrap_or(false)
        {
            count += 1;
        }
    }
    Some(count)
}

fn should_scan_fd_counts(process_count: usize) -> bool {
    if let Ok(raw) = std::env::var("OJO_PROCESS_FD_SCAN") {
        let normalized = raw.trim().to_ascii_lowercase();
        if matches!(normalized.as_str(), "0" | "false" | "no" | "off") {
            return false;
        }
        if matches!(normalized.as_str(), "1" | "true" | "yes" | "on") {
            return true;
        }
    }

    process_count <= PROCESS_FD_SCAN_LIMIT
}

fn collect_processes(
    cache: &mut ReadCache,
) -> Result<(Vec<ProcessSnapshot>, ProcessCollectionMeta)> {
    let mut out = Vec::new();
    let page_size = page_size_bytes();
    let process_count_hint = estimate_process_count_from_proc().unwrap_or(0);
    let fd_scan_enabled = should_scan_fd_counts(process_count_hint);

    for entry in all_processes()? {
        let Ok(process) = entry else { continue };
        let Ok(stat) = process.stat() else { continue };

        let io = process.io().ok();
        let proc_dir = Path::new("/proc").join(stat.pid.to_string());
        let status_path = proc_dir.join("status");
        let status_fields = cache
            .read_raw(&status_path)
            .map(parse_process_status_fields)
            .unwrap_or_default();

        out.push(ProcessSnapshot {
            pid: stat.pid,
            ppid: stat.ppid,
            comm: stat.comm,
            state: stat.state.to_string(),
            num_threads: stat.num_threads,
            priority: stat.priority,
            nice: stat.nice,
            minflt: stat.minflt,
            majflt: stat.majflt,
            vsize_bytes: stat.vsize,
            rss_pages: u64_to_i64(stat.rss),
            virtual_size_bytes: Some(stat.vsize),
            resident_bytes: status_fields
                .vm_rss_kib
                .map(|kib| kib.saturating_mul(1024))
                .or_else(|| Some(stat.rss.saturating_mul(page_size))),
            utime_ticks: stat.utime,
            stime_ticks: stat.stime,
            start_time_ticks: stat.starttime,
            processor: stat.processor.map(|value| value as i64),
            rt_priority: stat.rt_priority.map(|value| value as u64),
            policy: stat.policy.map(|value| value as u64),
            oom_score: cache.read_i64_first(proc_dir.join("oom_score")),
            fd_count: if fd_scan_enabled {
                count_dir_entries(&proc_dir.join("fd"))
            } else {
                None
            },
            fd_table_size: status_fields.fd_table_size,
            read_chars: io.as_ref().map(|v| v.rchar),
            write_chars: io.as_ref().map(|v| v.wchar),
            syscr: io.as_ref().map(|v| v.syscr),
            syscw: io.as_ref().map(|v| v.syscw),
            read_bytes: io.as_ref().map(|v| v.read_bytes),
            write_bytes: io.as_ref().map(|v| v.write_bytes),
            cancelled_write_bytes: io.as_ref().map(|v| u64_to_i64(v.cancelled_write_bytes)),
            vm_size_kib: status_fields.vm_size_kib,
            vm_rss_kib: status_fields.vm_rss_kib,
            vm_data_kib: status_fields.vm_data_kib,
            vm_stack_kib: status_fields.vm_stack_kib,
            vm_exe_kib: status_fields.vm_exe_kib,
            vm_lib_kib: status_fields.vm_lib_kib,
            vm_swap_kib: status_fields.vm_swap_kib,
            vm_pte_kib: status_fields.vm_pte_kib,
            vm_hwm_kib: status_fields.vm_hwm_kib,
            working_set_bytes: None,
            private_bytes: None,
            peak_working_set_bytes: None,
            pagefile_usage_bytes: None,
            commit_charge_bytes: None,
            voluntary_ctxt_switches: status_fields.voluntary_ctxt_switches,
            nonvoluntary_ctxt_switches: status_fields.nonvoluntary_ctxt_switches,
        });
    }

    Ok((out, ProcessCollectionMeta { fd_scan_enabled }))
}
