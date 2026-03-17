use crate::model::{
    CpuInfoSnapshot, CpuTimes, CpuTimesSeconds, DiskSnapshot, LoadSnapshot, MemorySnapshot,
    MountSnapshot, NetDevSnapshot, ProcessSnapshot, Snapshot, SoftnetCpuSnapshot,
    SwapDeviceSnapshot, SystemSnapshot,
};
use anyhow::Result;
use procfs::{process::all_processes, Current, CurrentSI};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

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
        self.read_raw(path)?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    }

    fn read_bool_num(&mut self, path: impl AsRef<Path>) -> Option<bool> {
        match self.read_trimmed(path)? {
            "0" => Some(false),
            "1" => Some(true),
            _ => None,
        }
    }
}

fn linux_support_state() -> BTreeMap<String, String> {
    BTreeMap::new()
}

fn linux_metric_classification() -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();

    // Core system metrics
    out.insert("system.cpu.time".to_string(), "counter".to_string());
    out.insert(
        "system.cpu.utilization".to_string(),
        "gauge_derived_ratio".to_string(),
    );

    // Load averages
    out.insert("system.cpu.load_average.1m".to_string(), "gauge".to_string());
    out.insert("system.cpu.load_average.5m".to_string(), "gauge".to_string());
    out.insert("system.cpu.load_average.15m".to_string(), "gauge".to_string());
    out.insert("system.linux.load.runnable".to_string(), "gauge".to_string());
    out.insert("system.linux.load.entities".to_string(), "gauge".to_string());
    out.insert("system.linux.load.latest_pid".to_string(), "gauge".to_string());

    // Memory / swap
    out.insert("system.memory.*".to_string(), "gauge".to_string());
    out.insert("system.swap.*".to_string(), "gauge".to_string());

    // Pressure
    out.insert("system.linux.pressure".to_string(), "gauge_ratio".to_string());
    out.insert("system.linux.pressure.stall_time".to_string(), "counter".to_string());

    // Kernel counters
    out.insert("system.context_switches".to_string(), "counter".to_string());
    out.insert("system.linux.interrupts".to_string(), "counter".to_string());
    out.insert("system.linux.softirqs".to_string(), "counter".to_string());

    // vmstat is a snapshot of kernel counters; exposed as gauges
    out.insert("system.linux.vmstat".to_string(), "gauge".to_string());

    // Disk metrics
    out.insert("system.disk.io".to_string(), "counter".to_string());
    out.insert("system.disk.operations".to_string(), "counter".to_string());
    out.insert("system.disk.operation_time".to_string(), "counter".to_string());
    out.insert("system.disk.io_time".to_string(), "counter".to_string());
    out.insert("system.disk.pending_operations".to_string(), "gauge".to_string());
    out.insert("system.disk.utilization".to_string(), "gauge_derived_ratio".to_string());
    out.insert("system.disk.queue_depth".to_string(), "gauge".to_string());
    out.insert("system.disk.*_per_sec".to_string(), "gauge_derived".to_string());
    out.insert("system.disk.*".to_string(), "gauge".to_string());

    // Network metrics
    out.insert("system.network.io".to_string(), "counter".to_string());
    out.insert("system.network.packets".to_string(), "counter".to_string());
    out.insert("system.network.errors".to_string(), "counter".to_string());
    out.insert("system.network.dropped".to_string(), "counter".to_string());
    out.insert("system.network.*_per_sec".to_string(), "gauge_derived".to_string());
    out.insert("system.network.*".to_string(), "gauge".to_string());

    // net-snmp counters (exposed as gauge values)
    out.insert("system.linux.net.snmp".to_string(), "counter".to_string());

    // Process metrics
    out.insert("process.cpu.time".to_string(), "counter".to_string());
    out.insert("process.disk.io".to_string(), "counter".to_string());
    out.insert("process.io.chars".to_string(), "counter".to_string());
    out.insert("process.io.syscalls".to_string(), "counter".to_string());
    out.insert("process.context_switches".to_string(), "counter".to_string());
    out.insert("process.paging.faults".to_string(), "counter".to_string());
    out.insert("process.memory.usage".to_string(), "gauge".to_string());
    out.insert("process.open_file_descriptors".to_string(), "gauge".to_string());
    out.insert("process.oom_score".to_string(), "gauge".to_string());
    out.insert("process.cpu.last_id".to_string(), "gauge".to_string());
    out.insert("process.start_time".to_string(), "gauge".to_string());
    out.insert("process.linux.start_time".to_string(), "gauge".to_string());
    out.insert("process.linux.scheduler".to_string(), "gauge".to_string());

    // Misc
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

fn read_cpu_frequency_mhz(cache: &mut ReadCache, cpu: usize) -> Option<f64> {
    let base = Path::new("/sys/devices/system/cpu").join(format!("cpu{cpu}/cpufreq"));
    let khz = cache
        .read_u64(base.join("scaling_cur_freq"))
        .or_else(|| cache.read_u64(base.join("cpuinfo_cur_freq")))
        .or_else(|| cache.read_u64(base.join("base_frequency")))?;
    Some(khz as f64 / 1000.0)
}

fn page_size_bytes() -> u64 {
    (procfs::page_size() as u64).max(1)
}

pub fn collect_snapshot(include_process_metrics: bool) -> Result<Snapshot> {
    let mut cache = ReadCache::default();

    let (processes, process_count_hint) = if include_process_metrics {
        let processes = collect_processes(&mut cache)?;
        let count = processes.len() as u64;
        (processes, Some(count))
    } else {
        (Vec::new(), None)
    };

    Ok(Snapshot {
        system: collect_system(&mut cache, process_count_hint)?,
        memory: collect_memory()?,
        load: Some(collect_load()?),
        pressure: collect_pressure()?,
        pressure_totals_us: collect_pressure_totals()?,
        vmstat: procfs::vmstat().unwrap_or_default().into_iter().collect(),
        interrupts: collect_interrupts()?,
        softirqs: collect_softirqs()?,
        net_snmp: collect_net_snmp()?,
        sockets: collect_sockets()?,
        softnet: collect_softnet()?,
        swaps: collect_swaps()?,
        mounts: collect_mounts()?,
        cpuinfo: collect_cpuinfo(&mut cache)?,
        zoneinfo: collect_zoneinfo()?,
        buddyinfo: collect_buddyinfo()?,
        disks: collect_disks(&mut cache)?,
        net: collect_net(&mut cache)?,
        processes,
        support_state: linux_support_state(),
        metric_classification: linux_metric_classification(),
        windows: None,
    })
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
    /// FDSize is the size of the process' file descriptor table allocation (not the number of open fds).
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

        out.push(MountSnapshot {
            device: cols[0].replace("\\040", " "),
            mountpoint: cols[1].replace("\\040", " "),
            fs_type: cols[2].to_string(),
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
            "vendor_id" => current.vendor_id = Some(value.to_string()),
            "model name" => current.model_name = Some(value.to_string()),
            "cpu MHz" => current.mhz = value.parse::<f64>().ok(),
            "cache size" => {
                let size_kib = value
                    .split_whitespace()
                    .next()
                    .and_then(|v| v.parse::<u64>().ok());
                current.cache_size_bytes = size_kib.map(|v| v.saturating_mul(1024));
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

fn collect_disks(cache: &mut ReadCache) -> Result<Vec<DiskSnapshot>> {
    let stats = procfs::DiskStats::current()?;
    Ok(stats
        .0
        .into_iter()
        .filter(|d| Path::new("/sys/block").join(&d.name).exists())
        .map(|d| {
            let base = Path::new("/sys/block").join(&d.name).join("queue");
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
                logical_block_size: cache.read_u64(base.join("logical_block_size")),
                physical_block_size: cache.read_u64(base.join("physical_block_size")),
                rotational: cache.read_bool_num(base.join("rotational")),
            }
        })
        .collect())
}

fn collect_net(cache: &mut ReadCache) -> Result<Vec<NetDevSnapshot>> {
    let devs = fs::read_to_string("/proc/net/dev")?;
    let mut out = Vec::new();

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

        out.push(NetDevSnapshot {
            name,
            stable_id: cache
                .read_trimmed(sys.join("ifindex"))
                .map(|v| format!("ifindex:{v}")),
            interface_index: cache.read_u64(sys.join("ifindex")).map(|v| v as u32),
            interface_luid: None,
            is_virtual: None,
            is_loopback: Some(is_loopback),
            is_physical: None,
            is_primary: None,
            mtu: cache.read_u64(sys.join("mtu")),
            speed_mbps: cache.read_u64(sys.join("speed")),
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

fn collect_processes(cache: &mut ReadCache) -> Result<Vec<ProcessSnapshot>> {
    let mut out = Vec::new();
    let page_size = page_size_bytes();

    for entry in all_processes()? {
        let Ok(process) = entry else { continue };
        let Ok(stat) = process.stat() else { continue };

        let io = process.io().ok();
        let proc_dir = Path::new("/proc").join(stat.pid.to_string());
        let status_path = proc_dir.join("status");
        let status_fields = cache
            .read_raw(&status_path)
            .map(|contents| parse_process_status_fields(&contents))
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
                .or_else(|| Some((stat.rss as u64).saturating_mul(page_size))),
            utime_ticks: stat.utime,
            stime_ticks: stat.stime,
            start_time_ticks: stat.starttime,
            processor: stat.processor.map(|value| value as i64),
            rt_priority: stat.rt_priority.map(|value| value as u64),
            policy: stat.policy.map(|value| value as u64),
            oom_score: cache.read_i64_first(proc_dir.join("oom_score")),
            fd_count: count_dir_entries(&proc_dir.join("fd")).or(status_fields.fd_table_size),
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

    Ok(out)
}