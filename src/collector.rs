use crate::model::{
    CpuTimes, DiskSnapshot, LoadSnapshot, MemorySnapshot, NetDevSnapshot, ProcessSnapshot,
    Snapshot, SoftnetCpuSnapshot, SystemSnapshot,
};
use anyhow::Result;
use procfs::process::all_processes;
use procfs::{Current, CurrentSI};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

fn opt_u64(value: Option<u64>) -> u64 {
    value.unwrap_or(0)
}

fn u64_to_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

fn read_sysfs_value(path: impl AsRef<Path>) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn read_sysfs_u64(path: impl AsRef<Path>) -> Option<u64> {
    read_sysfs_value(path)?.parse().ok()
}

fn read_sysfs_bool_num(path: impl AsRef<Path>) -> Option<bool> {
    match read_sysfs_value(path)?.as_str() {
        "0" => Some(false),
        "1" => Some(true),
        _ => None,
    }
}

pub fn collect_snapshot(include_process_metrics: bool) -> Result<Snapshot> {
    Ok(Snapshot {
        system: collect_system()?,
        memory: collect_memory()?,
        load: collect_load()?,
        pressure: collect_pressure()?,
        pressure_totals_us: collect_pressure_totals()?,
        meminfo_raw: collect_meminfo_raw()?,
        vmstat: procfs::vmstat().unwrap_or_default().into_iter().collect(),
        net_snmp: collect_net_snmp()?,
        softnet: collect_softnet()?,
        disks: collect_disks()?,
        net: collect_net()?,
        processes: if include_process_metrics {
            collect_processes()?
        } else {
            Vec::new()
        },
    })
}

fn read_proc_first_value(path: impl AsRef<Path>) -> Option<String> {
    fs::read_to_string(path)
        .ok()
        .and_then(|value| value.split_whitespace().next().map(str::to_string))
}

fn read_proc_u64(path: impl AsRef<Path>) -> Option<u64> {
    read_proc_first_value(path)?.parse().ok()
}

fn read_proc_i64(path: impl AsRef<Path>) -> Option<i64> {
    read_proc_first_value(path)?.parse().ok()
}

fn parse_status_kib(path: &Path, key: &str) -> Option<u64> {
    let prefix = format!("{key}:");
    for line in fs::read_to_string(path).ok()?.lines() {
        if let Some(rest) = line.strip_prefix(&prefix) {
            return rest.split_whitespace().next()?.parse().ok();
        }
    }
    None
}

fn parse_status_u64(path: &Path, key: &str) -> Option<u64> {
    let prefix = format!("{key}:");
    for line in fs::read_to_string(path).ok()?.lines() {
        if let Some(rest) = line.strip_prefix(&prefix) {
            return rest.split_whitespace().next()?.parse().ok();
        }
    }
    None
}

fn count_fds(pid: i32) -> Option<u64> {
    let path = Path::new("/proc").join(pid.to_string()).join("fd");
    Some(fs::read_dir(path).ok()?.filter_map(|entry| entry.ok()).count() as u64)
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
                    out.insert(format!("{resource}.{scope}.{name}"), parsed / 100.0);
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
                    out.insert(format!("{resource}.{scope}"), parsed);
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

fn collect_meminfo_raw() -> Result<BTreeMap<String, u64>> {
    let contents = std::fs::read_to_string("/proc/meminfo")?;
    let mut out = BTreeMap::new();

    for line in contents.lines() {
        let mut parts = line.split_whitespace();
        let Some(raw_key) = parts.next() else { continue };
        let key = raw_key.trim_end_matches(':').to_string();
        let Some(value) = parts.next() else { continue };
        let Ok(mut parsed) = value.parse::<u64>() else { continue };
        if matches!(parts.next(), Some("kB")) {
            parsed = parsed.saturating_mul(1024);
        }
        out.insert(key, parsed);
    }

    Ok(out)
}

fn collect_net_snmp() -> Result<BTreeMap<String, u64>> {
    let contents = std::fs::read_to_string("/proc/net/snmp")?;
    let mut out = BTreeMap::new();
    let mut pending: Option<(String, Vec<String>)> = None;

    for line in contents.lines() {
        let mut parts = line.split_whitespace();
        let Some(raw_prefix) = parts.next() else { continue };
        let prefix = raw_prefix.trim_end_matches(':').to_string();
        let cols = parts.map(str::to_string).collect::<Vec<_>>();

        if let Some((pending_prefix, headers)) = pending.take() {
            if pending_prefix == prefix {
                for (header, value) in headers.iter().zip(cols.iter()) {
                    if let Ok(parsed) = value.parse::<u64>() {
                        out.insert(format!("{prefix}.{header}"), parsed);
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

fn collect_softnet() -> Result<Vec<SoftnetCpuSnapshot>> {
    let contents = std::fs::read_to_string("/proc/net/softnet_stat")?;
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

fn collect_system() -> Result<SystemSnapshot> {
    let stat = procfs::KernelStats::current()?;
    let (interrupts_total, softirqs_total) = collect_proc_stat_totals()?;
    let uptime_secs = collect_uptime_secs()?;

    let per_cpu = stat
        .cpu_time
        .iter()
        .map(|cpu| CpuTimes {
            user: cpu.user,
            nice: cpu.nice,
            system: cpu.system,
            idle: cpu.idle,
            iowait: opt_u64(cpu.iowait),
            irq: opt_u64(cpu.irq),
            softirq: opt_u64(cpu.softirq),
            steal: opt_u64(cpu.steal),
            guest: opt_u64(cpu.guest),
            guest_nice: opt_u64(cpu.guest_nice),
        })
        .collect();

    Ok(SystemSnapshot {
        ticks_per_second: procfs::ticks_per_second() as u64,
        boot_time_epoch_secs: stat.btime,
        uptime_secs,
        context_switches: stat.ctxt,
        forks_since_boot: stat.processes,
        interrupts_total,
        softirqs_total,
        process_count: all_processes()?.count() as u64,
        pid_max: read_proc_u64("/proc/sys/kernel/pid_max").unwrap_or(0),
        entropy_available_bits: read_proc_u64("/proc/sys/kernel/random/entropy_avail").unwrap_or(0),
        entropy_pool_size_bits: read_proc_u64("/proc/sys/kernel/random/poolsize").unwrap_or(0),
        procs_running: stat.procs_running.unwrap_or(0),
        procs_blocked: stat.procs_blocked.unwrap_or(0),
        cpu_total: CpuTimes {
            user: stat.total.user,
            nice: stat.total.nice,
            system: stat.total.system,
            idle: stat.total.idle,
            iowait: opt_u64(stat.total.iowait),
            irq: opt_u64(stat.total.irq),
            softirq: opt_u64(stat.total.softirq),
            steal: opt_u64(stat.total.steal),
            guest: opt_u64(stat.total.guest),
            guest_nice: opt_u64(stat.total.guest_nice),
        },
        per_cpu,
    })
}

fn collect_memory() -> Result<MemorySnapshot> {
    let mem = procfs::Meminfo::current()?;
    Ok(MemorySnapshot {
        mem_total_bytes: mem.mem_total,
        mem_free_bytes: mem.mem_free,
        mem_available_bytes: opt_u64(mem.mem_available),
        buffers_bytes: mem.buffers,
        cached_bytes: mem.cached,
        active_bytes: mem.active,
        inactive_bytes: mem.inactive,
        anon_pages_bytes: opt_u64(mem.anon_pages),
        mapped_bytes: mem.mapped,
        shmem_bytes: opt_u64(mem.shmem),
        swap_total_bytes: mem.swap_total,
        swap_free_bytes: mem.swap_free,
        swap_cached_bytes: mem.swap_cached,
        dirty_bytes: mem.dirty,
        writeback_bytes: mem.writeback,
        slab_bytes: mem.slab,
        sreclaimable_bytes: opt_u64(mem.s_reclaimable),
        sunreclaim_bytes: opt_u64(mem.s_unreclaim),
        page_tables_bytes: opt_u64(mem.page_tables),
        committed_as_bytes: mem.committed_as,
        commit_limit_bytes: opt_u64(mem.commit_limit),
        kernel_stack_bytes: opt_u64(mem.kernel_stack),
        hugepages_total: opt_u64(mem.hugepages_total),
        hugepages_free: opt_u64(mem.hugepages_free),
        hugepage_size_bytes: opt_u64(mem.hugepagesize),
        anon_hugepages_bytes: opt_u64(mem.anon_hugepages),
    })
}

fn collect_load() -> Result<LoadSnapshot> {
    let load = procfs::LoadAverage::current()?;
    Ok(LoadSnapshot {
        one: load.one as f64,
        five: load.five as f64,
        fifteen: load.fifteen as f64,
        runnable: load.cur as u32,
        entities: load.max as u32,
        latest_pid: load.latest_pid as u32,
    })
}

fn collect_disks() -> Result<Vec<DiskSnapshot>> {
    let stats = procfs::DiskStats::current()?;
    Ok(stats
        .0
        .into_iter()
        .map(|d| {
            let base = Path::new("/sys/block").join(&d.name).join("queue");
            DiskSnapshot {
                name: d.name.clone(),
                reads: d.reads,
                writes: d.writes,
                sectors_read: d.sectors_read,
                sectors_written: d.sectors_written,
                time_reading_ms: d.time_reading,
                time_writing_ms: d.time_writing,
                in_progress: d.in_progress,
                time_in_progress_ms: d.time_in_progress,
                weighted_time_in_progress_ms: d.weighted_time_in_progress,
                logical_block_size: read_sysfs_u64(base.join("logical_block_size")),
                physical_block_size: read_sysfs_u64(base.join("physical_block_size")),
                rotational: read_sysfs_bool_num(base.join("rotational")),
            }
        })
        .collect())
}

fn collect_net() -> Result<Vec<NetDevSnapshot>> {
    let devs = std::fs::read_to_string("/proc/net/dev")?;
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

        out.push(NetDevSnapshot {
            name,
            mtu: read_sysfs_u64(sys.join("mtu")),
            speed_mbps: read_sysfs_u64(sys.join("speed")),
            tx_queue_len: read_sysfs_u64(sys.join("tx_queue_len")),
            carrier_up: read_sysfs_bool_num(sys.join("carrier")),
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

fn collect_processes() -> Result<Vec<ProcessSnapshot>> {
    let mut out = Vec::new();

    for entry in all_processes()? {
        let Ok(process) = entry else { continue };
        let Ok(stat) = process.stat() else { continue };

        let status = process.status().ok();
        let io = process.io().ok();
        let proc_dir = Path::new("/proc").join(stat.pid.to_string());
        let status_path = proc_dir.join("status");

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
            utime_ticks: stat.utime,
            stime_ticks: stat.stime,
            start_time_ticks: stat.starttime,
            processor: stat.processor.map(|value| value as i64),
            rt_priority: stat.rt_priority.map(|value| value as u64),
            policy: stat.policy.map(|value| value as u64),
            oom_score: read_proc_i64(proc_dir.join("oom_score")),
            fd_count: count_fds(stat.pid),
            read_chars: io.as_ref().map(|v| v.rchar),
            write_chars: io.as_ref().map(|v| v.wchar),
            syscr: io.as_ref().map(|v| v.syscr),
            syscw: io.as_ref().map(|v| v.syscw),
            read_bytes: io.as_ref().map(|v| v.read_bytes),
            write_bytes: io.as_ref().map(|v| v.write_bytes),
            cancelled_write_bytes: io
                .as_ref()
                .map(|v| u64_to_i64(v.cancelled_write_bytes)),
            vm_size_kib: status.as_ref().and_then(|s| s.vmsize),
            vm_rss_kib: status.as_ref().and_then(|s| s.vmrss),
            vm_data_kib: parse_status_kib(&status_path, "VmData"),
            vm_stack_kib: parse_status_kib(&status_path, "VmStk"),
            vm_exe_kib: parse_status_kib(&status_path, "VmExe"),
            vm_lib_kib: parse_status_kib(&status_path, "VmLib"),
            vm_swap_kib: parse_status_kib(&status_path, "VmSwap"),
            vm_pte_kib: parse_status_kib(&status_path, "VmPTE"),
            vm_hwm_kib: parse_status_kib(&status_path, "VmHWM"),
            voluntary_ctxt_switches: parse_status_u64(&status_path, "voluntary_ctxt_switches"),
            nonvoluntary_ctxt_switches: parse_status_u64(
                &status_path,
                "nonvoluntary_ctxt_switches",
            ),
        });
    }

    Ok(out)
}
