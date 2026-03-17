use crate::model::{
    CpuInfoSnapshot, CpuTimes, CpuTimesSeconds, DiskSnapshot, LoadSnapshot, MemorySnapshot,
    MountSnapshot, NetDevSnapshot, ProcessSnapshot, Snapshot, SoftnetCpuSnapshot,
    SwapDeviceSnapshot, SystemSnapshot,
};
use anyhow::{Context, Result};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

type KstatMap = BTreeMap<String, String>;

#[derive(Default)]
struct DiskAccum {
    name: String,
    reads: u64,
    writes: u64,
    nread: u64,
    nwritten: u64,
    rtime_ns: u64,
    wtime_ns: u64,
    rlentime_ns: u64,
    wlentime_ns: u64,
    rcnt: u64,
    wcnt: u64,
    seen: bool,
}

#[derive(Default)]
struct NetAccum {
    name: String,
    rx_bytes: u64,
    tx_bytes: u64,
    rx_packets: u64,
    tx_packets: u64,
    rx_errs: u64,
    tx_errs: u64,
    collisions: u64,
    rx_multicast: u64,
    speed_bps: u64,
    mtu: Option<u64>,
    tx_queue_len: Option<u64>,
    carrier_up: Option<bool>,
    seen: bool,
}

fn solaris_support_state() -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    out.insert("collector".to_string(), "supported".to_string());
    out.insert("system".to_string(), "supported".to_string());
    out.insert("memory".to_string(), "supported".to_string());
    out.insert("load".to_string(), "supported".to_string());
    out.insert("disks".to_string(), "supported".to_string());
    out.insert("network".to_string(), "supported".to_string());
    out.insert("mounts".to_string(), "supported".to_string());
    out.insert("swaps".to_string(), "supported".to_string());
    out.insert("cpuinfo".to_string(), "supported".to_string());
    out.insert("processes".to_string(), "partial".to_string());
    out.insert("pressure".to_string(), "unsupported".to_string());
    out.insert("interrupts".to_string(), "unsupported".to_string());
    out.insert("softirqs".to_string(), "unsupported".to_string());
    out.insert("softnet".to_string(), "unsupported".to_string());
    out.insert("zoneinfo".to_string(), "unsupported".to_string());
    out.insert("buddyinfo".to_string(), "unsupported".to_string());
    out.insert("net_snmp".to_string(), "unsupported".to_string());
    out.insert("sockets".to_string(), "unsupported".to_string());
    out
}

fn solaris_metric_classification() -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();

    out.insert("system.cpu.time".to_string(), "counter".to_string());
    out.insert("system.cpu.utilization".to_string(), "gauge_derived_ratio".to_string());
    out.insert("system.cpu.load_average.*".to_string(), "gauge".to_string());

    out.insert("system.memory.*".to_string(), "gauge".to_string());
    out.insert("system.swap.*".to_string(), "gauge".to_string());

    out.insert("system.context_switches".to_string(), "counter".to_string());
    out.insert("system.interrupts".to_string(), "counter".to_string());
    out.insert("system.vmstat".to_string(), "gauge".to_string());

    out.insert("system.disk.io".to_string(), "counter".to_string());
    out.insert("system.disk.operations".to_string(), "counter".to_string());
    out.insert("system.disk.operation_time".to_string(), "counter".to_string());
    out.insert("system.disk.pending_operations".to_string(), "gauge".to_string());

    out.insert("system.network.io".to_string(), "counter".to_string());
    out.insert("system.network.packets".to_string(), "counter".to_string());
    out.insert("system.network.errors".to_string(), "counter".to_string());

    out.insert("process.cpu.time".to_string(), "counter".to_string());
    out.insert("process.memory.usage".to_string(), "gauge".to_string());
    out.insert("process.open_file_descriptors".to_string(), "gauge".to_string());

    out.insert("system.mounts.*".to_string(), "state".to_string());
    out.insert("system.cpuinfo.*".to_string(), "inventory".to_string());

    out
}

fn u64_to_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

fn run_command(cmd: &str, args: &[&str]) -> Result<String> {
    let output = Command::new(cmd)
        .args(args)
        .output()
        .with_context(|| format!("failed to run {cmd}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("{cmd} failed: {}", stderr.trim());
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn run_command_optional(cmd: &str, args: &[&str]) -> Option<String> {
    Command::new(cmd)
        .args(args)
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).to_string())
}

fn split_first_whitespace(s: &str) -> Option<(&str, &str)> {
    let idx = s.find(char::is_whitespace)?;
    let left = &s[..idx];
    let right = s[idx..].trim();
    Some((left, right))
}

fn parse_kstat_map(text: &str) -> KstatMap {
    let mut out = BTreeMap::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let Some((key, value)) = split_first_whitespace(line) else {
            continue;
        };

        out.insert(key.to_string(), value.to_string());
    }

    out
}

fn get_kstats() -> Result<KstatMap> {
    Ok(parse_kstat_map(&run_command("kstat", &["-p"])?))
}

fn split_kstat_key(key: &str) -> Option<(&str, &str, &str, &str)> {
    let mut parts = key.splitn(4, ':');
    Some((parts.next()?, parts.next()?, parts.next()?, parts.next()?))
}

fn kstat_u64(kstats: &KstatMap, key: &str) -> u64 {
    kstats
        .get(key)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0)
}

fn kstat_str<'a>(kstats: &'a KstatMap, key: &str) -> Option<&'a str> {
    kstats.get(key).map(String::as_str)
}

fn count_numeric_dirs(path: &str) -> u64 {
    fs::read_dir(path)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .chars()
                .all(|c| c.is_ascii_digit())
        })
        .count() as u64
}

fn read_ticks_per_second() -> u64 {
    run_command_optional("getconf", &["CLK_TCK"])
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(100)
}

fn read_page_size() -> u64 {
    run_command_optional("getconf", &["PAGESIZE"])
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(4096)
}

fn count_fds(pid: i32) -> Option<u64> {
    let path = Path::new("/proc").join(pid.to_string()).join("fd");
    Some(fs::read_dir(path).ok()?.filter_map(|e| e.ok()).count() as u64)
}

fn decode_mount_field(value: &str) -> String {
    value.replace("\\040", " ")
}

fn parse_cpu_time_to_ticks(value: &str, ticks_per_second: u64) -> u64 {
    let value = value.trim();
    if value.is_empty() {
        return 0;
    }

    let (days, rest) = if let Some((d, r)) = value.split_once('-') {
        (d.parse::<u64>().unwrap_or(0), r)
    } else {
        (0, value)
    };

    let parts = rest
        .split(':')
        .filter_map(|p| p.parse::<u64>().ok())
        .collect::<Vec<_>>();

    let secs = match parts.as_slice() {
        [h, m, s] => {
            days.saturating_mul(86_400)
                .saturating_add(h.saturating_mul(3600))
                .saturating_add(m.saturating_mul(60))
                .saturating_add(*s)
        }
        [m, s] => days
            .saturating_mul(86_400)
            .saturating_add(m.saturating_mul(60))
            .saturating_add(*s),
        [s] => days.saturating_mul(86_400).saturating_add(*s),
        _ => 0,
    };

    secs.saturating_mul(ticks_per_second)
}

fn empty_cpu_times() -> CpuTimes {
    CpuTimes {
        user: 0,
        nice: 0,
        system: 0,
        idle: 0,
        iowait: 0,
        irq: 0,
        softirq: 0,
        steal: 0,
        guest: 0,
        guest_nice: 0,
    }
}

fn cpu_times_to_seconds(cpu: &CpuTimes, ticks_per_second: u64) -> CpuTimesSeconds {
    let hz = ticks_per_second.max(1) as f64;
    CpuTimesSeconds {
        user: cpu.user as f64 / hz,
        nice: cpu.nice as f64 / hz,
        system: cpu.system as f64 / hz,
        idle: cpu.idle as f64 / hz,
        iowait: cpu.iowait as f64 / hz,
        irq: cpu.irq as f64 / hz,
        softirq: cpu.softirq as f64 / hz,
        steal: cpu.steal as f64 / hz,
        guest: cpu.guest as f64 / hz,
        guest_nice: cpu.guest_nice as f64 / hz,
    }
}

pub fn collect_snapshot(include_process_metrics: bool) -> Result<Snapshot> {
    let kstats = get_kstats()?;
    let page_size = read_page_size();
    let ticks_per_second = read_ticks_per_second();

    let processes = if include_process_metrics {
        collect_processes(page_size, ticks_per_second)?
    } else {
        Vec::new()
    };

    let process_count_hint = if include_process_metrics {
        Some(processes.len() as u64)
    } else {
        None
    };

    let swaps = collect_swaps()?;

    Ok(Snapshot {
        system: collect_system(&kstats, process_count_hint)?,
        memory: collect_memory(&kstats, &swaps, page_size)?,
        load: Some(collect_load(&kstats)?),
        pressure: BTreeMap::new(),
        pressure_totals_us: BTreeMap::new(),
        vmstat: collect_vmstat(&kstats),
        interrupts: BTreeMap::new(),
        softirqs: BTreeMap::new(),
        net_snmp: BTreeMap::new(),
        sockets: BTreeMap::new(),
        softnet: Vec::<SoftnetCpuSnapshot>::new(),
        swaps,
        mounts: collect_mounts()?,
        cpuinfo: collect_cpuinfo(&kstats)?,
        zoneinfo: BTreeMap::new(),
        buddyinfo: BTreeMap::new(),
        disks: collect_disks(&kstats)?,
        net: collect_net(&kstats)?,
        processes,
        support_state: solaris_support_state(),
        metric_classification: solaris_metric_classification(),
        windows: None,
    })
}

fn collect_system(kstats: &KstatMap, process_count: Option<u64>) -> Result<SystemSnapshot> {
    let ticks_per_second = read_ticks_per_second();
    let boot_time = kstat_u64(kstats, "unix:0:system_misc:boot_time");

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time before unix epoch")?
        .as_secs_f64();

    let uptime_secs = if boot_time > 0 {
        (now - boot_time as f64).max(0.0)
    } else {
        0.0
    };

    let mut per_cpu_map: BTreeMap<usize, CpuTimes> = BTreeMap::new();
    let mut context_switches = 0u64;
    let mut interrupts_total = 0u64;

    for (key, value) in kstats {
        let Some((module, instance, name, stat)) = split_kstat_key(key) else {
            continue;
        };

        if module == "cpu" && name == "sys" {
            let cpu_id = instance.parse::<usize>().unwrap_or(0);
            let cpu = per_cpu_map.entry(cpu_id).or_insert_with(empty_cpu_times);
            let parsed = value.parse::<u64>().unwrap_or(0);

            match stat {
                "cpu_ticks_user" => cpu.user = parsed,
                "cpu_ticks_kernel" => cpu.system = parsed,
                "cpu_ticks_idle" => cpu.idle = parsed,
                "cpu_ticks_wait" => cpu.iowait = parsed,
                _ => {}
            }
        }

        if module == "cpu_stat" && name == "sys" {
            let parsed = value.parse::<u64>().unwrap_or(0);
            match stat {
                "pswitch" | "inv_swtch" => {
                    context_switches = context_switches.saturating_add(parsed);
                }
                "intr" => {
                    interrupts_total = interrupts_total.saturating_add(parsed);
                }
                _ => {}
            }
        }
    }

    let per_cpu = per_cpu_map.into_values().collect::<Vec<_>>();
    let cpu_total = per_cpu.iter().fold(empty_cpu_times(), |mut acc, cpu| {
        acc.user = acc.user.saturating_add(cpu.user);
        acc.nice = acc.nice.saturating_add(cpu.nice);
        acc.system = acc.system.saturating_add(cpu.system);
        acc.idle = acc.idle.saturating_add(cpu.idle);
        acc.iowait = acc.iowait.saturating_add(cpu.iowait);
        acc.irq = acc.irq.saturating_add(cpu.irq);
        acc.softirq = acc.softirq.saturating_add(cpu.softirq);
        acc.steal = acc.steal.saturating_add(cpu.steal);
        acc.guest = acc.guest.saturating_add(cpu.guest);
        acc.guest_nice = acc.guest_nice.saturating_add(cpu.guest_nice);
        acc
    });

    let cpu_total_seconds = cpu_times_to_seconds(&cpu_total, ticks_per_second);
    let per_cpu_seconds = per_cpu
        .iter()
        .map(|cpu| cpu_times_to_seconds(cpu, ticks_per_second))
        .collect::<Vec<_>>();

    let process_count = process_count
        .or_else(|| {
            kstat_str(kstats, "unix:0:system_misc:nproc").and_then(|v| v.parse::<u64>().ok())
        })
        .unwrap_or_else(|| count_numeric_dirs("/proc"));

    Ok(SystemSnapshot {
        is_windows: false,
        ticks_per_second,
        cpu_cycle_utilization: None,
        boot_time_epoch_secs: boot_time,
        uptime_secs,
        context_switches,
        forks_since_boot: None,
        interrupts_total,
        softirqs_total: 0,
        process_count,
        pid_max: None,
        entropy_available_bits: None,
        entropy_pool_size_bits: None,
        procs_running: 0,
        procs_blocked: 0,
        cpu_total,
        cpu_total_seconds,
        per_cpu,
        per_cpu_seconds,
    })
}

fn collect_memory(
    kstats: &KstatMap,
    swaps: &[SwapDeviceSnapshot],
    page_size: u64,
) -> Result<MemorySnapshot> {
    let physmem_pages = kstat_u64(kstats, "unix:0:system_pages:physmem");
    let freemem_pages = kstat_u64(kstats, "unix:0:system_pages:freemem");
    let availrmem_pages = kstat_u64(kstats, "unix:0:system_pages:availrmem");

    let swap_total_bytes = swaps.iter().map(|s| s.size_bytes).sum::<u64>();
    let swap_free_bytes = swaps
        .iter()
        .map(|s| s.size_bytes.saturating_sub(s.used_bytes))
        .sum::<u64>();

    Ok(MemorySnapshot {
        mem_total_bytes: physmem_pages.saturating_mul(page_size),
        mem_free_bytes: freemem_pages.saturating_mul(page_size),
        mem_available_bytes: availrmem_pages.saturating_mul(page_size),
        buffers_bytes: None,
        cached_bytes: 0,
        active_bytes: None,
        inactive_bytes: None,
        anon_pages_bytes: None,
        mapped_bytes: None,
        shmem_bytes: None,
        swap_total_bytes,
        swap_free_bytes,
        swap_cached_bytes: None,
        dirty_bytes: None,
        writeback_bytes: None,
        slab_bytes: None,
        sreclaimable_bytes: None,
        sunreclaim_bytes: None,
        page_tables_bytes: None,
        committed_as_bytes: 0,
        commit_limit_bytes: 0,
        kernel_stack_bytes: None,
        hugepages_total: None,
        hugepages_free: None,
        hugepage_size_bytes: None,
        anon_hugepages_bytes: None,
    })
}

fn collect_load(kstats: &KstatMap) -> Result<LoadSnapshot> {
    let one = kstat_u64(kstats, "unix:0:system_misc:avenrun_1min") as f64 / 256.0;
    let five = kstat_u64(kstats, "unix:0:system_misc:avenrun_5min") as f64 / 256.0;
    let fifteen = kstat_u64(kstats, "unix:0:system_misc:avenrun_15min") as f64 / 256.0;

    Ok(LoadSnapshot {
        one,
        five,
        fifteen,
        runnable: 0,
        entities: 0,
        latest_pid: 0,
    })
}

fn collect_vmstat(kstats: &KstatMap) -> BTreeMap<String, i64> {
    let mut out = BTreeMap::new();

    for (key, value) in kstats {
        if !(key.contains(":vminfo:") || key.contains(":system_pages:")) {
            continue;
        }

        let Some((module, instance, name, stat)) = split_kstat_key(key) else {
            continue;
        };

        let parsed = value
            .parse::<i64>()
            .ok()
            .or_else(|| value.parse::<u64>().ok().map(|v| v.min(i64::MAX as u64) as i64));

        if let Some(parsed) = parsed {
            out.insert(format!("{module}.{instance}.{name}.{stat}"), parsed);
        }
    }

    out
}

fn collect_swaps() -> Result<Vec<SwapDeviceSnapshot>> {
    let Some(output) = run_command_optional("swap", &["-l"]) else {
        return Ok(Vec::new());
    };

    let mut out = Vec::new();

    for (idx, line) in output.lines().enumerate() {
        if idx == 0 {
            continue;
        }

        let cols = line.split_whitespace().collect::<Vec<_>>();
        if cols.len() < 5 {
            continue;
        }

        let blocks = cols[3].parse::<u64>().unwrap_or(0);
        let free = cols[4].parse::<u64>().unwrap_or(0);
        let size_bytes = blocks.saturating_mul(512);
        let free_bytes = free.saturating_mul(512);

        out.push(SwapDeviceSnapshot {
            device: cols[0].to_string(),
            swap_type: "swap".to_string(),
            size_bytes,
            used_bytes: size_bytes.saturating_sub(free_bytes),
            priority: 0,
        });
    }

    Ok(out)
}

fn collect_mounts() -> Result<Vec<MountSnapshot>> {
    let contents = fs::read_to_string("/etc/mnttab")?;
    let mut out = Vec::new();

    for line in contents.lines() {
        let cols = line.split_whitespace().collect::<Vec<_>>();
        if cols.len() < 4 {
            continue;
        }

        let device = decode_mount_field(cols[0]);
        let mountpoint = decode_mount_field(cols[1]);
        let fs_type = cols[2].to_string();
        let read_only = cols[3].split(',').any(|v| v == "ro");

        out.push(MountSnapshot {
            device,
            mountpoint,
            fs_type,
            read_only,
        });
    }

    Ok(out)
}

fn collect_cpuinfo(kstats: &KstatMap) -> Result<Vec<CpuInfoSnapshot>> {
    let mut out: BTreeMap<usize, CpuInfoSnapshot> = BTreeMap::new();

    for (key, value) in kstats {
        let Some((module, instance, _name, stat)) = split_kstat_key(key) else {
            continue;
        };

        if module != "cpu_info" {
            continue;
        }

        let cpu = instance.parse::<usize>().unwrap_or(0);
        let entry = out.entry(cpu).or_insert_with(|| CpuInfoSnapshot {
            cpu,
            ..CpuInfoSnapshot::default()
        });

        match stat {
            "vendor_id" => entry.vendor_id = Some(value.clone()),
            "brand" => entry.model_name = Some(value.clone()),
            "implementation" if entry.model_name.is_none() => {
                entry.model_name = Some(value.clone())
            }
            "clock_MHz" => entry.mhz = value.parse::<f64>().ok(),
            "current_clock_Hz" if entry.mhz.is_none() => {
                entry.mhz = value.parse::<f64>().ok().map(|v| v / 1_000_000.0)
            }
            "cache_size" | "l2_cache_size" => {
                entry.cache_size_bytes = value.parse::<u64>().ok().map(|v| v.saturating_mul(1024))
            }
            _ => {}
        }
    }

    Ok(out.into_values().collect())
}

fn collect_disks(kstats: &KstatMap) -> Result<Vec<DiskSnapshot>> {
    let mut groups: HashMap<String, DiskAccum> = HashMap::new();

    for (key, value) in kstats {
        let Some((module, instance, name, stat)) = split_kstat_key(key) else {
            continue;
        };

        if name.contains(',') {
            continue;
        }

        let Some(parsed) = value.parse::<u64>().ok() else {
            continue;
        };

        let interesting = matches!(
            stat,
            "reads"
                | "writes"
                | "nread"
                | "nwritten"
                | "rtime"
                | "wtime"
                | "rlentime"
                | "wlentime"
                | "rcnt"
                | "wcnt"
        );

        if !interesting {
            continue;
        }

        let group_key = format!("{module}:{instance}:{name}");
        let disk = groups.entry(group_key).or_insert_with(|| DiskAccum {
            name: name.to_string(),
            ..DiskAccum::default()
        });

        disk.seen = true;

        match stat {
            "reads" => disk.reads = parsed,
            "writes" => disk.writes = parsed,
            "nread" => disk.nread = parsed,
            "nwritten" => disk.nwritten = parsed,
            "rtime" => disk.rtime_ns = parsed,
            "wtime" => disk.wtime_ns = parsed,
            "rlentime" => disk.rlentime_ns = parsed,
            "wlentime" => disk.wlentime_ns = parsed,
            "rcnt" => disk.rcnt = parsed,
            "wcnt" => disk.wcnt = parsed,
            _ => {}
        }
    }

    let mut out = groups
        .into_values()
        .filter(|d| d.seen && !d.name.is_empty())
        .map(|d| DiskSnapshot {
            name: d.name,
            has_counters: d.reads > 0 || d.writes > 0 || d.nread > 0 || d.nwritten > 0,
            reads: d.reads,
            writes: d.writes,
            sectors_read: d.nread / 512,
            sectors_written: d.nwritten / 512,
            time_reading_ms: d.rtime_ns / 1_000_000,
            time_writing_ms: d.wtime_ns / 1_000_000,
            in_progress: d.rcnt.saturating_add(d.wcnt),
            time_in_progress_ms: 0,
            weighted_time_in_progress_ms: d.rlentime_ns.saturating_add(d.wlentime_ns) / 1_000_000,
            logical_block_size: None,
            physical_block_size: None,
            rotational: None,
        })
        .collect::<Vec<_>>();

    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

fn collect_net(kstats: &KstatMap) -> Result<Vec<NetDevSnapshot>> {
    let mut groups: HashMap<String, NetAccum> = HashMap::new();

    for (key, value) in kstats {
        let Some((module, instance, name, stat)) = split_kstat_key(key) else {
            continue;
        };

        if name.contains(',') {
            continue;
        }

        let interesting = matches!(
            stat,
            "rbytes64"
                | "rbytes"
                | "obytes64"
                | "obytes"
                | "ipackets64"
                | "ipackets"
                | "opackets64"
                | "opackets"
                | "ierrors"
                | "oerrors"
                | "collisions"
                | "multircv"
                | "ifspeed"
                | "mtu"
                | "tx_queue_len"
                | "link_up"
        );

        if !interesting {
            continue;
        }

        let group_key = format!("{module}:{instance}:{name}");
        let net = groups.entry(group_key).or_insert_with(|| NetAccum {
            name: name.to_string(),
            ..NetAccum::default()
        });

        net.seen = true;

        match stat {
            "rbytes64" | "rbytes" => net.rx_bytes = value.parse::<u64>().unwrap_or(net.rx_bytes),
            "obytes64" | "obytes" => net.tx_bytes = value.parse::<u64>().unwrap_or(net.tx_bytes),
            "ipackets64" | "ipackets" => {
                net.rx_packets = value.parse::<u64>().unwrap_or(net.rx_packets)
            }
            "opackets64" | "opackets" => {
                net.tx_packets = value.parse::<u64>().unwrap_or(net.tx_packets)
            }
            "ierrors" => net.rx_errs = value.parse::<u64>().unwrap_or(0),
            "oerrors" => net.tx_errs = value.parse::<u64>().unwrap_or(0),
            "collisions" => net.collisions = value.parse::<u64>().unwrap_or(0),
            "multircv" => net.rx_multicast = value.parse::<u64>().unwrap_or(0),
            "ifspeed" => net.speed_bps = value.parse::<u64>().unwrap_or(0),
            "mtu" => net.mtu = value.parse::<u64>().ok(),
            "tx_queue_len" => net.tx_queue_len = value.parse::<u64>().ok(),
            "link_up" => {
                net.carrier_up = match value.trim() {
                    "0" => Some(false),
                    "1" => Some(true),
                    _ => None,
                }
            }
            _ => {}
        }
    }

    let mut out = groups
        .into_values()
        .filter(|n| n.seen && !n.name.is_empty())
        .map(|n| {
            let is_loopback = n.name == "lo0" || n.name == "lo";
            NetDevSnapshot {
                name: n.name.clone(),
                stable_id: Some(format!("name:{}", n.name)),
                interface_index: None,
                interface_luid: None,
                is_virtual: None,
                is_loopback: Some(is_loopback),
                is_physical: None,
                is_primary: None,
                mtu: n.mtu,
                speed_mbps: if n.speed_bps > 0 {
                    Some(n.speed_bps / 1_000_000)
                } else {
                    None
                },
                tx_queue_len: n.tx_queue_len,
                carrier_up: n.carrier_up,
                rx_bytes: n.rx_bytes,
                rx_packets: n.rx_packets,
                rx_errs: n.rx_errs,
                rx_drop: 0,
                rx_fifo: 0,
                rx_frame: 0,
                rx_compressed: 0,
                rx_multicast: n.rx_multicast,
                tx_bytes: n.tx_bytes,
                tx_packets: n.tx_packets,
                tx_errs: n.tx_errs,
                tx_drop: 0,
                tx_fifo: 0,
                tx_colls: n.collisions,
                tx_carrier: 0,
                tx_compressed: 0,
            }
        })
        .collect::<Vec<_>>();

    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

fn collect_processes(page_size: u64, ticks_per_second: u64) -> Result<Vec<ProcessSnapshot>> {
    let Some(output) = run_command_optional(
        "ps",
        &[
            "-e",
            "-o",
            "pid=",
            "-o",
            "ppid=",
            "-o",
            "s=",
            "-o",
            "nlwp=",
            "-o",
            "pri=",
            "-o",
            "nice=",
            "-o",
            "rss=",
            "-o",
            "vsz=",
            "-o",
            "time=",
            "-o",
            "fname=",
        ],
    ) else {
        return Ok(Vec::new());
    };

    let mut out = Vec::new();

    for line in output.lines() {
        let cols = line.split_whitespace().collect::<Vec<_>>();
        if cols.len() < 10 {
            continue;
        }

        let pid = cols[0].parse::<i32>().unwrap_or(0);
        let ppid = cols[1].parse::<i32>().unwrap_or(0);
        let state = cols[2].to_string();
        let num_threads = cols[3].parse::<i64>().unwrap_or(0);
        let priority = cols[4].parse::<i64>().unwrap_or(0);
        let nice = cols[5].parse::<i64>().unwrap_or(0);
        let rss_kib = cols[6].parse::<u64>().unwrap_or(0);
        let vsz_kib = cols[7].parse::<u64>().unwrap_or(0);
        let cpu_ticks = parse_cpu_time_to_ticks(cols[8], ticks_per_second);
        let comm = cols[9].to_string();

        let resident_bytes = rss_kib.saturating_mul(1024);
        let vsize_bytes = vsz_kib.saturating_mul(1024);

        out.push(ProcessSnapshot {
            pid,
            ppid,
            comm,
            state,
            num_threads,
            priority,
            nice,
            minflt: 0,
            majflt: 0,
            vsize_bytes,
            rss_pages: u64_to_i64(resident_bytes / page_size.max(1)),
            virtual_size_bytes: Some(vsize_bytes),
            resident_bytes: Some(resident_bytes),
            utime_ticks: cpu_ticks,
            stime_ticks: 0,
            start_time_ticks: 0,
            processor: None,
            rt_priority: None,
            policy: None,
            oom_score: None,
            fd_count: count_fds(pid),
            fd_table_size: None,
            read_chars: None,
            write_chars: None,
            syscr: None,
            syscw: None,
            read_bytes: None,
            write_bytes: None,
            cancelled_write_bytes: None,
            vm_size_kib: Some(vsz_kib),
            vm_rss_kib: Some(rss_kib),
            vm_data_kib: None,
            vm_stack_kib: None,
            vm_exe_kib: None,
            vm_lib_kib: None,
            vm_swap_kib: None,
            vm_pte_kib: None,
            vm_hwm_kib: None,
            working_set_bytes: None,
            private_bytes: None,
            peak_working_set_bytes: None,
            pagefile_usage_bytes: None,
            commit_charge_bytes: None,
            voluntary_ctxt_switches: None,
            nonvoluntary_ctxt_switches: None,
        });
    }

    Ok(out)
}
