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
    out.insert(
        "system.cpu.utilization".to_string(),
        "gauge_derived_ratio".to_string(),
    );
    out.insert("system.cpu.load_average.*".to_string(), "gauge".to_string());

    out.insert("system.memory.*".to_string(), "gauge".to_string());
    out.insert("system.swap.*".to_string(), "gauge".to_string());

    out.insert("system.context_switches".to_string(), "counter".to_string());
    out.insert("system.interrupts".to_string(), "counter".to_string());
    out.insert("system.vmstat".to_string(), "gauge".to_string());

    out.insert("system.disk.io".to_string(), "counter".to_string());
    out.insert("system.disk.operations".to_string(), "counter".to_string());
    out.insert(
        "system.disk.operation_time".to_string(),
        "counter".to_string(),
    );
    out.insert(
        "system.disk.pending_operations".to_string(),
        "gauge".to_string(),
    );

    out.insert("system.network.io".to_string(), "counter".to_string());
    out.insert("system.network.packets".to_string(), "counter".to_string());
    out.insert("system.network.errors".to_string(), "counter".to_string());

    out.insert("process.cpu.time".to_string(), "counter".to_string());
    out.insert("process.memory.usage".to_string(), "gauge".to_string());
    out.insert(
        "process.open_file_descriptors".to_string(),
        "gauge".to_string(),
    );

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
        [h, m, s] => days
            .saturating_mul(86_400)
            .saturating_add(h.saturating_mul(3600))
            .saturating_add(m.saturating_mul(60))
            .saturating_add(*s),
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

