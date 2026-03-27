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
            "process.unix.file_descriptor.count.collection".to_string(),
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
    out.insert(
        "system.network.packet.count".to_string(),
        "counter".to_string(),
    );
    out.insert("system.network.errors".to_string(), "counter".to_string());
    out.insert(
        "system.network.packet.dropped".to_string(),
        "counter".to_string(),
    );
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
        "process.unix.file_descriptor.count".to_string(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn unique_temp_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("ojo-core-{name}-{}-{nanos}", std::process::id()))
    }

    #[test]
    fn cgroup_mode_and_support_state_cover_variants() {
        assert_eq!(CgroupMode::None.as_str(), "unsupported");
        assert_eq!(CgroupMode::V1.as_str(), "v1");
        assert_eq!(CgroupMode::V2.as_str(), "v2");
        assert_eq!(CgroupMode::Hybrid.as_str(), "hybrid");

        let meta_disabled = ProcessCollectionMeta {
            fd_scan_enabled: false,
        };
        let state = linux_support_state(LinuxSupportInputs {
            cgroup_mode: CgroupMode::V1,
            psi_supported: false,
            psi_irq_supported: false,
            schedstat_supported: false,
            process_meta: Some(&meta_disabled),
            in_container: false,
            system_ok: false,
            memory_ok: false,
            process_ok: false,
        });
        assert_eq!(
            state.get("system.linux.pressure").map(String::as_str),
            Some("unsupported_or_disabled")
        );
        assert_eq!(
            state.get("snapshot.processes").map(String::as_str),
            Some("fallback_empty_due_to_collection_failure")
        );
        assert_eq!(
            state
                .get("process.unix.file_descriptor.count.collection")
                .map(String::as_str),
            Some("disabled_for_scale_fd_count_omitted")
        );
    }

    #[test]
    fn read_cache_helpers_cover_common_parsing_paths() {
        let dir = unique_temp_path("cache");
        fs::create_dir_all(&dir).expect("mkdir");
        let bool_file = dir.join("bool");
        let int_file = dir.join("int");
        let bad_bool = dir.join("bad_bool");
        fs::write(&bool_file, "1\n").expect("write bool");
        fs::write(&int_file, "-42 trailing\n").expect("write int");
        fs::write(&bad_bool, "maybe\n").expect("write bad bool");

        let mut cache = ReadCache::default();
        assert_eq!(cache.read_bool_num(&bool_file), Some(true));
        assert_eq!(cache.read_i64_first(&int_file), Some(-42));
        assert_eq!(cache.read_bool_num(&bad_bool), None);

        fs::remove_file(bool_file).expect("cleanup bool");
        fs::remove_file(int_file).expect("cleanup int");
        fs::remove_file(bad_bool).expect("cleanup bad bool");
        fs::remove_dir_all(dir).expect("cleanup dir");
    }

    #[test]
    fn env_and_scope_helpers_cover_toggle_paths() {
        let _guard = env_lock().lock().expect("env lock");

        std::env::set_var("OJO_LINUX_INCLUDE_PSEUDO_FS", "yes");
        std::env::set_var("OJO_LINUX_INCLUDE_VIRTUAL_INTERFACES", "on");
        std::env::set_var("OJO_LINUX_INCLUDE_RAW_CGROUP_PATHS", "1");
        assert!(include_pseudo_filesystems());
        assert!(include_virtual_interfaces());
        assert!(include_raw_cgroup_paths());
        assert_eq!(normalize_cgroup_scope("v2/abc"), "v2/abc");

        std::env::set_var("OJO_LINUX_INCLUDE_RAW_CGROUP_PATHS", "0");
        assert_eq!(normalize_cgroup_scope(""), "root");
        assert_eq!(
            normalize_cgroup_scope("v2/0123456789abcdef0123456789abcdef"),
            "v2/{id}"
        );

        std::env::remove_var("OJO_LINUX_INCLUDE_PSEUDO_FS");
        std::env::remove_var("OJO_LINUX_INCLUDE_VIRTUAL_INTERFACES");
        std::env::remove_var("OJO_LINUX_INCLUDE_RAW_CGROUP_PATHS");
    }

    #[test]
    fn misc_helpers_cover_path_and_key_formatting() {
        assert_eq!(parse_u64_with_max_flag("max"), (None, true));
        assert_eq!(parse_u64_with_max_flag("42"), (Some(42), false));
        assert_eq!(unescape_mount_field("/path\\040with\\040space"), "/path with space");
        assert_eq!(key_dot2("a", "b"), "a.b");
        assert_eq!(key_dot3("a", "b", "c"), "a.b.c");
        assert_eq!(key_pipe2("irq", 3), "irq|3");
        assert_eq!(key_pipe3("scope", "key", "value"), "scope|key|value");
        assert_eq!(key_pipe4("a", "b", "c", 1), "a|b|c|1");
        assert!(page_size_bytes() > 0);
        let _ = read_primary_interfaces();
    }

    #[test]
    fn read_cpu_frequency_returns_none_for_missing_cpu_directory() {
        let mut cache = ReadCache::default();
        assert_eq!(read_cpu_frequency_mhz(&mut cache, usize::MAX), None);
    }
}
