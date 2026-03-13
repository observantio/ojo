use std::collections::BTreeMap;

#[derive(Clone, Debug, Default)]
pub struct Snapshot {
    pub system: SystemSnapshot,
    pub memory: MemorySnapshot,
    pub load: LoadSnapshot,
    pub pressure: BTreeMap<String, f64>,
    pub pressure_totals_us: BTreeMap<String, u64>,
    pub vmstat: BTreeMap<String, i64>,
    pub interrupts: BTreeMap<String, u64>,
    pub softirqs: BTreeMap<String, u64>,
    pub net_snmp: BTreeMap<String, u64>,
    pub softnet: Vec<SoftnetCpuSnapshot>,
    pub swaps: Vec<SwapDeviceSnapshot>,
    pub mounts: Vec<MountSnapshot>,
    pub cpuinfo: Vec<CpuInfoSnapshot>,
    pub zoneinfo: BTreeMap<String, u64>,
    pub buddyinfo: BTreeMap<String, u64>,
    pub disks: Vec<DiskSnapshot>,
    pub net: Vec<NetDevSnapshot>,
    pub processes: Vec<ProcessSnapshot>,
}

#[derive(Clone, Debug, Default)]
pub struct SoftnetCpuSnapshot {
    pub cpu: usize,
    pub processed: u64,
    pub dropped: u64,
    pub time_squeezed: u64,
}

#[derive(Clone, Debug, Default)]
pub struct SystemSnapshot {
    pub ticks_per_second: u64,
    pub boot_time_epoch_secs: u64,
    pub uptime_secs: f64,
    pub context_switches: u64,
    pub forks_since_boot: u64,
    pub interrupts_total: u64,
    pub softirqs_total: u64,
    pub process_count: u64,
    pub pid_max: u64,
    pub entropy_available_bits: u64,
    pub entropy_pool_size_bits: u64,
    pub procs_running: u32,
    pub procs_blocked: u32,
    pub cpu_total: CpuTimes,
    pub per_cpu: Vec<CpuTimes>,
}

#[derive(Clone, Debug, Default)]
pub struct CpuTimes {
    pub user: u64,
    pub nice: u64,
    pub system: u64,
    pub idle: u64,
    pub iowait: u64,
    pub irq: u64,
    pub softirq: u64,
    pub steal: u64,
    pub guest: u64,
    pub guest_nice: u64,
}

impl CpuTimes {
    pub fn total(&self) -> u64 {
        self.user
            + self.nice
            + self.system
            + self.idle
            + self.iowait
            + self.irq
            + self.softirq
            + self.steal
            + self.guest
            + self.guest_nice
    }

    pub fn busy(&self) -> u64 {
        self.user + self.nice + self.system + self.irq + self.softirq + self.steal
    }
}

#[derive(Clone, Debug, Default)]
pub struct MemorySnapshot {
    pub mem_total_bytes: u64,
    pub mem_free_bytes: u64,
    pub mem_available_bytes: u64,
    pub buffers_bytes: u64,
    pub cached_bytes: u64,
    pub active_bytes: u64,
    pub inactive_bytes: u64,
    pub anon_pages_bytes: u64,
    pub mapped_bytes: u64,
    pub shmem_bytes: u64,
    pub swap_total_bytes: u64,
    pub swap_free_bytes: u64,
    pub swap_cached_bytes: u64,
    pub dirty_bytes: u64,
    pub writeback_bytes: u64,
    pub slab_bytes: u64,
    pub sreclaimable_bytes: u64,
    pub sunreclaim_bytes: u64,
    pub page_tables_bytes: u64,
    pub committed_as_bytes: u64,
    pub commit_limit_bytes: u64,
    pub kernel_stack_bytes: u64,
    pub hugepages_total: u64,
    pub hugepages_free: u64,
    pub hugepage_size_bytes: u64,
    pub anon_hugepages_bytes: u64,
}

#[derive(Clone, Debug, Default)]
pub struct LoadSnapshot {
    pub one: f64,
    pub five: f64,
    pub fifteen: f64,
    pub runnable: u32,
    pub entities: u32,
    pub latest_pid: u32,
}

#[derive(Clone, Debug, Default)]
pub struct SwapDeviceSnapshot {
    pub device: String,
    pub swap_type: String,
    pub size_bytes: u64,
    pub used_bytes: u64,
    pub priority: i64,
}

#[derive(Clone, Debug, Default)]
pub struct MountSnapshot {
    pub device: String,
    pub mountpoint: String,
    pub fs_type: String,
    pub read_only: bool,
}

#[derive(Clone, Debug, Default)]
pub struct CpuInfoSnapshot {
    pub cpu: usize,
    pub vendor_id: Option<String>,
    pub model_name: Option<String>,
    pub mhz: Option<f64>,
    pub cache_size_bytes: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct DiskSnapshot {
    pub name: String,
    pub reads: u64,
    pub writes: u64,
    pub sectors_read: u64,
    pub sectors_written: u64,
    pub time_reading_ms: u64,
    pub time_writing_ms: u64,
    pub in_progress: u64,
    pub time_in_progress_ms: u64,
    pub weighted_time_in_progress_ms: u64,
    pub logical_block_size: Option<u64>,
    pub physical_block_size: Option<u64>,
    pub rotational: Option<bool>,
}

#[derive(Clone, Debug, Default)]
pub struct NetDevSnapshot {
    pub name: String,
    pub mtu: Option<u64>,
    pub speed_mbps: Option<u64>,
    pub tx_queue_len: Option<u64>,
    pub carrier_up: Option<bool>,
    pub rx_bytes: u64,
    pub rx_packets: u64,
    pub rx_errs: u64,
    pub rx_drop: u64,
    pub rx_fifo: u64,
    pub rx_frame: u64,
    pub rx_compressed: u64,
    pub rx_multicast: u64,
    pub tx_bytes: u64,
    pub tx_packets: u64,
    pub tx_errs: u64,
    pub tx_drop: u64,
    pub tx_fifo: u64,
    pub tx_colls: u64,
    pub tx_carrier: u64,
    pub tx_compressed: u64,
}

#[derive(Clone, Debug, Default)]
pub struct ProcessSnapshot {
    pub pid: i32,
    pub ppid: i32,
    pub comm: String,
    pub state: String,
    pub num_threads: i64,
    pub priority: i64,
    pub nice: i64,
    pub minflt: u64,
    pub majflt: u64,
    pub vsize_bytes: u64,
    pub rss_pages: i64,
    pub utime_ticks: u64,
    pub stime_ticks: u64,
    pub start_time_ticks: u64,
    pub processor: Option<i64>,
    pub rt_priority: Option<u64>,
    pub policy: Option<u64>,
    pub oom_score: Option<i64>,
    pub fd_count: Option<u64>,
    pub read_chars: Option<u64>,
    pub write_chars: Option<u64>,
    pub syscr: Option<u64>,
    pub syscw: Option<u64>,
    pub read_bytes: Option<u64>,
    pub write_bytes: Option<u64>,
    pub cancelled_write_bytes: Option<i64>,
    pub vm_size_kib: Option<u64>,
    pub vm_rss_kib: Option<u64>,
    pub vm_data_kib: Option<u64>,
    pub vm_stack_kib: Option<u64>,
    pub vm_exe_kib: Option<u64>,
    pub vm_lib_kib: Option<u64>,
    pub vm_swap_kib: Option<u64>,
    pub vm_pte_kib: Option<u64>,
    pub vm_hwm_kib: Option<u64>,
    pub voluntary_ctxt_switches: Option<u64>,
    pub nonvoluntary_ctxt_switches: Option<u64>,
}
