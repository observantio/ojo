use crate::catalog::{
    buddyinfo_attrs, cgroup_attrs, filesystem_attrs, interrupts_attrs, net_snmp_attrs,
    netstat_attrs, pressure_attrs, pressure_stall_time_attrs, runqueue_attrs, schedstat_attrs,
    slabinfo_attrs, softirqs_attrs, vmstat_attrs, zoneinfo_attrs,
};
use crate::delta::DerivedMetrics;
use crate::model::{ProcessSnapshot, Snapshot};
use opentelemetry::metrics::{Counter, Gauge, Meter};
use opentelemetry::KeyValue;
use std::collections::{HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use tracing::warn;

pub const ATTR_CPU_MODE: &str = "cpu.mode";
pub const ATTR_SYSTEM_DEVICE: &str = "system.device";
pub const ATTR_NETWORK_INTERFACE: &str = "network.interface.name";
pub const ATTR_DISK_IO_DIRECTION: &str = "disk.io.direction";
pub const ATTR_NETWORK_IO_DIRECTION: &str = "network.io.direction";
pub const ATTR_PROCESS_PID: &str = "process.pid";
pub const ATTR_PROCESS_COMMAND: &str = "process.command";
pub const ATTR_PROCESS_STATE: &str = "process.state";

#[derive(Clone, Copy, Debug)]
pub struct ProcessLabelConfig {
    pub include_pid: bool,
    pub include_command: bool,
    pub include_state: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct StreamCardinalityConfig {
    pub process_max_series: usize,
    pub cgroup_max_series: usize,
}

#[derive(Debug, Default)]
struct SeriesBudget {
    process: SeriesTracker,
    cgroup: SeriesTracker,
}

impl SeriesBudget {
    fn new(config: StreamCardinalityConfig) -> Self {
        Self {
            process: SeriesTracker::new(config.process_max_series),
            cgroup: SeriesTracker::new(config.cgroup_max_series),
        }
    }

    fn allow(&mut self, metric_name: &str, attrs: &[KeyValue]) -> bool {
        if metric_name.starts_with("process.") {
            return self.process.allow(series_fingerprint(metric_name, attrs), "process");
        }
        if metric_name == "system.linux.cgroup" {
            return self.cgroup.allow(series_fingerprint(metric_name, attrs), "system.linux.cgroup");
        }
        true
    }
}

#[derive(Debug, Default)]
struct SeriesTracker {
    limit: usize,
    seen: HashSet<u64>,
    order: VecDeque<u64>,
    dropped: u64,
}

impl SeriesTracker {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            seen: HashSet::new(),
            order: VecDeque::new(),
            dropped: 0,
        }
    }

    fn allow(&mut self, fingerprint: u64, family: &str) -> bool {
        if self.limit == 0 {
            return true;
        }
        if self.seen.contains(&fingerprint) {
            return true;
        }
        if self.seen.len() < self.limit {
            self.seen.insert(fingerprint);
            self.order.push_back(fingerprint);
            return true;
        }

        self.dropped = self.dropped.saturating_add(1);
        if self.dropped == 1 || self.dropped.is_multiple_of(1000) {
            warn!(
                family,
                dropped_new_series = self.dropped,
                max_series = self.limit,
                "stream cardinality limit reached; dropping new series"
            );
        }
        false
    }
}

fn series_fingerprint(metric_name: &str, attrs: &[KeyValue]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    metric_name.hash(&mut hasher);
    for kv in attrs {
        kv.key.as_str().hash(&mut hasher);
        kv.value.to_string().hash(&mut hasher);
    }
    hasher.finish()
}

impl Default for ProcessLabelConfig {
    fn default() -> Self {
        Self {
            include_pid: false,
            include_command: true,
            include_state: true,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct MetricFilter {
    include: Arc<[String]>,
    exclude: Arc<[String]>,
}

impl MetricFilter {
    pub fn new(include: Vec<String>, exclude: Vec<String>) -> Self {
        Self {
            include: include.into(),
            exclude: exclude.into(),
        }
    }

    #[inline]
    fn matches(patterns: &[String], name: &str) -> bool {
        patterns.iter().any(|p| {
            let pat = p.trim();
            if name == pat {
                return true;
            }
            if let Some(prefix) = pat.strip_suffix('.') {
                return name.starts_with(pat) || name == prefix;
            }
            if let Some(rem) = name.strip_prefix(pat) {
                return rem.starts_with('.');
            }
            false
        })
    }

    #[inline]
    pub fn enabled(&self, name: &str) -> bool {
        let included = self.include.is_empty() || Self::matches(&self.include, name);
        included && !Self::matches(&self.exclude, name)
    }
}

#[inline]
fn non_negative_u64<T>(value: T) -> u64
where
    T: TryInto<u64>,
{
    value.try_into().unwrap_or(0)
}

#[inline]
fn pages_to_bytes_4k(pages: i64) -> u64 {
    non_negative_u64(pages).saturating_mul(4096)
}

#[inline]
fn process_rss_bytes(proc: &ProcessSnapshot, is_windows: bool) -> Option<u64> {
    if let Some(bytes) = proc.resident_bytes {
        return Some(bytes);
    }
    if is_windows {
        if let Some(bytes) = proc.working_set_bytes {
            return Some(bytes);
        }
    }
    if proc.rss_pages >= 0 {
        Some(pages_to_bytes_4k(proc.rss_pages))
    } else {
        None
    }
}

#[inline]
fn kib_to_bytes(kib: u64) -> u64 {
    kib.saturating_mul(1024)
}

#[inline]
fn is_linux_like(snap: &Snapshot) -> bool {
    matches!(snap.system.os_type.as_str(), "linux" | "android")
}

pub struct ProcMetrics {
    filter: MetricFilter,
    process_labels: ProcessLabelConfig,
    series_budget: Mutex<SeriesBudget>,
    pub otel_system_cpu_time: Counter<f64>,
    pub otel_system_interrupts: Counter<u64>,
    pub otel_system_softirqs: Counter<u64>,
    pub otel_system_context_switches: Counter<u64>,
    pub otel_system_process_created: Counter<u64>,
    pub otel_system_paging_faults: Counter<u64>,
    pub otel_system_paging_operations: Counter<u64>,
    pub otel_system_swap_operations: Counter<u64>,
    pub otel_system_pressure_stall_time: Counter<f64>,
    pub otel_system_uptime: Gauge<f64>,
    pub otel_system_process_count: Gauge<u64>,
    pub otel_system_pid_max: Gauge<u64>,
    pub otel_system_entropy: Gauge<u64>,
    pub otel_system_pressure: Gauge<f64>,
    pub otel_linux_interrupts: Counter<u64>,
    pub otel_linux_softirqs: Counter<u64>,
    pub otel_disk_io: Counter<u64>,
    pub otel_disk_operations: Counter<u64>,
    pub otel_disk_operation_time: Counter<f64>,
    pub otel_disk_io_time: Counter<f64>,
    pub otel_disk_pending: Gauge<u64>,
    pub otel_network_io: Counter<u64>,
    pub otel_network_packet_count: Counter<u64>,
    pub otel_network_errors: Counter<u64>,
    pub otel_network_packet_dropped: Counter<u64>,
    pub otel_process_cpu_time: Counter<f64>,
    pub otel_process_io: Counter<u64>,
    pub otel_process_io_chars: Counter<u64>,
    pub otel_process_io_syscalls: Counter<u64>,
    pub otel_process_context_switches: Counter<u64>,
    pub otel_process_page_faults: Counter<u64>,
    pub otel_process_memory_usage: Gauge<u64>,
    pub otel_process_unix_file_descriptor_count: Gauge<u64>,
    pub otel_process_oom_score: Gauge<i64>,
    pub otel_process_processor: Gauge<i64>,
    pub otel_process_start_time: Gauge<f64>,
    pub otel_process_start_time_ticks: Gauge<u64>,
    pub otel_process_sched_priority: Gauge<u64>,

    pub cpu_utilization: Gauge<f64>,
    pub load_1m: Gauge<f64>,
    pub load_5m: Gauge<f64>,
    pub load_15m: Gauge<f64>,
    pub load_runnable: Gauge<u64>,
    pub load_entities: Gauge<u64>,
    pub load_latest_pid: Gauge<u64>,

    pub mem_total_bytes: Gauge<u64>,
    pub mem_free_bytes: Gauge<u64>,
    pub mem_available_bytes: Gauge<u64>,
    pub mem_buffers_bytes: Gauge<u64>,
    pub mem_cached_bytes: Gauge<u64>,
    pub mem_active_bytes: Gauge<u64>,
    pub mem_inactive_bytes: Gauge<u64>,
    pub mem_anon_bytes: Gauge<u64>,
    pub mem_mapped_bytes: Gauge<u64>,
    pub mem_shmem_bytes: Gauge<u64>,
    pub swap_total_bytes: Gauge<u64>,
    pub swap_free_bytes: Gauge<u64>,
    pub swap_cached_bytes: Gauge<u64>,
    pub mem_dirty_bytes: Gauge<u64>,
    pub mem_writeback_bytes: Gauge<u64>,
    pub mem_slab_bytes: Gauge<u64>,
    pub mem_sreclaimable_bytes: Gauge<u64>,
    pub mem_sunreclaim_bytes: Gauge<u64>,
    pub mem_page_tables_bytes: Gauge<u64>,
    pub mem_commit_limit_bytes: Gauge<u64>,
    pub mem_committed_as_bytes: Gauge<u64>,
    pub mem_kernel_stack_bytes: Gauge<u64>,
    pub mem_anon_hugepages_bytes: Gauge<u64>,
    pub mem_hugepages_total: Gauge<u64>,
    pub mem_hugepages_free: Gauge<u64>,
    pub mem_hugepage_size_bytes: Gauge<u64>,
    pub mem_used_ratio: Gauge<f64>,
    pub swap_used_ratio: Gauge<f64>,
    pub mem_dirty_writeback_ratio: Gauge<f64>,
    pub page_faults_per_sec: Gauge<f64>,
    pub major_page_faults_per_sec: Gauge<f64>,
    pub page_ins_per_sec: Gauge<f64>,
    pub page_outs_per_sec: Gauge<f64>,
    pub swap_ins_per_sec: Gauge<f64>,
    pub swap_outs_per_sec: Gauge<f64>,

    pub boot_time_epoch_secs: Gauge<u64>,
    pub forks_total: Gauge<u64>,
    pub procs_running: Gauge<u64>,
    pub procs_blocked: Gauge<u64>,
    pub per_cpu_utilization: Gauge<f64>,
    pub per_cpu_iowait: Gauge<f64>,
    pub per_cpu_system: Gauge<f64>,
    pub vmstat_value: Gauge<i64>,
    pub windows_vmstat_value: Gauge<i64>,
    pub windows_interrupts_value: Gauge<u64>,
    pub windows_dpc_value: Gauge<u64>,
    pub swap_device_size: Gauge<u64>,
    pub swap_device_used: Gauge<u64>,
    pub swap_device_priority: Gauge<i64>,
    pub filesystem_mount_state: Gauge<u64>,
    pub cpu_frequency_hz: Gauge<f64>,
    pub cpu_cache_size: Gauge<u64>,
    pub cpu_info_state: Gauge<u64>,
    pub zoneinfo_value: Gauge<u64>,
    pub buddyinfo_blocks: Gauge<u64>,
    pub net_snmp_value: Gauge<u64>,
    pub netstat_value: Gauge<u64>,
    pub windows_net_snmp_value: Gauge<u64>,
    pub socket_count: Gauge<u64>,
    pub schedstat_value: Gauge<u64>,
    pub runqueue_depth_value: Gauge<f64>,
    pub slabinfo_value: Gauge<u64>,
    pub filesystem_value: Gauge<u64>,
    pub cgroup_value: Gauge<u64>,
    pub metric_support_state: Gauge<u64>,
    pub metric_classification_state: Gauge<u64>,
    pub kernel_ip_in_discards_per_sec: Gauge<f64>,
    pub kernel_ip_out_discards_per_sec: Gauge<f64>,
    pub kernel_tcp_retrans_segs_per_sec: Gauge<f64>,
    pub kernel_udp_in_errors_per_sec: Gauge<f64>,
    pub kernel_udp_rcvbuf_errors_per_sec: Gauge<f64>,
    pub softnet_processed_per_sec: Gauge<f64>,
    pub softnet_dropped_per_sec: Gauge<f64>,
    pub softnet_time_squeezed_per_sec: Gauge<f64>,
    pub softnet_drop_ratio: Gauge<f64>,
    pub softnet_cpu_processed: Gauge<u64>,
    pub softnet_cpu_dropped: Gauge<u64>,
    pub softnet_cpu_time_squeezed: Gauge<u64>,

    pub disk_read_bps: Gauge<f64>,
    pub disk_write_bps: Gauge<f64>,
    pub disk_total_bps: Gauge<f64>,
    pub disk_reads_per_sec: Gauge<f64>,
    pub disk_writes_per_sec: Gauge<f64>,
    pub disk_total_iops: Gauge<f64>,
    pub disk_read_await_ms: Gauge<f64>,
    pub disk_write_await_ms: Gauge<f64>,
    pub disk_avg_read_size_bytes: Gauge<f64>,
    pub disk_avg_write_size_bytes: Gauge<f64>,
    pub disk_utilization: Gauge<f64>,
    pub disk_queue_depth: Gauge<f64>,
    pub disk_logical_block_size: Gauge<u64>,
    pub disk_physical_block_size: Gauge<u64>,
    pub disk_rotational: Gauge<u64>,
    pub disk_in_progress: Gauge<u64>,
    pub disk_time_reading_ms: Gauge<u64>,
    pub disk_time_writing_ms: Gauge<u64>,
    pub disk_time_in_progress_ms: Gauge<u64>,
    pub disk_weighted_time_in_progress_ms: Gauge<u64>,

    pub net_rx_bps: Gauge<f64>,
    pub net_tx_bps: Gauge<f64>,
    pub net_total_bps: Gauge<f64>,
    pub net_rx_pps: Gauge<f64>,
    pub net_tx_pps: Gauge<f64>,
    pub net_rx_errs_per_sec: Gauge<f64>,
    pub net_tx_errs_per_sec: Gauge<f64>,
    pub net_rx_drop_per_sec: Gauge<f64>,
    pub net_tx_drop_per_sec: Gauge<f64>,
    pub net_rx_loss_ratio: Gauge<f64>,
    pub net_tx_loss_ratio: Gauge<f64>,
    pub net_mtu: Gauge<u64>,
    pub net_speed_mbps: Gauge<u64>,
    pub net_tx_queue_len: Gauge<u64>,
    pub net_carrier_up: Gauge<u64>,
    pub net_rx_packets: Gauge<u64>,
    pub net_rx_errs: Gauge<u64>,
    pub net_rx_drop: Gauge<u64>,
    pub net_rx_fifo: Gauge<u64>,
    pub net_rx_frame: Gauge<u64>,
    pub net_rx_compressed: Gauge<u64>,
    pub net_rx_multicast: Gauge<u64>,
    pub net_tx_packets: Gauge<u64>,
    pub net_tx_errs: Gauge<u64>,
    pub net_tx_drop: Gauge<u64>,
    pub net_tx_fifo: Gauge<u64>,
    pub net_tx_colls: Gauge<u64>,
    pub net_tx_carrier: Gauge<u64>,
    pub net_tx_compressed: Gauge<u64>,

    pub process_cpu_ratio: Gauge<f64>,
    pub process_rss_bytes: Gauge<u64>,
    pub process_ppid: Gauge<i64>,
    pub process_num_threads: Gauge<i64>,
    pub process_priority: Gauge<i64>,
    pub process_nice: Gauge<i64>,
    pub process_vsize_bytes: Gauge<u64>,
    pub process_read_bytes: Gauge<u64>,
    pub process_write_bytes: Gauge<u64>,
    pub process_cancelled_write_bytes: Gauge<i64>,
    pub process_vm_size_bytes: Gauge<u64>,
    pub process_vm_rss_bytes: Gauge<u64>,
    pub process_working_set_bytes: Gauge<u64>,
    pub process_peak_working_set_bytes: Gauge<u64>,
    pub process_pagefile_usage_bytes: Gauge<u64>,
    pub process_private_bytes: Gauge<u64>,
    pub process_commit_charge_bytes: Gauge<u64>,
}
