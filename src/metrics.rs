use crate::catalog::{
    buddyinfo_attrs, interrupts_attrs, net_snmp_attrs, pressure_attrs, pressure_stall_time_attrs,
    softirqs_attrs, vmstat_attrs, zoneinfo_attrs,
};
use crate::delta::DerivedMetrics;
use crate::model::Snapshot;
use opentelemetry::metrics::{Counter, Gauge, Meter};
use opentelemetry::KeyValue;
use std::sync::Arc;

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
        patterns
            .iter()
            .any(|p| name == p.as_str() || name.starts_with(p))
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
fn kib_to_bytes(kib: u64) -> u64 {
    kib.saturating_mul(1024)
}

pub struct ProcMetrics {
    filter: MetricFilter,
    pub otel_system_cpu_time: Counter<f64>,
    pub otel_system_interrupts: Counter<u64>,
    pub otel_system_softirqs: Counter<u64>,
    pub otel_system_context_switches: Counter<u64>,
    pub otel_system_processes_created: Counter<u64>,
    pub otel_system_paging_faults: Counter<u64>,
    pub otel_system_paging_operations: Counter<u64>,
    pub otel_system_swap_operations: Counter<u64>,
    pub otel_system_pressure_stall_time: Counter<f64>,
    pub otel_system_uptime: Gauge<f64>,
    pub otel_system_processes: Gauge<u64>,
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
    pub otel_network_packets: Counter<u64>,
    pub otel_network_errors: Counter<u64>,
    pub otel_network_dropped: Counter<u64>,
    pub otel_process_cpu_time: Counter<f64>,
    pub otel_process_io: Counter<u64>,
    pub otel_process_io_chars: Counter<u64>,
    pub otel_process_io_syscalls: Counter<u64>,
    pub otel_process_context_switches: Counter<u64>,
    pub otel_process_page_faults: Counter<u64>,
    pub otel_process_memory_usage: Gauge<u64>,
    pub otel_process_open_fds: Gauge<u64>,
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
    pub socket_count: Gauge<u64>,
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
}

impl ProcMetrics {
    pub fn new(meter: Meter, filter: MetricFilter) -> Self {
        Self {
            filter,
            otel_system_cpu_time: meter
                .f64_counter("system.cpu.time")
                .with_unit("s")
                .with_description("Total system CPU time by state.")
                .build(),
            otel_system_interrupts: meter
                .u64_counter("system.cpu.interrupts")
                .with_unit("{interrupt}")
                .with_description("Total interrupts handled by the system.")
                .build(),
            otel_system_softirqs: meter
                .u64_counter("system.cpu.softirqs")
                .with_unit("{softirq}")
                .with_description("Total softirqs handled by the system.")
                .build(),
            otel_system_context_switches: meter
                .u64_counter("system.context_switches")
                .with_unit("{switch}")
                .with_description("Total context switches.")
                .build(),
            otel_system_processes_created: meter
                .u64_counter("system.processes.created")
                .with_unit("{process}")
                .with_description("Total processes created since boot.")
                .build(),
            otel_system_paging_faults: meter
                .u64_counter("system.paging.faults")
                .with_unit("{fault}")
                .with_description("Total paging faults.")
                .build(),
            otel_system_paging_operations: meter
                .u64_counter("system.paging.operations")
                .with_unit("{operation}")
                .with_description("Total paging operations.")
                .build(),
            otel_system_swap_operations: meter
                .u64_counter("system.swap.operations")
                .with_unit("{operation}")
                .with_description("Total swap operations.")
                .build(),
            otel_system_pressure_stall_time: meter
                .f64_counter("system.linux.pressure.stall_time")
                .with_unit("s")
                .with_description("Cumulative Linux PSI stall time.")
                .build(),
            otel_system_uptime: meter
                .f64_gauge("system.uptime")
                .with_unit("s")
                .with_description("System uptime.")
                .build(),
            otel_system_processes: meter
                .u64_gauge("system.processes.count")
                .with_unit("{process}")
                .with_description("Current process count by state.")
                .build(),
            otel_system_pid_max: meter
                .u64_gauge("system.linux.pid.max")
                .with_unit("{pid}")
                .with_description("Configured maximum PID value.")
                .build(),
            otel_system_entropy: meter
                .u64_gauge("system.linux.entropy")
                .with_unit("bit")
                .with_description("Linux entropy pool state.")
                .build(),
            otel_system_pressure: meter
                .f64_gauge("system.linux.pressure")
                .with_unit("1")
                .with_description("Linux PSI pressure average.")
                .build(),
            otel_linux_interrupts: meter
                .u64_counter("system.linux.interrupts")
                .with_unit("{interrupt}")
                .with_description("Linux interrupt counters by IRQ and CPU.")
                .build(),
            otel_linux_softirqs: meter
                .u64_counter("system.linux.softirqs")
                .with_unit("{softirq}")
                .with_description("Linux softirq counters by type and CPU.")
                .build(),
            otel_disk_io: meter
                .u64_counter("system.disk.io")
                .with_unit("By")
                .with_description("Disk I/O bytes by device and direction.")
                .build(),
            otel_disk_operations: meter
                .u64_counter("system.disk.operations")
                .with_unit("{operation}")
                .with_description("Disk operations by device and direction.")
                .build(),
            otel_disk_operation_time: meter
                .f64_counter("system.disk.operation_time")
                .with_unit("s")
                .with_description("Disk operation time by device and direction.")
                .build(),
            otel_disk_io_time: meter
                .f64_counter("system.disk.io_time")
                .with_unit("s")
                .with_description("Disk busy time by device.")
                .build(),
            otel_disk_pending: meter
                .u64_gauge("system.disk.pending_operations")
                .with_unit("{operation}")
                .with_description("Current disk operations in progress.")
                .build(),
            otel_network_io: meter
                .u64_counter("system.network.io")
                .with_unit("By")
                .with_description("Network I/O bytes by interface and direction.")
                .build(),
            otel_network_packets: meter
                .u64_counter("system.network.packets")
                .with_unit("{packet}")
                .with_description("Network packets by interface and direction.")
                .build(),
            otel_network_errors: meter
                .u64_counter("system.network.errors")
                .with_unit("{error}")
                .with_description("Network errors by interface and direction.")
                .build(),
            otel_network_dropped: meter
                .u64_counter("system.network.dropped")
                .with_unit("{packet}")
                .with_description("Network dropped packets by interface and direction.")
                .build(),
            otel_process_cpu_time: meter
                .f64_counter("process.cpu.time")
                .with_unit("s")
                .with_description("Process CPU time by CPU mode.")
                .build(),
            otel_process_io: meter
                .u64_counter("process.disk.io")
                .with_unit("By")
                .with_description("Process disk I/O bytes by direction.")
                .build(),
            otel_process_io_chars: meter
                .u64_counter("process.io.chars")
                .with_unit("By")
                .with_description("Process character I/O volume by direction.")
                .build(),
            otel_process_io_syscalls: meter
                .u64_counter("process.io.syscalls")
                .with_unit("{syscall}")
                .with_description("Process I/O syscalls by direction.")
                .build(),
            otel_process_context_switches: meter
                .u64_counter("process.context_switches")
                .with_unit("{switch}")
                .with_description("Process context switches by type.")
                .build(),
            otel_process_page_faults: meter
                .u64_counter("process.paging.faults")
                .with_unit("{fault}")
                .with_description("Process page faults by type.")
                .build(),
            otel_process_memory_usage: meter
                .u64_gauge("process.memory.usage")
                .with_unit("By")
                .with_description("Process memory usage by type.")
                .build(),
            otel_process_open_fds: meter
                .u64_gauge("process.open_file_descriptors")
                .with_unit("{file}")
                .with_description("Open file descriptors per process.")
                .build(),
            otel_process_oom_score: meter
                .i64_gauge("process.oom_score")
                .with_unit("1")
                .with_description("Linux OOM score per process.")
                .build(),
            otel_process_processor: meter
                .i64_gauge("process.cpu.last_id")
                .with_unit("{cpu}")
                .with_description("Last CPU core a process ran on.")
                .build(),
            otel_process_start_time: meter
                .f64_gauge("process.start_time")
                .with_unit("s")
                .with_description("Process start time as Unix time.")
                .build(),
            otel_process_start_time_ticks: meter
                .u64_gauge("process.linux.start_time")
                .with_unit("{tick}")
                .with_description("Process start time in clock ticks since boot.")
                .build(),
            otel_process_sched_priority: meter
                .u64_gauge("process.linux.scheduler")
                .with_unit("1")
                .with_description("Linux scheduler metadata per process.")
                .build(),

            cpu_utilization: meter.f64_gauge("system.cpu.utilization").build(),
            load_1m: meter.f64_gauge("system.cpu.load_average.1m").build(),
            load_5m: meter.f64_gauge("system.cpu.load_average.5m").build(),
            load_15m: meter.f64_gauge("system.cpu.load_average.15m").build(),
            load_runnable: meter.u64_gauge("system.linux.load.runnable").build(),
            load_entities: meter.u64_gauge("system.linux.load.entities").build(),
            load_latest_pid: meter.u64_gauge("system.linux.load.latest_pid").build(),

            mem_total_bytes: meter.u64_gauge("system.memory.total").with_unit("By").build(),
            mem_free_bytes: meter.u64_gauge("system.memory.free").with_unit("By").build(),
            mem_available_bytes: meter
                .u64_gauge("system.memory.available")
                .with_unit("By")
                .build(),
            mem_buffers_bytes: meter
                .u64_gauge("system.memory.buffers")
                .with_unit("By")
                .build(),
            mem_cached_bytes: meter
                .u64_gauge("system.memory.cached")
                .with_unit("By")
                .build(),
            mem_active_bytes: meter
                .u64_gauge("system.memory.active")
                .with_unit("By")
                .build(),
            mem_inactive_bytes: meter
                .u64_gauge("system.memory.inactive")
                .with_unit("By")
                .build(),
            mem_anon_bytes: meter.u64_gauge("system.memory.anon").with_unit("By").build(),
            mem_mapped_bytes: meter
                .u64_gauge("system.memory.mapped")
                .with_unit("By")
                .build(),
            mem_shmem_bytes: meter
                .u64_gauge("system.memory.shmem")
                .with_unit("By")
                .build(),
            swap_total_bytes: meter.u64_gauge("system.swap.total").with_unit("By").build(),
            swap_free_bytes: meter.u64_gauge("system.swap.free").with_unit("By").build(),
            swap_cached_bytes: meter
                .u64_gauge("system.swap.cached")
                .with_unit("By")
                .build(),
            mem_dirty_bytes: meter
                .u64_gauge("system.memory.dirty")
                .with_unit("By")
                .build(),
            mem_writeback_bytes: meter
                .u64_gauge("system.memory.writeback")
                .with_unit("By")
                .build(),
            mem_slab_bytes: meter.u64_gauge("system.memory.slab").with_unit("By").build(),
            mem_sreclaimable_bytes: meter
                .u64_gauge("system.memory.sreclaimable")
                .with_unit("By")
                .build(),
            mem_sunreclaim_bytes: meter
                .u64_gauge("system.memory.sunreclaim")
                .with_unit("By")
                .build(),
            mem_page_tables_bytes: meter
                .u64_gauge("system.memory.page_tables")
                .with_unit("By")
                .build(),
            mem_commit_limit_bytes: meter
                .u64_gauge("system.memory.commit_limit")
                .with_unit("By")
                .build(),
            mem_committed_as_bytes: meter
                .u64_gauge("system.memory.committed_as")
                .with_unit("By")
                .build(),
            mem_kernel_stack_bytes: meter
                .u64_gauge("system.memory.kernel_stack")
                .with_unit("By")
                .build(),
            mem_anon_hugepages_bytes: meter
                .u64_gauge("system.memory.anon_hugepages")
                .with_unit("By")
                .build(),
            mem_hugepages_total: meter.u64_gauge("system.memory.hugepages_total").build(),
            mem_hugepages_free: meter.u64_gauge("system.memory.hugepages_free").build(),
            mem_hugepage_size_bytes: meter
                .u64_gauge("system.memory.hugepage_size")
                .with_unit("By")
                .build(),
            mem_used_ratio: meter.f64_gauge("system.memory.used_ratio").build(),
            swap_used_ratio: meter.f64_gauge("system.swap.used_ratio").build(),
            mem_dirty_writeback_ratio: meter
                .f64_gauge("system.memory.dirty_writeback_ratio")
                .build(),
            page_faults_per_sec: meter.f64_gauge("system.paging.faults_per_sec").build(),
            major_page_faults_per_sec: meter
                .f64_gauge("system.paging.major_faults_per_sec")
                .build(),
            page_ins_per_sec: meter.f64_gauge("system.paging.page_ins_per_sec").build(),
            page_outs_per_sec: meter.f64_gauge("system.paging.page_outs_per_sec").build(),
            swap_ins_per_sec: meter.f64_gauge("system.swap.ins_per_sec").build(),
            swap_outs_per_sec: meter.f64_gauge("system.swap.outs_per_sec").build(),

            boot_time_epoch_secs: meter.u64_gauge("system.boot.time").with_unit("s").build(),
            forks_total: meter.u64_gauge("system.processes.forks").build(),
            procs_running: meter.u64_gauge("system.processes.running").build(),
            procs_blocked: meter.u64_gauge("system.processes.blocked").build(),
            per_cpu_utilization: meter.f64_gauge("system.cpu.core.utilization").build(),
            per_cpu_iowait: meter.f64_gauge("system.cpu.core.iowait_ratio").build(),
            per_cpu_system: meter.f64_gauge("system.cpu.core.system_ratio").build(),
            vmstat_value: meter.i64_gauge("system.linux.vmstat").build(),
            swap_device_size: meter
                .u64_gauge("system.linux.swap.device.size")
                .with_unit("By")
                .build(),
            swap_device_used: meter
                .u64_gauge("system.linux.swap.device.used")
                .with_unit("By")
                .build(),
            swap_device_priority: meter.i64_gauge("system.linux.swap.device.priority").build(),
            filesystem_mount_state: meter.u64_gauge("system.filesystem.mount.state").build(),
            cpu_frequency_hz: meter
                .f64_gauge("system.cpu.frequency")
                .with_unit("Hz")
                .build(),
            cpu_cache_size: meter
                .u64_gauge("system.cpu.cache.size")
                .with_unit("By")
                .build(),
            cpu_info_state: meter.u64_gauge("system.cpu.info").build(),
            zoneinfo_value: meter.u64_gauge("system.linux.zoneinfo").build(),
            buddyinfo_blocks: meter.u64_gauge("system.linux.buddy.blocks").build(),
            net_snmp_value: meter.u64_gauge("system.linux.net.snmp").build(),
            socket_count: meter.u64_gauge("system.socket.count").build(),
            kernel_ip_in_discards_per_sec: meter
                .f64_gauge("system.linux.net.ip.in_discards_per_sec")
                .build(),
            kernel_ip_out_discards_per_sec: meter
                .f64_gauge("system.linux.net.ip.out_discards_per_sec")
                .build(),
            kernel_tcp_retrans_segs_per_sec: meter
                .f64_gauge("system.linux.net.tcp.retrans_segs_per_sec")
                .build(),
            kernel_udp_in_errors_per_sec: meter
                .f64_gauge("system.linux.net.udp.in_errors_per_sec")
                .build(),
            kernel_udp_rcvbuf_errors_per_sec: meter
                .f64_gauge("system.linux.net.udp.rcvbuf_errors_per_sec")
                .build(),
            softnet_processed_per_sec: meter
                .f64_gauge("system.linux.net.softnet.processed_per_sec")
                .build(),
            softnet_dropped_per_sec: meter
                .f64_gauge("system.linux.net.softnet.dropped_per_sec")
                .build(),
            softnet_time_squeezed_per_sec: meter
                .f64_gauge("system.linux.net.softnet.time_squeezed_per_sec")
                .build(),
            softnet_drop_ratio: meter
                .f64_gauge("system.linux.net.softnet.drop_ratio")
                .build(),
            softnet_cpu_processed: meter
                .u64_gauge("system.linux.net.softnet.cpu.processed")
                .build(),
            softnet_cpu_dropped: meter
                .u64_gauge("system.linux.net.softnet.cpu.dropped")
                .build(),
            softnet_cpu_time_squeezed: meter
                .u64_gauge("system.linux.net.softnet.cpu.time_squeezed")
                .build(),

            disk_read_bps: meter.f64_gauge("system.disk.read_bytes_per_sec").build(),
            disk_write_bps: meter.f64_gauge("system.disk.write_bytes_per_sec").build(),
            disk_total_bps: meter.f64_gauge("system.disk.total_bytes_per_sec").build(),
            disk_reads_per_sec: meter.f64_gauge("system.disk.read_ops_per_sec").build(),
            disk_writes_per_sec: meter.f64_gauge("system.disk.write_ops_per_sec").build(),
            disk_total_iops: meter.f64_gauge("system.disk.ops_per_sec").build(),
            disk_read_await_ms: meter
                .f64_gauge("system.disk.read_await")
                .with_unit("ms")
                .build(),
            disk_write_await_ms: meter
                .f64_gauge("system.disk.write_await")
                .with_unit("ms")
                .build(),
            disk_avg_read_size_bytes: meter
                .f64_gauge("system.disk.avg_read_size")
                .with_unit("By")
                .build(),
            disk_avg_write_size_bytes: meter
                .f64_gauge("system.disk.avg_write_size")
                .with_unit("By")
                .build(),
            disk_utilization: meter.f64_gauge("system.disk.utilization").build(),
            disk_queue_depth: meter.f64_gauge("system.disk.queue_depth").build(),
            disk_logical_block_size: meter
                .u64_gauge("system.disk.logical_block_size")
                .with_unit("By")
                .build(),
            disk_physical_block_size: meter
                .u64_gauge("system.disk.physical_block_size")
                .with_unit("By")
                .build(),
            disk_rotational: meter.u64_gauge("system.disk.rotational").build(),
            disk_in_progress: meter.u64_gauge("system.disk.io_in_progress").build(),
            disk_time_reading_ms: meter
                .u64_gauge("system.disk.time_reading")
                .with_unit("ms")
                .build(),
            disk_time_writing_ms: meter
                .u64_gauge("system.disk.time_writing")
                .with_unit("ms")
                .build(),
            disk_time_in_progress_ms: meter
                .u64_gauge("system.disk.time_in_progress")
                .with_unit("ms")
                .build(),
            disk_weighted_time_in_progress_ms: meter
                .u64_gauge("system.disk.weighted_time_in_progress")
                .with_unit("ms")
                .build(),

            net_rx_bps: meter.f64_gauge("system.network.rx_bytes_per_sec").build(),
            net_tx_bps: meter.f64_gauge("system.network.tx_bytes_per_sec").build(),
            net_total_bps: meter
                .f64_gauge("system.network.total_bytes_per_sec")
                .build(),
            net_rx_pps: meter.f64_gauge("system.network.rx_packets_per_sec").build(),
            net_tx_pps: meter.f64_gauge("system.network.tx_packets_per_sec").build(),
            net_rx_errs_per_sec: meter.f64_gauge("system.network.rx_errors_per_sec").build(),
            net_tx_errs_per_sec: meter.f64_gauge("system.network.tx_errors_per_sec").build(),
            net_rx_drop_per_sec: meter.f64_gauge("system.network.rx_drops_per_sec").build(),
            net_tx_drop_per_sec: meter.f64_gauge("system.network.tx_drops_per_sec").build(),
            net_rx_loss_ratio: meter.f64_gauge("system.network.rx_loss_ratio").build(),
            net_tx_loss_ratio: meter.f64_gauge("system.network.tx_loss_ratio").build(),
            net_mtu: meter.u64_gauge("system.network.mtu").build(),
            net_speed_mbps: meter.u64_gauge("system.network.speed").build(),
            net_tx_queue_len: meter.u64_gauge("system.network.tx_queue_len").build(),
            net_carrier_up: meter.u64_gauge("system.network.carrier_up").build(),
            net_rx_packets: meter.u64_gauge("system.network.rx_packets").build(),
            net_rx_errs: meter.u64_gauge("system.network.rx_errors").build(),
            net_rx_drop: meter.u64_gauge("system.network.rx_dropped").build(),
            net_rx_fifo: meter.u64_gauge("system.network.rx_fifo").build(),
            net_rx_frame: meter.u64_gauge("system.network.rx_frame").build(),
            net_rx_compressed: meter.u64_gauge("system.network.rx_compressed").build(),
            net_rx_multicast: meter.u64_gauge("system.network.rx_multicast").build(),
            net_tx_packets: meter.u64_gauge("system.network.tx_packets").build(),
            net_tx_errs: meter.u64_gauge("system.network.tx_errors").build(),
            net_tx_drop: meter.u64_gauge("system.network.tx_dropped").build(),
            net_tx_fifo: meter.u64_gauge("system.network.tx_fifo").build(),
            net_tx_colls: meter.u64_gauge("system.network.tx_collisions").build(),
            net_tx_carrier: meter.u64_gauge("system.network.tx_carrier").build(),
            net_tx_compressed: meter.u64_gauge("system.network.tx_compressed").build(),

            process_cpu_ratio: meter.f64_gauge("process.cpu.utilization").build(),
            process_rss_bytes: meter.u64_gauge("process.memory.rss").with_unit("By").build(),
            process_ppid: meter.i64_gauge("process.parent_pid").build(),
            process_num_threads: meter.i64_gauge("process.thread.count").build(),
            process_priority: meter.i64_gauge("process.priority").build(),
            process_nice: meter.i64_gauge("process.linux.nice").build(),
            process_vsize_bytes: meter
                .u64_gauge("process.memory.virtual")
                .with_unit("By")
                .build(),
            process_read_bytes: meter.u64_gauge("process.io.read_bytes").build(),
            process_write_bytes: meter.u64_gauge("process.io.write_bytes").build(),
            process_cancelled_write_bytes: meter
                .i64_gauge("process.linux.io.cancelled_write_bytes")
                .build(),
            process_vm_size_bytes: meter
                .u64_gauge("process.memory.vm_size")
                .with_unit("By")
                .build(),
            process_vm_rss_bytes: meter
                .u64_gauge("process.memory.vm_rss")
                .with_unit("By")
                .build(),
        }
    }

    #[inline]
    fn record_f64(&self, name: &str, gauge: &Gauge<f64>, value: f64, attrs: &[KeyValue]) {
        if self.filter.enabled(name) && value.is_finite() {
            gauge.record(value, attrs);
        }
    }

    #[inline]
    fn record_u64(&self, name: &str, gauge: &Gauge<u64>, value: u64, attrs: &[KeyValue]) {
        if self.filter.enabled(name) {
            gauge.record(value, attrs);
        }
    }

    #[inline]
    fn record_i64(&self, name: &str, gauge: &Gauge<i64>, value: i64, attrs: &[KeyValue]) {
        if self.filter.enabled(name) {
            gauge.record(value, attrs);
        }
    }

    #[inline]
    fn add_f64(&self, name: &str, counter: &Counter<f64>, value: f64, attrs: &[KeyValue]) {
        if self.filter.enabled(name) && value.is_finite() && value > 0.0 {
            counter.add(value, attrs);
        }
    }

    #[inline]
    fn add_u64(&self, name: &str, counter: &Counter<u64>, value: u64, attrs: &[KeyValue]) {
        if self.filter.enabled(name) && value > 0 {
            counter.add(value, attrs);
        }
    }

    pub fn record(&self, snap: &Snapshot, derived: &DerivedMetrics, include_processes: bool) {
        self.record_system(snap, derived);
        self.record_load(snap);
        self.record_memory(snap, derived);
        self.record_paging(derived);
        self.record_pressure(snap, derived);
        self.record_stat(snap);
        self.record_linux_proc(snap, derived);
        self.record_net_kernel(snap, derived);
        self.record_disks(snap, derived);
        self.record_network_interfaces(snap, derived);
        if include_processes {
            self.record_processes(snap, derived);
        }
    }

    fn record_system(&self, snap: &Snapshot, derived: &DerivedMetrics) {
        let is_windows = snap.system.is_windows;
        self.record_f64(
            "system.uptime",
            &self.otel_system_uptime,
            snap.system.uptime_secs,
            &[],
        );

        self.record_u64(
            "system.processes.count",
            &self.otel_system_processes,
            snap.system.process_count,
            &[KeyValue::new("state", "all")],
        );
        self.record_u64(
            "system.processes.count",
            &self.otel_system_processes,
            non_negative_u64(snap.system.procs_running),
            &[KeyValue::new("state", "running")],
        );
        if !is_windows {
            self.record_u64(
                "system.processes.count",
                &self.otel_system_processes,
                non_negative_u64(snap.system.procs_blocked),
                &[KeyValue::new("state", "blocked")],
            );
        }

        if !is_windows {
            self.record_u64(
                "system.linux.pid.max",
                &self.otel_system_pid_max,
                snap.system.pid_max,
                &[],
            );
            self.record_u64(
                "system.linux.entropy",
                &self.otel_system_entropy,
                snap.system.entropy_available_bits,
                &[KeyValue::new("state", "available")],
            );
            self.record_u64(
                "system.linux.entropy",
                &self.otel_system_entropy,
                snap.system.entropy_pool_size_bits,
                &[KeyValue::new("state", "pool_size")],
            );
        }

        self.add_u64(
            "system.cpu.interrupts",
            &self.otel_system_interrupts,
            derived.interrupts_delta,
            &[],
        );
        self.add_u64(
            "system.cpu.softirqs",
            &self.otel_system_softirqs,
            derived.softirqs_delta,
            &[],
        );
        self.add_u64(
            "system.context_switches",
            &self.otel_system_context_switches,
            derived.context_switches_delta,
            &[],
        );
        self.add_u64(
            "system.processes.created",
            &self.otel_system_processes_created,
            derived.forks_delta,
            &[],
        );

        for (state, value) in &derived.cpu_time_delta_secs {
            if is_windows && *state == "iowait" {
                continue;
            }
            self.add_f64(
                "system.cpu.time",
                &self.otel_system_cpu_time,
                *value,
                &[KeyValue::new("state", (*state).to_string())],
            );
        }

        self.add_u64(
            "system.paging.faults",
            &self.otel_system_paging_faults,
            derived.page_faults_delta,
            &[KeyValue::new("type", "minor")],
        );
        self.add_u64(
            "system.paging.faults",
            &self.otel_system_paging_faults,
            derived.major_page_faults_delta,
            &[KeyValue::new("type", "major")],
        );
        self.add_u64(
            "system.paging.operations",
            &self.otel_system_paging_operations,
            derived.page_ins_delta,
            &[KeyValue::new("direction", "in")],
        );
        self.add_u64(
            "system.paging.operations",
            &self.otel_system_paging_operations,
            derived.page_outs_delta,
            &[KeyValue::new("direction", "out")],
        );
        self.add_u64(
            "system.swap.operations",
            &self.otel_system_swap_operations,
            derived.swap_ins_delta,
            &[KeyValue::new("direction", "in")],
        );
        self.add_u64(
            "system.swap.operations",
            &self.otel_system_swap_operations,
            derived.swap_outs_delta,
            &[KeyValue::new("direction", "out")],
        );

        self.record_f64(
            "system.cpu.utilization",
            &self.cpu_utilization,
            derived.cpu_utilization_ratio,
            &[],
        );

        for (cpu, ratio) in &derived.per_cpu_utilization_ratio {
            self.record_f64(
                "system.cpu.core.utilization",
                &self.per_cpu_utilization,
                *ratio,
                &[KeyValue::new("cpu", cpu.to_string())],
            );
        }
        if !is_windows {
            for (cpu, ratio) in &derived.per_cpu_iowait_ratio {
                self.record_f64(
                    "system.cpu.core.iowait_ratio",
                    &self.per_cpu_iowait,
                    *ratio,
                    &[KeyValue::new("cpu", cpu.to_string())],
                );
            }
        }
        for (cpu, ratio) in &derived.per_cpu_system_ratio {
            self.record_f64(
                "system.cpu.core.system_ratio",
                &self.per_cpu_system,
                *ratio,
                &[KeyValue::new("cpu", cpu.to_string())],
            );
        }
    }

    fn record_load(&self, snap: &Snapshot) {
        self.record_f64("system.cpu.load_average.1m", &self.load_1m, snap.load.one, &[]);
        self.record_f64("system.cpu.load_average.5m", &self.load_5m, snap.load.five, &[]);
        self.record_f64(
            "system.cpu.load_average.15m",
            &self.load_15m,
            snap.load.fifteen,
            &[],
        );
        if !snap.system.is_windows {
            self.record_u64(
                "system.linux.load.runnable",
                &self.load_runnable,
                non_negative_u64(snap.load.runnable),
                &[],
            );
            self.record_u64(
                "system.linux.load.entities",
                &self.load_entities,
                non_negative_u64(snap.load.entities),
                &[],
            );
            self.record_u64(
                "system.linux.load.latest_pid",
                &self.load_latest_pid,
                non_negative_u64(snap.load.latest_pid),
                &[],
            );
        }
    }

    fn record_memory(&self, snap: &Snapshot, derived: &DerivedMetrics) {
        let is_windows = snap.system.is_windows;
        let m = &snap.memory;

        self.record_u64("system.memory.total", &self.mem_total_bytes, m.mem_total_bytes, &[]);
        self.record_u64("system.memory.free", &self.mem_free_bytes, m.mem_free_bytes, &[]);
        self.record_u64(
            "system.memory.available",
            &self.mem_available_bytes,
            m.mem_available_bytes,
            &[],
        );
        if !is_windows {
            self.record_u64(
                "system.memory.buffers",
                &self.mem_buffers_bytes,
                m.buffers_bytes,
                &[],
            );
        }
        self.record_u64(
            "system.memory.cached",
            &self.mem_cached_bytes,
            m.cached_bytes,
            &[],
        );
        if !is_windows {
            self.record_u64(
                "system.memory.active",
                &self.mem_active_bytes,
                m.active_bytes,
                &[],
            );
            self.record_u64(
                "system.memory.inactive",
                &self.mem_inactive_bytes,
                m.inactive_bytes,
                &[],
            );
            self.record_u64(
                "system.memory.anon",
                &self.mem_anon_bytes,
                m.anon_pages_bytes,
                &[],
            );
            self.record_u64(
                "system.memory.mapped",
                &self.mem_mapped_bytes,
                m.mapped_bytes,
                &[],
            );
            self.record_u64(
                "system.memory.shmem",
                &self.mem_shmem_bytes,
                m.shmem_bytes,
                &[],
            );
        }
        self.record_u64("system.swap.total", &self.swap_total_bytes, m.swap_total_bytes, &[]);
        self.record_u64("system.swap.free", &self.swap_free_bytes, m.swap_free_bytes, &[]);
        if !is_windows {
            self.record_u64(
                "system.swap.cached",
                &self.swap_cached_bytes,
                m.swap_cached_bytes,
                &[],
            );
            self.record_u64("system.memory.dirty", &self.mem_dirty_bytes, m.dirty_bytes, &[]);
            self.record_u64(
                "system.memory.writeback",
                &self.mem_writeback_bytes,
                m.writeback_bytes,
                &[],
            );
            self.record_u64("system.memory.slab", &self.mem_slab_bytes, m.slab_bytes, &[]);
            self.record_u64(
                "system.memory.sreclaimable",
                &self.mem_sreclaimable_bytes,
                m.sreclaimable_bytes,
                &[],
            );
            self.record_u64(
                "system.memory.sunreclaim",
                &self.mem_sunreclaim_bytes,
                m.sunreclaim_bytes,
                &[],
            );
            self.record_u64(
                "system.memory.page_tables",
                &self.mem_page_tables_bytes,
                m.page_tables_bytes,
                &[],
            );
        }
        self.record_u64(
            "system.memory.commit_limit",
            &self.mem_commit_limit_bytes,
            m.commit_limit_bytes,
            &[],
        );
        self.record_u64(
            "system.memory.committed_as",
            &self.mem_committed_as_bytes,
            m.committed_as_bytes,
            &[],
        );
        if !is_windows {
            self.record_u64(
                "system.memory.kernel_stack",
                &self.mem_kernel_stack_bytes,
                m.kernel_stack_bytes,
                &[],
            );
            self.record_u64(
                "system.memory.anon_hugepages",
                &self.mem_anon_hugepages_bytes,
                m.anon_hugepages_bytes,
                &[],
            );
            self.record_u64(
                "system.memory.hugepages_total",
                &self.mem_hugepages_total,
                m.hugepages_total,
                &[],
            );
            self.record_u64(
                "system.memory.hugepages_free",
                &self.mem_hugepages_free,
                m.hugepages_free,
                &[],
            );
            self.record_u64(
                "system.memory.hugepage_size",
                &self.mem_hugepage_size_bytes,
                m.hugepage_size_bytes,
                &[],
            );
        }

        self.record_f64(
            "system.memory.used_ratio",
            &self.mem_used_ratio,
            derived.memory_used_ratio,
            &[],
        );
        self.record_f64(
            "system.swap.used_ratio",
            &self.swap_used_ratio,
            derived.swap_used_ratio,
            &[],
        );
        if !is_windows {
            self.record_f64(
                "system.memory.dirty_writeback_ratio",
                &self.mem_dirty_writeback_ratio,
                derived.dirty_writeback_ratio,
                &[],
            );
        }
    }

    fn record_paging(&self, derived: &DerivedMetrics) {
        self.record_f64(
            "system.paging.faults_per_sec",
            &self.page_faults_per_sec,
            derived.page_faults_per_sec,
            &[],
        );
        self.record_f64(
            "system.paging.major_faults_per_sec",
            &self.major_page_faults_per_sec,
            derived.major_page_faults_per_sec,
            &[],
        );
        self.record_f64(
            "system.paging.page_ins_per_sec",
            &self.page_ins_per_sec,
            derived.page_ins_per_sec,
            &[],
        );
        self.record_f64(
            "system.paging.page_outs_per_sec",
            &self.page_outs_per_sec,
            derived.page_outs_per_sec,
            &[],
        );
        self.record_f64(
            "system.swap.ins_per_sec",
            &self.swap_ins_per_sec,
            derived.swap_ins_per_sec,
            &[],
        );
        self.record_f64(
            "system.swap.outs_per_sec",
            &self.swap_outs_per_sec,
            derived.swap_outs_per_sec,
            &[],
        );
    }

    fn record_pressure(&self, snap: &Snapshot, derived: &DerivedMetrics) {
        for (key, value) in &snap.pressure {
            let Some(attrs) = pressure_attrs(key) else {
                continue;
            };
            self.record_f64("system.linux.pressure", &self.otel_system_pressure, *value, &attrs);
        }

        for (key, value) in &derived.pressure_total_delta_secs {
            let Some(attrs) = pressure_stall_time_attrs(key) else {
                continue;
            };
            self.add_f64(
                "system.linux.pressure.stall_time",
                &self.otel_system_pressure_stall_time,
                *value,
                &attrs,
            );
        }
    }

    fn record_stat(&self, snap: &Snapshot) {
        self.record_u64(
            "system.boot.time",
            &self.boot_time_epoch_secs,
            snap.system.boot_time_epoch_secs,
            &[],
        );
        if !snap.system.is_windows {
            self.record_u64(
                "system.processes.forks",
                &self.forks_total,
                snap.system.forks_since_boot,
                &[],
            );
        }
        self.record_u64(
            "system.processes.running",
            &self.procs_running,
            non_negative_u64(snap.system.procs_running),
            &[],
        );
        if !snap.system.is_windows {
            self.record_u64(
                "system.processes.blocked",
                &self.procs_blocked,
                non_negative_u64(snap.system.procs_blocked),
                &[],
            );
        }

        for (key, value) in &snap.vmstat {
            self.record_i64(
                "system.linux.vmstat",
                &self.vmstat_value,
                *value,
                &vmstat_attrs(key),
            );
        }
        for (key, value) in &snap.net_snmp {
            self.record_u64(
                "system.linux.net.snmp",
                &self.net_snmp_value,
                *value,
                &net_snmp_attrs(key),
            );
        }
        for (key, value) in &snap.sockets {
            self.record_u64(
                "system.socket.count",
                &self.socket_count,
                *value,
                &[KeyValue::new("key", key.clone())],
            );
        }
    }

    fn record_linux_proc(&self, snap: &Snapshot, derived: &DerivedMetrics) {
        for (key, value) in &derived.linux_interrupts_delta {
            let Some(attrs) = interrupts_attrs(key) else {
                continue;
            };
            self.add_u64("system.linux.interrupts", &self.otel_linux_interrupts, *value, &attrs);
        }

        for (key, value) in &derived.linux_softirqs_delta {
            let Some(attrs) = softirqs_attrs(key) else {
                continue;
            };
            self.add_u64("system.linux.softirqs", &self.otel_linux_softirqs, *value, &attrs);
        }

        for swap in &snap.swaps {
            let attrs = [
                KeyValue::new("device", swap.device.clone()),
                KeyValue::new("swap_type", swap.swap_type.clone()),
            ];
            self.record_u64(
                "system.linux.swap.device.size",
                &self.swap_device_size,
                swap.size_bytes,
                &attrs,
            );
            self.record_u64(
                "system.linux.swap.device.used",
                &self.swap_device_used,
                swap.used_bytes,
                &attrs,
            );
            self.record_i64(
                "system.linux.swap.device.priority",
                &self.swap_device_priority,
                swap.priority,
                &attrs,
            );
        }

        for mount in &snap.mounts {
            let attrs = [
                KeyValue::new("device", mount.device.clone()),
                KeyValue::new("mountpoint", mount.mountpoint.clone()),
                KeyValue::new("fs_type", mount.fs_type.clone()),
                KeyValue::new("read_only", mount.read_only.to_string()),
            ];
            self.record_u64(
                "system.filesystem.mount.state",
                &self.filesystem_mount_state,
                1,
                &attrs,
            );
        }

        for cpu in &snap.cpuinfo {
            let cpu_attr = [KeyValue::new("cpu", cpu.cpu.to_string())];

            if let Some(value) = cpu.mhz {
                self.record_f64(
                    "system.cpu.frequency",
                    &self.cpu_frequency_hz,
                    value * 1_000_000.0,
                    &cpu_attr,
                );
            }
            if let Some(value) = cpu.cache_size_bytes {
                self.record_u64("system.cpu.cache.size", &self.cpu_cache_size, value, &cpu_attr);
            }

            let mut attrs = vec![KeyValue::new("cpu", cpu.cpu.to_string())];
            if let Some(vendor_id) = &cpu.vendor_id {
                attrs.push(KeyValue::new("vendor_id", vendor_id.clone()));
            }
            if let Some(model_name) = &cpu.model_name {
                attrs.push(KeyValue::new("model_name", model_name.clone()));
            }
            self.record_u64("system.cpu.info", &self.cpu_info_state, 1, &attrs);
        }

        for (key, value) in &snap.zoneinfo {
            let Some(attrs) = zoneinfo_attrs(key) else {
                continue;
            };
            self.record_u64("system.linux.zoneinfo", &self.zoneinfo_value, *value, &attrs);
        }

        for (key, value) in &snap.buddyinfo {
            let Some(attrs) = buddyinfo_attrs(key) else {
                continue;
            };
            self.record_u64("system.linux.buddy.blocks", &self.buddyinfo_blocks, *value, &attrs);
        }
    }

    fn record_net_kernel(&self, snap: &Snapshot, derived: &DerivedMetrics) {
        self.record_f64(
            "system.linux.net.ip.in_discards_per_sec",
            &self.kernel_ip_in_discards_per_sec,
            derived.kernel_ip_in_discards_per_sec,
            &[],
        );
        self.record_f64(
            "system.linux.net.ip.out_discards_per_sec",
            &self.kernel_ip_out_discards_per_sec,
            derived.kernel_ip_out_discards_per_sec,
            &[],
        );
        self.record_f64(
            "system.linux.net.tcp.retrans_segs_per_sec",
            &self.kernel_tcp_retrans_segs_per_sec,
            derived.kernel_tcp_retrans_segs_per_sec,
            &[],
        );
        self.record_f64(
            "system.linux.net.udp.in_errors_per_sec",
            &self.kernel_udp_in_errors_per_sec,
            derived.kernel_udp_in_errors_per_sec,
            &[],
        );
        self.record_f64(
            "system.linux.net.udp.rcvbuf_errors_per_sec",
            &self.kernel_udp_rcvbuf_errors_per_sec,
            derived.kernel_udp_rcvbuf_errors_per_sec,
            &[],
        );
        self.record_f64(
            "system.linux.net.softnet.processed_per_sec",
            &self.softnet_processed_per_sec,
            derived.softnet_processed_per_sec,
            &[],
        );
        self.record_f64(
            "system.linux.net.softnet.dropped_per_sec",
            &self.softnet_dropped_per_sec,
            derived.softnet_dropped_per_sec,
            &[],
        );
        self.record_f64(
            "system.linux.net.softnet.time_squeezed_per_sec",
            &self.softnet_time_squeezed_per_sec,
            derived.softnet_time_squeezed_per_sec,
            &[],
        );
        self.record_f64(
            "system.linux.net.softnet.drop_ratio",
            &self.softnet_drop_ratio,
            derived.softnet_drop_ratio,
            &[],
        );

        for cpu in &snap.softnet {
            let attrs = [KeyValue::new("cpu", cpu.cpu.to_string())];
            self.record_u64(
                "system.linux.net.softnet.cpu.processed",
                &self.softnet_cpu_processed,
                cpu.processed,
                &attrs,
            );
            self.record_u64(
                "system.linux.net.softnet.cpu.dropped",
                &self.softnet_cpu_dropped,
                cpu.dropped,
                &attrs,
            );
            self.record_u64(
                "system.linux.net.softnet.cpu.time_squeezed",
                &self.softnet_cpu_time_squeezed,
                cpu.time_squeezed,
                &attrs,
            );
        }
    }

    fn record_disks(&self, snap: &Snapshot, derived: &DerivedMetrics) {
        for disk in &snap.disks {
            let device = disk.name.clone();

            let attrs = [KeyValue::new("device", device.clone())];
            let read_attrs = [
                KeyValue::new("device", device.clone()),
                KeyValue::new("direction", "read"),
            ];
            let write_attrs = [
                KeyValue::new("device", device.clone()),
                KeyValue::new("direction", "write"),
            ];

            if let Some(v) = derived.disk_read_bytes_per_sec.get(&disk.name) {
                self.record_f64("system.disk.read_bytes_per_sec", &self.disk_read_bps, *v, &attrs);
            }
            if let Some(v) = derived.disk_write_bytes_per_sec.get(&disk.name) {
                self.record_f64(
                    "system.disk.write_bytes_per_sec",
                    &self.disk_write_bps,
                    *v,
                    &attrs,
                );
            }
            if let Some(v) = derived.disk_total_bytes_per_sec.get(&disk.name) {
                self.record_f64(
                    "system.disk.total_bytes_per_sec",
                    &self.disk_total_bps,
                    *v,
                    &attrs,
                );
            }
            if let Some(v) = derived.disk_reads_per_sec.get(&disk.name) {
                self.record_f64("system.disk.read_ops_per_sec", &self.disk_reads_per_sec, *v, &attrs);
            }
            if let Some(v) = derived.disk_writes_per_sec.get(&disk.name) {
                self.record_f64(
                    "system.disk.write_ops_per_sec",
                    &self.disk_writes_per_sec,
                    *v,
                    &attrs,
                );
            }
            if let Some(v) = derived.disk_total_iops.get(&disk.name) {
                self.record_f64("system.disk.ops_per_sec", &self.disk_total_iops, *v, &attrs);
            }
            if let Some(v) = derived.disk_read_await_ms.get(&disk.name) {
                self.record_f64("system.disk.read_await", &self.disk_read_await_ms, *v, &attrs);
            }
            if let Some(v) = derived.disk_write_await_ms.get(&disk.name) {
                self.record_f64("system.disk.write_await", &self.disk_write_await_ms, *v, &attrs);
            }
            if let Some(v) = derived.disk_avg_read_size_bytes.get(&disk.name) {
                self.record_f64(
                    "system.disk.avg_read_size",
                    &self.disk_avg_read_size_bytes,
                    *v,
                    &attrs,
                );
            }
            if let Some(v) = derived.disk_avg_write_size_bytes.get(&disk.name) {
                self.record_f64(
                    "system.disk.avg_write_size",
                    &self.disk_avg_write_size_bytes,
                    *v,
                    &attrs,
                );
            }
            if let Some(v) = derived.disk_utilization_ratio.get(&disk.name) {
                self.record_f64("system.disk.utilization", &self.disk_utilization, *v, &attrs);
            }
            if let Some(v) = derived.disk_queue_depth.get(&disk.name) {
                self.record_f64("system.disk.queue_depth", &self.disk_queue_depth, *v, &attrs);
            }

            if let Some(v) = derived.disk_read_bytes_delta.get(&disk.name) {
                self.add_u64("system.disk.io", &self.otel_disk_io, *v, &read_attrs);
            }
            if let Some(v) = derived.disk_write_bytes_delta.get(&disk.name) {
                self.add_u64("system.disk.io", &self.otel_disk_io, *v, &write_attrs);
            }
            if let Some(v) = derived.disk_reads_delta.get(&disk.name) {
                self.add_u64(
                    "system.disk.operations",
                    &self.otel_disk_operations,
                    *v,
                    &read_attrs,
                );
            }
            if let Some(v) = derived.disk_writes_delta.get(&disk.name) {
                self.add_u64(
                    "system.disk.operations",
                    &self.otel_disk_operations,
                    *v,
                    &write_attrs,
                );
            }
            if let Some(v) = derived.disk_read_time_delta_secs.get(&disk.name) {
                self.add_f64(
                    "system.disk.operation_time",
                    &self.otel_disk_operation_time,
                    *v,
                    &read_attrs,
                );
            }
            if let Some(v) = derived.disk_write_time_delta_secs.get(&disk.name) {
                self.add_f64(
                    "system.disk.operation_time",
                    &self.otel_disk_operation_time,
                    *v,
                    &write_attrs,
                );
            }
            if let Some(v) = derived.disk_io_time_delta_secs.get(&disk.name) {
                self.add_f64("system.disk.io_time", &self.otel_disk_io_time, *v, &attrs);
            }

            if let Some(v) = disk.logical_block_size {
                self.record_u64(
                    "system.disk.logical_block_size",
                    &self.disk_logical_block_size,
                    v,
                    &attrs,
                );
            }
            if let Some(v) = disk.physical_block_size {
                self.record_u64(
                    "system.disk.physical_block_size",
                    &self.disk_physical_block_size,
                    v,
                    &attrs,
                );
            }
            if let Some(v) = disk.rotational {
                self.record_u64(
                    "system.disk.rotational",
                    &self.disk_rotational,
                    u64::from(v),
                    &attrs,
                );
            }

            if disk.has_counters {
                self.record_u64(
                    "system.disk.io_in_progress",
                    &self.disk_in_progress,
                    disk.in_progress,
                    &attrs,
                );
                self.record_u64(
                    "system.disk.pending_operations",
                    &self.otel_disk_pending,
                    disk.in_progress,
                    &attrs,
                );
                self.record_u64(
                    "system.disk.time_reading",
                    &self.disk_time_reading_ms,
                    disk.time_reading_ms,
                    &attrs,
                );
                self.record_u64(
                    "system.disk.time_writing",
                    &self.disk_time_writing_ms,
                    disk.time_writing_ms,
                    &attrs,
                );
                self.record_u64(
                    "system.disk.time_in_progress",
                    &self.disk_time_in_progress_ms,
                    disk.time_in_progress_ms,
                    &attrs,
                );
                self.record_u64(
                    "system.disk.weighted_time_in_progress",
                    &self.disk_weighted_time_in_progress_ms,
                    disk.weighted_time_in_progress_ms,
                    &attrs,
                );
            }
        }
    }

    fn record_network_interfaces(&self, snap: &Snapshot, derived: &DerivedMetrics) {
        for net in &snap.net {
            let device = net.name.clone();

            let attrs = [KeyValue::new("device", device.clone())];
            let rx_attrs = [
                KeyValue::new("device", device.clone()),
                KeyValue::new("direction", "receive"),
            ];
            let tx_attrs = [
                KeyValue::new("device", device.clone()),
                KeyValue::new("direction", "transmit"),
            ];

            if let Some(v) = derived.net_rx_bytes_per_sec.get(&net.name) {
                self.record_f64("system.network.rx_bytes_per_sec", &self.net_rx_bps, *v, &attrs);
            }
            if let Some(v) = derived.net_tx_bytes_per_sec.get(&net.name) {
                self.record_f64("system.network.tx_bytes_per_sec", &self.net_tx_bps, *v, &attrs);
            }
            if let Some(v) = derived.net_total_bytes_per_sec.get(&net.name) {
                self.record_f64(
                    "system.network.total_bytes_per_sec",
                    &self.net_total_bps,
                    *v,
                    &attrs,
                );
            }
            if let Some(v) = derived.net_rx_packets_per_sec.get(&net.name) {
                self.record_f64(
                    "system.network.rx_packets_per_sec",
                    &self.net_rx_pps,
                    *v,
                    &attrs,
                );
            }
            if let Some(v) = derived.net_tx_packets_per_sec.get(&net.name) {
                self.record_f64(
                    "system.network.tx_packets_per_sec",
                    &self.net_tx_pps,
                    *v,
                    &attrs,
                );
            }
            if let Some(v) = derived.net_rx_errs_per_sec.get(&net.name) {
                self.record_f64(
                    "system.network.rx_errors_per_sec",
                    &self.net_rx_errs_per_sec,
                    *v,
                    &attrs,
                );
            }
            if let Some(v) = derived.net_tx_errs_per_sec.get(&net.name) {
                self.record_f64(
                    "system.network.tx_errors_per_sec",
                    &self.net_tx_errs_per_sec,
                    *v,
                    &attrs,
                );
            }
            if let Some(v) = derived.net_rx_drop_per_sec.get(&net.name) {
                self.record_f64(
                    "system.network.rx_drops_per_sec",
                    &self.net_rx_drop_per_sec,
                    *v,
                    &attrs,
                );
            }
            if let Some(v) = derived.net_tx_drop_per_sec.get(&net.name) {
                self.record_f64(
                    "system.network.tx_drops_per_sec",
                    &self.net_tx_drop_per_sec,
                    *v,
                    &attrs,
                );
            }
            if let Some(v) = derived.net_rx_loss_ratio.get(&net.name) {
                self.record_f64(
                    "system.network.rx_loss_ratio",
                    &self.net_rx_loss_ratio,
                    *v,
                    &attrs,
                );
            }
            if let Some(v) = derived.net_tx_loss_ratio.get(&net.name) {
                self.record_f64(
                    "system.network.tx_loss_ratio",
                    &self.net_tx_loss_ratio,
                    *v,
                    &attrs,
                );
            }

            if let Some(v) = derived.net_rx_bytes_delta.get(&net.name) {
                self.add_u64("system.network.io", &self.otel_network_io, *v, &rx_attrs);
            }
            if let Some(v) = derived.net_tx_bytes_delta.get(&net.name) {
                self.add_u64("system.network.io", &self.otel_network_io, *v, &tx_attrs);
            }
            if let Some(v) = derived.net_rx_packets_delta.get(&net.name) {
                self.add_u64(
                    "system.network.packets",
                    &self.otel_network_packets,
                    *v,
                    &rx_attrs,
                );
            }
            if let Some(v) = derived.net_tx_packets_delta.get(&net.name) {
                self.add_u64(
                    "system.network.packets",
                    &self.otel_network_packets,
                    *v,
                    &tx_attrs,
                );
            }
            if let Some(v) = derived.net_rx_errs_delta.get(&net.name) {
                self.add_u64(
                    "system.network.errors",
                    &self.otel_network_errors,
                    *v,
                    &rx_attrs,
                );
            }
            if let Some(v) = derived.net_tx_errs_delta.get(&net.name) {
                self.add_u64(
                    "system.network.errors",
                    &self.otel_network_errors,
                    *v,
                    &tx_attrs,
                );
            }
            if let Some(v) = derived.net_rx_drop_delta.get(&net.name) {
                self.add_u64(
                    "system.network.dropped",
                    &self.otel_network_dropped,
                    *v,
                    &rx_attrs,
                );
            }
            if let Some(v) = derived.net_tx_drop_delta.get(&net.name) {
                self.add_u64(
                    "system.network.dropped",
                    &self.otel_network_dropped,
                    *v,
                    &tx_attrs,
                );
            }

            if let Some(v) = net.mtu {
                self.record_u64("system.network.mtu", &self.net_mtu, v, &attrs);
            }
            if let Some(v) = net.speed_mbps {
                self.record_u64("system.network.speed", &self.net_speed_mbps, v, &attrs);
            }
            if let Some(v) = net.tx_queue_len {
                self.record_u64("system.network.tx_queue_len", &self.net_tx_queue_len, v, &attrs);
            }
            if let Some(v) = net.carrier_up {
                self.record_u64(
                    "system.network.carrier_up",
                    &self.net_carrier_up,
                    u64::from(v),
                    &attrs,
                );
            }

            self.record_u64(
                "system.network.rx_packets",
                &self.net_rx_packets,
                net.rx_packets,
                &attrs,
            );
            self.record_u64(
                "system.network.rx_errors",
                &self.net_rx_errs,
                net.rx_errs,
                &attrs,
            );
            self.record_u64(
                "system.network.rx_dropped",
                &self.net_rx_drop,
                net.rx_drop,
                &attrs,
            );
            self.record_u64("system.network.rx_fifo", &self.net_rx_fifo, net.rx_fifo, &attrs);
            self.record_u64("system.network.rx_frame", &self.net_rx_frame, net.rx_frame, &attrs);
            self.record_u64(
                "system.network.rx_compressed",
                &self.net_rx_compressed,
                net.rx_compressed,
                &attrs,
            );
            self.record_u64(
                "system.network.rx_multicast",
                &self.net_rx_multicast,
                net.rx_multicast,
                &attrs,
            );
            self.record_u64(
                "system.network.tx_packets",
                &self.net_tx_packets,
                net.tx_packets,
                &attrs,
            );
            self.record_u64(
                "system.network.tx_errors",
                &self.net_tx_errs,
                net.tx_errs,
                &attrs,
            );
            self.record_u64(
                "system.network.tx_dropped",
                &self.net_tx_drop,
                net.tx_drop,
                &attrs,
            );
            self.record_u64("system.network.tx_fifo", &self.net_tx_fifo, net.tx_fifo, &attrs);
            self.record_u64(
                "system.network.tx_collisions",
                &self.net_tx_colls,
                net.tx_colls,
                &attrs,
            );
            self.record_u64(
                "system.network.tx_carrier",
                &self.net_tx_carrier,
                net.tx_carrier,
                &attrs,
            );
            self.record_u64(
                "system.network.tx_compressed",
                &self.net_tx_compressed,
                net.tx_compressed,
                &attrs,
            );
        }
    }

    fn record_processes(&self, snap: &Snapshot, derived: &DerivedMetrics) {
        let is_windows = snap.system.is_windows;
        for proc in &snap.processes {
            if proc.comm.is_empty() {
                continue;
            }

            let pid = proc.pid.to_string();
            let comm = proc.comm.clone();

            let pid_kv = KeyValue::new("pid", pid.clone());
            let comm_kv = KeyValue::new("comm", comm.clone());
            let proc_state_kv = KeyValue::new("state", proc.state.clone());

            let base_attrs = [pid_kv.clone(), comm_kv.clone(), proc_state_kv];

            if let Some(cpu) = derived.process_cpu_ratio.get(&proc.pid) {
                self.record_f64("process.cpu.utilization", &self.process_cpu_ratio, *cpu, &base_attrs);
            }

            if proc.rss_pages >= 0 {
                self.record_u64(
                    "process.memory.rss",
                    &self.process_rss_bytes,
                    pages_to_bytes_4k(proc.rss_pages),
                    &base_attrs,
                );
            }

            self.record_i64(
                "process.parent_pid",
                &self.process_ppid,
                proc.ppid as i64,
                &base_attrs,
            );
            self.record_i64(
                "process.thread.count",
                &self.process_num_threads,
                proc.num_threads,
                &base_attrs,
            );
            if is_windows {
                self.record_i64(
                    "process.priority",
                    &self.process_priority,
                    proc.priority,
                    &base_attrs,
                );
                if let Some(value) = proc.read_bytes {
                    self.record_u64(
                        "process.io.read_bytes",
                        &self.process_read_bytes,
                        value,
                        &base_attrs,
                    );
                }
                if let Some(value) = proc.write_bytes {
                    self.record_u64(
                        "process.io.write_bytes",
                        &self.process_write_bytes,
                        value,
                        &base_attrs,
                    );
                }
                if let Some(value) = proc.vm_size_kib {
                    self.record_u64(
                        "process.memory.vm_size",
                        &self.process_vm_size_bytes,
                        kib_to_bytes(value),
                        &base_attrs,
                    );
                }
                if let Some(value) = proc.vm_rss_kib {
                    self.record_u64(
                        "process.memory.vm_rss",
                        &self.process_vm_rss_bytes,
                        kib_to_bytes(value),
                        &base_attrs,
                    );
                }
            } else {
                self.record_i64(
                    "process.priority",
                    &self.process_priority,
                    proc.priority,
                    &base_attrs,
                );
                self.record_i64("process.linux.nice", &self.process_nice, proc.nice, &base_attrs);
                self.record_u64(
                    "process.memory.virtual",
                    &self.process_vsize_bytes,
                    proc.vsize_bytes,
                    &base_attrs,
                );

                if let Some(value) = proc.read_bytes {
                    self.record_u64(
                        "process.io.read_bytes",
                        &self.process_read_bytes,
                        value,
                        &base_attrs,
                    );
                }
                if let Some(value) = proc.write_bytes {
                    self.record_u64(
                        "process.io.write_bytes",
                        &self.process_write_bytes,
                        value,
                        &base_attrs,
                    );
                }
                if let Some(value) = proc.cancelled_write_bytes {
                    self.record_i64(
                        "process.linux.io.cancelled_write_bytes",
                        &self.process_cancelled_write_bytes,
                        value,
                        &base_attrs,
                    );
                }
                if let Some(value) = proc.vm_size_kib {
                    self.record_u64(
                        "process.memory.vm_size",
                        &self.process_vm_size_bytes,
                        kib_to_bytes(value),
                        &base_attrs,
                    );
                }
                if let Some(value) = proc.vm_rss_kib {
                    self.record_u64(
                        "process.memory.vm_rss",
                        &self.process_vm_rss_bytes,
                        kib_to_bytes(value),
                        &base_attrs,
                    );
                }
            }

            if let Some(value) = derived.process_cpu_user_delta_secs.get(&proc.pid) {
                self.add_f64(
                    "process.cpu.time",
                    &self.otel_process_cpu_time,
                    *value,
                    &[
                        pid_kv.clone(),
                        comm_kv.clone(),
                        KeyValue::new("cpu_mode", "user"),
                    ],
                );
            }
            if let Some(value) = derived.process_cpu_system_delta_secs.get(&proc.pid) {
                self.add_f64(
                    "process.cpu.time",
                    &self.otel_process_cpu_time,
                    *value,
                    &[
                        pid_kv.clone(),
                        comm_kv.clone(),
                        KeyValue::new("cpu_mode", "system"),
                    ],
                );
            }
            if let Some(value) = derived.process_read_bytes_delta.get(&proc.pid) {
                self.add_u64(
                    "process.disk.io",
                    &self.otel_process_io,
                    *value,
                    &[
                        pid_kv.clone(),
                        comm_kv.clone(),
                        KeyValue::new("direction", "read"),
                    ],
                );
            }
            if let Some(value) = derived.process_write_bytes_delta.get(&proc.pid) {
                self.add_u64(
                    "process.disk.io",
                    &self.otel_process_io,
                    *value,
                    &[
                        pid_kv.clone(),
                        comm_kv.clone(),
                        KeyValue::new("direction", "write"),
                    ],
                );
            }
            if let Some(value) = derived.process_read_chars_delta.get(&proc.pid) {
                self.add_u64(
                    "process.io.chars",
                    &self.otel_process_io_chars,
                    *value,
                    &[
                        pid_kv.clone(),
                        comm_kv.clone(),
                        KeyValue::new("direction", "read"),
                    ],
                );
            }
            if let Some(value) = derived.process_write_chars_delta.get(&proc.pid) {
                self.add_u64(
                    "process.io.chars",
                    &self.otel_process_io_chars,
                    *value,
                    &[
                        pid_kv.clone(),
                        comm_kv.clone(),
                        KeyValue::new("direction", "write"),
                    ],
                );
            }
            if let Some(value) = derived.process_syscr_delta.get(&proc.pid) {
                self.add_u64(
                    "process.io.syscalls",
                    &self.otel_process_io_syscalls,
                    *value,
                    &[
                        pid_kv.clone(),
                        comm_kv.clone(),
                        KeyValue::new("direction", "read"),
                    ],
                );
            }
            if let Some(value) = derived.process_syscw_delta.get(&proc.pid) {
                self.add_u64(
                    "process.io.syscalls",
                    &self.otel_process_io_syscalls,
                    *value,
                    &[
                        pid_kv.clone(),
                        comm_kv.clone(),
                        KeyValue::new("direction", "write"),
                    ],
                );
            }
            if let Some(value) = derived.process_voluntary_ctxt_delta.get(&proc.pid) {
                self.add_u64(
                    "process.context_switches",
                    &self.otel_process_context_switches,
                    *value,
                    &[
                        pid_kv.clone(),
                        comm_kv.clone(),
                        KeyValue::new("type", "voluntary"),
                    ],
                );
            }
            if let Some(value) = derived.process_nonvoluntary_ctxt_delta.get(&proc.pid) {
                self.add_u64(
                    "process.context_switches",
                    &self.otel_process_context_switches,
                    *value,
                    &[
                        pid_kv.clone(),
                        comm_kv.clone(),
                        KeyValue::new("type", "involuntary"),
                    ],
                );
            }
            if let Some(value) = derived.process_minor_faults_delta.get(&proc.pid) {
                self.add_u64(
                    "process.paging.faults",
                    &self.otel_process_page_faults,
                    *value,
                    &[
                        pid_kv.clone(),
                        comm_kv.clone(),
                        KeyValue::new("type", "minor"),
                    ],
                );
            }
            if let Some(value) = derived.process_major_faults_delta.get(&proc.pid) {
                self.add_u64(
                    "process.paging.faults",
                    &self.otel_process_page_faults,
                    *value,
                    &[
                        pid_kv.clone(),
                        comm_kv.clone(),
                        KeyValue::new("type", "major"),
                    ],
                );
            }

            if let Some(value) = proc.fd_count {
                self.record_u64(
                    "process.open_file_descriptors",
                    &self.otel_process_open_fds,
                    value,
                    &base_attrs,
                );
            }
            if !is_windows {
                if let Some(value) = proc.oom_score {
                    self.record_i64(
                        "process.oom_score",
                        &self.otel_process_oom_score,
                        value,
                        &base_attrs,
                    );
                }
                if let Some(value) = proc.processor {
                    self.record_i64(
                        "process.cpu.last_id",
                        &self.otel_process_processor,
                        value,
                        &base_attrs,
                    );
                }
            }

            let start_time_unix = snap.system.boot_time_epoch_secs as f64
                + (proc.start_time_ticks as f64 / snap.system.ticks_per_second.max(1) as f64);

            self.record_f64(
                "process.start_time",
                &self.otel_process_start_time,
                start_time_unix,
                &base_attrs,
            );
            if !is_windows {
                self.record_u64(
                    "process.linux.start_time",
                    &self.otel_process_start_time_ticks,
                    proc.start_time_ticks,
                    &base_attrs,
                );
            }

            if !is_windows {
                if let Some(value) = proc.rt_priority {
                    self.record_u64(
                        "process.linux.scheduler",
                        &self.otel_process_sched_priority,
                        value,
                        &[
                            pid_kv.clone(),
                            comm_kv.clone(),
                            KeyValue::new("field", "rt_priority"),
                        ],
                    );
                }
                if let Some(value) = proc.policy {
                    self.record_u64(
                        "process.linux.scheduler",
                        &self.otel_process_sched_priority,
                        value,
                        &[
                            pid_kv.clone(),
                            comm_kv.clone(),
                            KeyValue::new("field", "policy"),
                        ],
                    );
                }
            }

            if proc.rss_pages >= 0 {
                self.record_u64(
                    "process.memory.usage",
                    &self.otel_process_memory_usage,
                    pages_to_bytes_4k(proc.rss_pages),
                    &[
                        pid_kv.clone(),
                        comm_kv.clone(),
                        KeyValue::new("type", "rss"),
                    ],
                );
            }

            self.record_u64(
                "process.memory.usage",
                &self.otel_process_memory_usage,
                proc.vsize_bytes,
                &[
                    pid_kv.clone(),
                    comm_kv.clone(),
                    KeyValue::new("type", "virtual"),
                ],
            );

            for (kind, maybe_value) in [
                ("vm_size", proc.vm_size_kib),
                ("vm_rss", proc.vm_rss_kib),
                ("vm_data", proc.vm_data_kib),
                ("vm_stack", proc.vm_stack_kib),
                ("vm_exe", proc.vm_exe_kib),
                ("vm_lib", proc.vm_lib_kib),
                ("vm_swap", proc.vm_swap_kib),
                ("vm_pte", proc.vm_pte_kib),
                ("vm_hwm", proc.vm_hwm_kib),
            ] {
                if let Some(value) = maybe_value {
                    self.record_u64(
                        "process.memory.usage",
                        &self.otel_process_memory_usage,
                        kib_to_bytes(value),
                        &[
                            pid_kv.clone(),
                            comm_kv.clone(),
                            KeyValue::new("type", kind),
                        ],
                    );
                }
            }
        }
    }
}