impl ProcMetrics {
    pub fn new(meter: Meter, filter: MetricFilter, process_labels: ProcessLabelConfig) -> Self {
        Self {
            filter,
            process_labels,
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
            otel_system_process_created: meter
                .u64_counter("system.process.created")
                .with_unit("{process}")
                .with_description("Total processes created since boot (OTEL semantic convention).")
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
            otel_system_process_count: meter
                .u64_gauge("system.process.count")
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
            otel_network_packet_count: meter
                .u64_counter("system.network.packet.count")
                .with_unit("{packet}")
                .with_description("Network packet count by interface and direction (OTEL semantic convention).")
                .build(),
            otel_network_errors: meter
                .u64_counter("system.network.errors")
                .with_unit("{error}")
                .with_description("Network errors by interface and direction.")
                .build(),
            otel_network_packet_dropped: meter
                .u64_counter("system.network.packet.dropped")
                .with_unit("{packet}")
                .with_description("Network dropped packet count by interface and direction (OTEL semantic convention).")
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
            otel_process_unix_file_descriptor_count: meter
                .u64_gauge("process.unix.file_descriptor.count")
                .with_unit("{file}")
                .with_description("Open file descriptors per process (OTEL semantic convention).")
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

            mem_total_bytes: meter
                .u64_gauge("system.memory.total")
                .with_unit("By")
                .build(),
            mem_free_bytes: meter
                .u64_gauge("system.memory.free")
                .with_unit("By")
                .build(),
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
            mem_anon_bytes: meter
                .u64_gauge("system.memory.anon")
                .with_unit("By")
                .build(),
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
            mem_slab_bytes: meter
                .u64_gauge("system.memory.slab")
                .with_unit("By")
                .build(),
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
            windows_vmstat_value: meter.i64_gauge("system.windows.vmstat").build(),
            windows_interrupts_value: meter.u64_gauge("system.windows.interrupts").build(),
            windows_dpc_value: meter.u64_gauge("system.windows.dpc").build(),
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
            netstat_value: meter.u64_gauge("system.linux.netstat").build(),
            windows_net_snmp_value: meter.u64_gauge("system.windows.net.snmp").build(),
            socket_count: meter.u64_gauge("system.socket.count").build(),
            schedstat_value: meter.u64_gauge("system.linux.schedstat").build(),
            runqueue_depth_value: meter.f64_gauge("system.linux.runqueue.depth").build(),
            slabinfo_value: meter.u64_gauge("system.linux.slab").build(),
            filesystem_value: meter.u64_gauge("system.filesystem.usage").build(),
            cgroup_value: meter.u64_gauge("system.linux.cgroup").build(),
            metric_support_state: meter.u64_gauge("system.metric.support_state").build(),
            metric_classification_state: meter.u64_gauge("system.metric.classification").build(),
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
            process_rss_bytes: meter
                .u64_gauge("process.memory.rss")
                .with_unit("By")
                .build(),
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
            process_working_set_bytes: meter
                .u64_gauge("process.memory.working_set")
                .with_unit("By")
                .build(),
            process_peak_working_set_bytes: meter
                .u64_gauge("process.memory.peak_working_set")
                .with_unit("By")
                .build(),
            process_pagefile_usage_bytes: meter
                .u64_gauge("process.memory.pagefile_usage")
                .with_unit("By")
                .build(),
            process_private_bytes: meter
                .u64_gauge("process.memory.private_bytes")
                .with_unit("By")
                .build(),
            process_commit_charge_bytes: meter
                .u64_gauge("process.memory.commit_charge")
                .with_unit("By")
                .build(),
        }
    }

    #[inline]
    fn process_base_attrs(&self, proc: &ProcessSnapshot) -> Vec<KeyValue> {
        let mut attrs = Vec::with_capacity(3);
        if self.process_labels.include_pid {
            attrs.push(KeyValue::new(ATTR_PROCESS_PID, proc.pid as i64));
        }
        if self.process_labels.include_command {
            attrs.push(KeyValue::new(ATTR_PROCESS_COMMAND, proc.comm.clone()));
        }
        if self.process_labels.include_state {
            attrs.push(KeyValue::new(ATTR_PROCESS_STATE, proc.state.clone()));
        }
        attrs
    }

    #[inline]
    fn process_attrs_with(&self, proc: &ProcessSnapshot, extras: &[KeyValue]) -> Vec<KeyValue> {
        let mut attrs = self.process_base_attrs(proc);
        attrs.extend_from_slice(extras);
        attrs
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

}
