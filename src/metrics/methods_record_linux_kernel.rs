impl ProcMetrics {
    fn record_pressure(&self, snap: &Snapshot, derived: &DerivedMetrics) {
        for (key, value) in &snap.pressure {
            let Some(attrs) = pressure_attrs(key) else {
                continue;
            };
            self.record_f64(
                "system.linux.pressure",
                &self.otel_system_pressure,
                *value,
                &attrs,
            );
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
                snap.system.forks_since_boot.unwrap_or(0),
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
            if snap.system.is_windows {
                self.record_i64(
                    "system.windows.vmstat",
                    &self.windows_vmstat_value,
                    *value,
                    &vmstat_attrs(key),
                );
            } else {
                self.record_i64(
                    "system.linux.vmstat",
                    &self.vmstat_value,
                    *value,
                    &vmstat_attrs(key),
                );
            }
        }
        for (key, value) in &snap.net_snmp {
            if snap.system.is_windows {
                self.record_u64(
                    "system.windows.net.snmp",
                    &self.windows_net_snmp_value,
                    *value,
                    &net_snmp_attrs(key),
                );
            } else {
                self.record_u64(
                    "system.linux.net.snmp",
                    &self.net_snmp_value,
                    *value,
                    &net_snmp_attrs(key),
                );
            }
        }
        for (key, value) in &snap.net_stat {
            let Some(attrs) = netstat_attrs(key) else {
                continue;
            };
            self.record_u64("system.linux.netstat", &self.netstat_value, *value, &attrs);
        }
        for (key, value) in &snap.sockets {
            self.record_u64(
                "system.socket.count",
                &self.socket_count,
                *value,
                &[KeyValue::new("key", key.clone())],
            );
        }

        if let Some(windows) = &snap.windows {
            for (key, value) in &windows.vmstat {
                self.record_i64(
                    "system.windows.vmstat",
                    &self.windows_vmstat_value,
                    *value,
                    &[KeyValue::new("key", key.clone())],
                );
            }
            for (key, value) in &windows.interrupts {
                self.record_u64(
                    "system.windows.interrupts",
                    &self.windows_interrupts_value,
                    *value,
                    &[KeyValue::new("key", key.clone())],
                );
            }
            for (key, value) in &windows.dpc {
                self.record_u64(
                    "system.windows.dpc",
                    &self.windows_dpc_value,
                    *value,
                    &[KeyValue::new("key", key.clone())],
                );
            }
        }
    }

    fn record_linux_extended(&self, snap: &Snapshot) {
        if snap.system.is_windows {
            return;
        }

        for (key, value) in &snap.schedstat {
            let Some(attrs) = schedstat_attrs(key) else {
                continue;
            };
            self.record_u64(
                "system.linux.schedstat",
                &self.schedstat_value,
                *value,
                &attrs,
            );
        }

        for (key, value) in &snap.runqueue_depth {
            let Some(attrs) = runqueue_attrs(key) else {
                continue;
            };
            self.record_f64(
                "system.linux.runqueue.depth",
                &self.runqueue_depth_value,
                *value,
                &attrs,
            );
        }

        for (key, value) in &snap.slabinfo {
            let Some(attrs) = slabinfo_attrs(key) else {
                continue;
            };
            self.record_u64("system.linux.slab", &self.slabinfo_value, *value, &attrs);
        }

        for (key, value) in &snap.cgroup {
            let Some(attrs) = cgroup_attrs(key) else {
                continue;
            };
            self.record_u64("system.linux.cgroup", &self.cgroup_value, *value, &attrs);
        }
    }

    fn record_filesystem_usage(&self, snap: &Snapshot) {
        for (key, value) in &snap.filesystem {
            let Some(attrs) = filesystem_attrs(key) else {
                continue;
            };
            self.record_u64(
                "system.filesystem.usage",
                &self.filesystem_value,
                *value,
                &attrs,
            );
        }
    }

    fn record_linux_proc(&self, snap: &Snapshot, derived: &DerivedMetrics) {
        if snap.system.is_windows {
            return;
        }
        for (key, value) in &derived.linux_interrupts_delta {
            let Some(attrs) = interrupts_attrs(key) else {
                continue;
            };
            self.add_u64(
                "system.linux.interrupts",
                &self.otel_linux_interrupts,
                *value,
                &attrs,
            );
        }

        for (key, value) in &derived.linux_softirqs_delta {
            let Some(attrs) = softirqs_attrs(key) else {
                continue;
            };
            self.add_u64(
                "system.linux.softirqs",
                &self.otel_linux_softirqs,
                *value,
                &attrs,
            );
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

        for (key, value) in &snap.zoneinfo {
            let Some(attrs) = zoneinfo_attrs(key) else {
                continue;
            };
            self.record_u64(
                "system.linux.zoneinfo",
                &self.zoneinfo_value,
                *value,
                &attrs,
            );
        }

        for (key, value) in &snap.buddyinfo {
            let Some(attrs) = buddyinfo_attrs(key) else {
                continue;
            };
            self.record_u64(
                "system.linux.buddy.blocks",
                &self.buddyinfo_blocks,
                *value,
                &attrs,
            );
        }
    }

    fn record_mounts(&self, snap: &Snapshot) {
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
    }

    fn record_cpu_inventory(&self, snap: &Snapshot) {
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
                self.record_u64(
                    "system.cpu.cache.size",
                    &self.cpu_cache_size,
                    value,
                    &cpu_attr,
                );
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
    }

    fn record_net_kernel(&self, snap: &Snapshot, derived: &DerivedMetrics) {
        if snap.system.is_windows {
            return;
        }
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

}
