impl ProcMetrics {
    fn record_disks(&self, snap: &Snapshot, derived: &DerivedMetrics) {
        for disk in &snap.disks {
            let device = disk.name.clone();

            let attrs = [KeyValue::new(ATTR_SYSTEM_DEVICE, device.clone())];
            let read_attrs = [
                KeyValue::new(ATTR_SYSTEM_DEVICE, device.clone()),
                KeyValue::new(ATTR_DISK_IO_DIRECTION, "read"),
            ];
            let write_attrs = [
                KeyValue::new(ATTR_SYSTEM_DEVICE, device.clone()),
                KeyValue::new(ATTR_DISK_IO_DIRECTION, "write"),
            ];

            if let Some(v) = derived.disk_read_bytes_per_sec.get(&disk.name) {
                self.record_f64(
                    "system.disk.read_bytes_per_sec",
                    &self.disk_read_bps,
                    *v,
                    &attrs,
                );
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
                self.record_f64(
                    "system.disk.read_ops_per_sec",
                    &self.disk_reads_per_sec,
                    *v,
                    &attrs,
                );
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
                self.record_f64(
                    "system.disk.read_await",
                    &self.disk_read_await_ms,
                    *v,
                    &attrs,
                );
            }
            if let Some(v) = derived.disk_write_await_ms.get(&disk.name) {
                self.record_f64(
                    "system.disk.write_await",
                    &self.disk_write_await_ms,
                    *v,
                    &attrs,
                );
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
                self.record_f64(
                    "system.disk.utilization",
                    &self.disk_utilization,
                    *v,
                    &attrs,
                );
            }
            if let Some(v) = derived.disk_queue_depth.get(&disk.name) {
                self.record_f64(
                    "system.disk.queue_depth",
                    &self.disk_queue_depth,
                    *v,
                    &attrs,
                );
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
            let mut attrs_vec = vec![KeyValue::new(ATTR_NETWORK_INTERFACE, device.clone())];
            if let Some(stable_id) = &net.stable_id {
                attrs_vec.push(KeyValue::new("stable_id", stable_id.clone()));
            }
            if let Some(index) = net.interface_index {
                attrs_vec.push(KeyValue::new("if_index", index.to_string()));
            }
            if let Some(luid) = net.interface_luid {
                attrs_vec.push(KeyValue::new("if_luid", luid.to_string()));
            }
            if let Some(v) = net.is_virtual {
                attrs_vec.push(KeyValue::new("is_virtual", v.to_string()));
            }
            if let Some(v) = net.is_loopback {
                attrs_vec.push(KeyValue::new("is_loopback", v.to_string()));
            }
            if let Some(v) = net.is_physical {
                attrs_vec.push(KeyValue::new("is_physical", v.to_string()));
            }
            if let Some(v) = net.is_primary {
                attrs_vec.push(KeyValue::new("is_primary", v.to_string()));
            }

            let attrs = attrs_vec.clone();
            let rx_attrs = [
                KeyValue::new(ATTR_NETWORK_INTERFACE, device.clone()),
                KeyValue::new(ATTR_NETWORK_IO_DIRECTION, "receive"),
            ];
            let tx_attrs = [
                KeyValue::new(ATTR_NETWORK_INTERFACE, device.clone()),
                KeyValue::new(ATTR_NETWORK_IO_DIRECTION, "transmit"),
            ];

            if let Some(v) = derived.net_rx_bytes_per_sec.get(&net.name) {
                self.record_f64(
                    "system.network.rx_bytes_per_sec",
                    &self.net_rx_bps,
                    *v,
                    &attrs,
                );
            }
            if let Some(v) = derived.net_tx_bytes_per_sec.get(&net.name) {
                self.record_f64(
                    "system.network.tx_bytes_per_sec",
                    &self.net_tx_bps,
                    *v,
                    &attrs,
                );
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
                    "system.network.packet.count",
                    &self.otel_network_packet_count,
                    *v,
                    &rx_attrs,
                );
            }
            if let Some(v) = derived.net_tx_packets_delta.get(&net.name) {
                self.add_u64(
                    "system.network.packet.count",
                    &self.otel_network_packet_count,
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
                    "system.network.packet.dropped",
                    &self.otel_network_packet_dropped,
                    *v,
                    &rx_attrs,
                );
            }
            if let Some(v) = derived.net_tx_drop_delta.get(&net.name) {
                self.add_u64(
                    "system.network.packet.dropped",
                    &self.otel_network_packet_dropped,
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
                self.record_u64(
                    "system.network.tx_queue_len",
                    &self.net_tx_queue_len,
                    v,
                    &attrs,
                );
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
            self.record_u64(
                "system.network.rx_fifo",
                &self.net_rx_fifo,
                net.rx_fifo,
                &attrs,
            );
            self.record_u64(
                "system.network.rx_frame",
                &self.net_rx_frame,
                net.rx_frame,
                &attrs,
            );
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
            self.record_u64(
                "system.network.tx_fifo",
                &self.net_tx_fifo,
                net.tx_fifo,
                &attrs,
            );
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

}
