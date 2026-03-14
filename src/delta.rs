use crate::model::Snapshot;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Default)]
pub struct PrevState {
    pub last: Option<Snapshot>,
}

#[derive(Clone, Debug, Default)]
pub struct DerivedMetrics {
    pub cpu_utilization_ratio: f64,
    pub cpu_time_delta_secs: HashMap<&'static str, f64>,
    pub per_cpu_utilization_ratio: Vec<(usize, f64)>,
    pub per_cpu_iowait_ratio: Vec<(usize, f64)>,
    pub per_cpu_system_ratio: Vec<(usize, f64)>,
    pub interrupts_delta: u64,
    pub softirqs_delta: u64,
    pub context_switches_delta: u64,
    pub forks_delta: u64,
    pub memory_used_ratio: f64,
    pub swap_used_ratio: f64,
    pub dirty_writeback_ratio: f64,
    pub pressure_total_delta_secs: HashMap<String, f64>,
    pub linux_interrupts_delta: HashMap<String, u64>,
    pub linux_softirqs_delta: HashMap<String, u64>,
    pub page_faults_per_sec: f64,
    pub major_page_faults_per_sec: f64,
    pub page_ins_per_sec: f64,
    pub page_outs_per_sec: f64,
    pub swap_ins_per_sec: f64,
    pub swap_outs_per_sec: f64,
    pub page_faults_delta: u64,
    pub major_page_faults_delta: u64,
    pub page_ins_delta: u64,
    pub page_outs_delta: u64,
    pub swap_ins_delta: u64,
    pub swap_outs_delta: u64,
    pub disk_read_bytes_per_sec: HashMap<String, f64>,
    pub disk_write_bytes_per_sec: HashMap<String, f64>,
    pub disk_total_bytes_per_sec: HashMap<String, f64>,
    pub disk_read_bytes_delta: HashMap<String, u64>,
    pub disk_write_bytes_delta: HashMap<String, u64>,
    pub disk_reads_per_sec: HashMap<String, f64>,
    pub disk_writes_per_sec: HashMap<String, f64>,
    pub disk_total_iops: HashMap<String, f64>,
    pub disk_reads_delta: HashMap<String, u64>,
    pub disk_writes_delta: HashMap<String, u64>,
    pub disk_read_await_ms: HashMap<String, f64>,
    pub disk_write_await_ms: HashMap<String, f64>,
    pub disk_read_time_delta_secs: HashMap<String, f64>,
    pub disk_write_time_delta_secs: HashMap<String, f64>,
    pub disk_io_time_delta_secs: HashMap<String, f64>,
    pub disk_avg_read_size_bytes: HashMap<String, f64>,
    pub disk_avg_write_size_bytes: HashMap<String, f64>,
    pub disk_utilization_ratio: HashMap<String, f64>,
    pub disk_queue_depth: HashMap<String, f64>,
    pub net_rx_bytes_per_sec: HashMap<String, f64>,
    pub net_tx_bytes_per_sec: HashMap<String, f64>,
    pub net_total_bytes_per_sec: HashMap<String, f64>,
    pub net_rx_bytes_delta: HashMap<String, u64>,
    pub net_tx_bytes_delta: HashMap<String, u64>,
    pub net_rx_packets_per_sec: HashMap<String, f64>,
    pub net_tx_packets_per_sec: HashMap<String, f64>,
    pub net_rx_packets_delta: HashMap<String, u64>,
    pub net_tx_packets_delta: HashMap<String, u64>,
    pub net_rx_errs_per_sec: HashMap<String, f64>,
    pub net_tx_errs_per_sec: HashMap<String, f64>,
    pub net_rx_errs_delta: HashMap<String, u64>,
    pub net_tx_errs_delta: HashMap<String, u64>,
    pub net_rx_drop_per_sec: HashMap<String, f64>,
    pub net_tx_drop_per_sec: HashMap<String, f64>,
    pub net_rx_drop_delta: HashMap<String, u64>,
    pub net_tx_drop_delta: HashMap<String, u64>,
    pub net_rx_loss_ratio: HashMap<String, f64>,
    pub net_tx_loss_ratio: HashMap<String, f64>,
    pub kernel_ip_in_discards_per_sec: f64,
    pub kernel_ip_out_discards_per_sec: f64,
    pub kernel_tcp_retrans_segs_per_sec: f64,
    pub kernel_udp_in_errors_per_sec: f64,
    pub kernel_udp_rcvbuf_errors_per_sec: f64,
    pub softnet_dropped_per_sec: f64,
    pub softnet_time_squeezed_per_sec: f64,
    pub softnet_processed_per_sec: f64,
    pub softnet_drop_ratio: f64,
    pub process_cpu_ratio: HashMap<i32, f64>,
    pub process_cpu_user_delta_secs: HashMap<i32, f64>,
    pub process_cpu_system_delta_secs: HashMap<i32, f64>,
    pub process_read_bytes_delta: HashMap<i32, u64>,
    pub process_write_bytes_delta: HashMap<i32, u64>,
    pub process_read_chars_delta: HashMap<i32, u64>,
    pub process_write_chars_delta: HashMap<i32, u64>,
    pub process_syscr_delta: HashMap<i32, u64>,
    pub process_syscw_delta: HashMap<i32, u64>,
    pub process_voluntary_ctxt_delta: HashMap<i32, u64>,
    pub process_nonvoluntary_ctxt_delta: HashMap<i32, u64>,
    pub process_minor_faults_delta: HashMap<i32, u64>,
    pub process_major_faults_delta: HashMap<i32, u64>,
}

fn per_sec(delta: u64, secs: f64) -> f64 {
    delta as f64 / secs
}

fn ratio(part: u64, total: u64) -> f64 {
    if total > 0 {
        part as f64 / total as f64
    } else {
        0.0
    }
}

impl PrevState {
    pub fn derive(&mut self, current: &Snapshot, elapsed: Duration) -> DerivedMetrics {
        let mut out = DerivedMetrics::default();
        let Some(prev) = self.last.as_ref() else {
            self.last = Some(current.clone());
            return out;
        };

        let secs = elapsed.as_secs_f64().max(0.001);

        let total_delta = current
            .system
            .cpu_total
            .total()
            .saturating_sub(prev.system.cpu_total.total());
        let busy_delta = current
            .system
            .cpu_total
            .busy()
            .saturating_sub(prev.system.cpu_total.busy());

        if total_delta > 0 {
            out.cpu_utilization_ratio = busy_delta as f64 / total_delta as f64;
        }
        let hz = current.system.ticks_per_second.max(1) as f64;
        out.cpu_time_delta_secs.insert(
            "user",
            current
                .system
                .cpu_total
                .user
                .saturating_sub(prev.system.cpu_total.user) as f64
                / hz,
        );
        out.cpu_time_delta_secs.insert(
            "nice",
            current
                .system
                .cpu_total
                .nice
                .saturating_sub(prev.system.cpu_total.nice) as f64
                / hz,
        );
        out.cpu_time_delta_secs.insert(
            "system",
            current
                .system
                .cpu_total
                .system
                .saturating_sub(prev.system.cpu_total.system) as f64
                / hz,
        );
        out.cpu_time_delta_secs.insert(
            "idle",
            current
                .system
                .cpu_total
                .idle
                .saturating_sub(prev.system.cpu_total.idle) as f64
                / hz,
        );
        out.cpu_time_delta_secs.insert(
            "iowait",
            current
                .system
                .cpu_total
                .iowait
                .saturating_sub(prev.system.cpu_total.iowait) as f64
                / hz,
        );
        out.cpu_time_delta_secs.insert(
            "irq",
            current
                .system
                .cpu_total
                .irq
                .saturating_sub(prev.system.cpu_total.irq) as f64
                / hz,
        );
        out.cpu_time_delta_secs.insert(
            "softirq",
            current
                .system
                .cpu_total
                .softirq
                .saturating_sub(prev.system.cpu_total.softirq) as f64
                / hz,
        );
        out.cpu_time_delta_secs.insert(
            "steal",
            current
                .system
                .cpu_total
                .steal
                .saturating_sub(prev.system.cpu_total.steal) as f64
                / hz,
        );
        out.cpu_time_delta_secs.insert(
            "guest",
            current
                .system
                .cpu_total
                .guest
                .saturating_sub(prev.system.cpu_total.guest) as f64
                / hz,
        );
        out.cpu_time_delta_secs.insert(
            "guest_nice",
            current
                .system
                .cpu_total
                .guest_nice
                .saturating_sub(prev.system.cpu_total.guest_nice) as f64
                / hz,
        );
        out.interrupts_delta = current
            .system
            .interrupts_total
            .saturating_sub(prev.system.interrupts_total);
        out.softirqs_delta = current
            .system
            .softirqs_total
            .saturating_sub(prev.system.softirqs_total);
        out.context_switches_delta = current
            .system
            .context_switches
            .saturating_sub(prev.system.context_switches);
        out.forks_delta = current
            .system
            .forks_since_boot
            .saturating_sub(prev.system.forks_since_boot);

        for (key, value) in &current.interrupts {
            let prev_value = prev.interrupts.get(key).copied().unwrap_or_default();
            let delta = value.saturating_sub(prev_value);
            if delta > 0 {
                out.linux_interrupts_delta.insert(key.clone(), delta);
            }
        }
        for (key, value) in &current.softirqs {
            let prev_value = prev.softirqs.get(key).copied().unwrap_or_default();
            let delta = value.saturating_sub(prev_value);
            if delta > 0 {
                out.linux_softirqs_delta.insert(key.clone(), delta);
            }
        }

        if current.memory.mem_total_bytes > 0 {
            let used = current
                .memory
                .mem_total_bytes
                .saturating_sub(current.memory.mem_available_bytes);
            out.memory_used_ratio = used as f64 / current.memory.mem_total_bytes as f64;
        }
        if current.memory.swap_total_bytes > 0 {
            let used = current
                .memory
                .swap_total_bytes
                .saturating_sub(current.memory.swap_free_bytes);
            out.swap_used_ratio = used as f64 / current.memory.swap_total_bytes as f64;
        }
        let dirty_total = current.memory.dirty_bytes + current.memory.writeback_bytes;
        if current.memory.mem_total_bytes > 0 {
            out.dirty_writeback_ratio = dirty_total as f64 / current.memory.mem_total_bytes as f64;
        }

        out.page_faults_delta = current
            .vmstat
            .get("pgfault")
            .copied()
            .unwrap_or_default()
            .saturating_sub(prev.vmstat.get("pgfault").copied().unwrap_or_default())
            as u64;
        out.page_faults_per_sec = out.page_faults_delta as f64 / secs;
        out.major_page_faults_delta = current
            .vmstat
            .get("pgmajfault")
            .copied()
            .unwrap_or_default()
            .saturating_sub(prev.vmstat.get("pgmajfault").copied().unwrap_or_default())
            as u64;
        out.major_page_faults_per_sec = out.major_page_faults_delta as f64 / secs;
        out.page_ins_delta = current
            .vmstat
            .get("pgpgin")
            .copied()
            .unwrap_or_default()
            .saturating_sub(prev.vmstat.get("pgpgin").copied().unwrap_or_default())
            as u64;
        out.page_ins_per_sec = out.page_ins_delta as f64 / secs;
        out.page_outs_delta = current
            .vmstat
            .get("pgpgout")
            .copied()
            .unwrap_or_default()
            .saturating_sub(prev.vmstat.get("pgpgout").copied().unwrap_or_default())
            as u64;
        out.page_outs_per_sec = out.page_outs_delta as f64 / secs;
        out.swap_ins_delta = current
            .vmstat
            .get("pswpin")
            .copied()
            .unwrap_or_default()
            .saturating_sub(prev.vmstat.get("pswpin").copied().unwrap_or_default())
            as u64;
        out.swap_ins_per_sec = out.swap_ins_delta as f64 / secs;
        out.swap_outs_delta = current
            .vmstat
            .get("pswpout")
            .copied()
            .unwrap_or_default()
            .saturating_sub(prev.vmstat.get("pswpout").copied().unwrap_or_default())
            as u64;
        out.swap_outs_per_sec = out.swap_outs_delta as f64 / secs;

        for (key, value) in &current.pressure_totals_us {
            let prev_value = prev
                .pressure_totals_us
                .get(key)
                .copied()
                .unwrap_or_default();
            out.pressure_total_delta_secs.insert(
                key.clone(),
                value.saturating_sub(prev_value) as f64 / 1_000_000.0,
            );
        }

        let snmp_delta = |key: &str| -> u64 {
            current
                .net_snmp
                .get(key)
                .copied()
                .unwrap_or_default()
                .saturating_sub(prev.net_snmp.get(key).copied().unwrap_or_default())
        };
        out.kernel_ip_in_discards_per_sec = per_sec(snmp_delta("Ip.InDiscards"), secs);
        out.kernel_ip_out_discards_per_sec = per_sec(snmp_delta("Ip.OutDiscards"), secs);
        out.kernel_tcp_retrans_segs_per_sec = per_sec(snmp_delta("Tcp.RetransSegs"), secs);
        out.kernel_udp_in_errors_per_sec = per_sec(snmp_delta("Udp.InErrors"), secs);
        out.kernel_udp_rcvbuf_errors_per_sec = per_sec(snmp_delta("Udp.RcvbufErrors"), secs);

        let mut softnet_processed = 0u64;
        let mut softnet_dropped = 0u64;
        let mut softnet_time_squeezed = 0u64;
        for cur in &current.softnet {
            if let Some(prv) = prev.softnet.iter().find(|cpu| cpu.cpu == cur.cpu) {
                softnet_processed += cur.processed.saturating_sub(prv.processed);
                softnet_dropped += cur.dropped.saturating_sub(prv.dropped);
                softnet_time_squeezed += cur.time_squeezed.saturating_sub(prv.time_squeezed);
            }
        }
        out.softnet_processed_per_sec = per_sec(softnet_processed, secs);
        out.softnet_dropped_per_sec = per_sec(softnet_dropped, secs);
        out.softnet_time_squeezed_per_sec = per_sec(softnet_time_squeezed, secs);
        out.softnet_drop_ratio = ratio(softnet_dropped, softnet_processed + softnet_dropped);

        for (idx, (cur, prv)) in current
            .system
            .per_cpu
            .iter()
            .zip(prev.system.per_cpu.iter())
            .enumerate()
        {
            let td = cur.total().saturating_sub(prv.total());
            let bd = cur.busy().saturating_sub(prv.busy());
            let ratio = if td > 0 { bd as f64 / td as f64 } else { 0.0 };
            out.per_cpu_utilization_ratio.push((idx, ratio));
            let iowait = cur.iowait.saturating_sub(prv.iowait);
            let system = cur.system.saturating_sub(prv.system);
            out.per_cpu_iowait_ratio.push((
                idx,
                if td > 0 {
                    iowait as f64 / td as f64
                } else {
                    0.0
                },
            ));
            out.per_cpu_system_ratio.push((
                idx,
                if td > 0 {
                    system as f64 / td as f64
                } else {
                    0.0
                },
            ));
        }

        for cur in &current.disks {
            if let Some(prv) = prev.disks.iter().find(|d| d.name == cur.name) {
                let sector_size = cur.logical_block_size.unwrap_or(512).max(1) as f64;
                let read_sectors = cur.sectors_read.saturating_sub(prv.sectors_read);
                let write_sectors = cur.sectors_written.saturating_sub(prv.sectors_written);
                let read_bytes = read_sectors as f64 * sector_size;
                let write_bytes = write_sectors as f64 * sector_size;
                let reads = cur.reads.saturating_sub(prv.reads);
                let writes = cur.writes.saturating_sub(prv.writes);
                let read_time_ms = cur.time_reading_ms.saturating_sub(prv.time_reading_ms);
                let write_time_ms = cur.time_writing_ms.saturating_sub(prv.time_writing_ms);
                let busy_time_ms = cur
                    .time_in_progress_ms
                    .saturating_sub(prv.time_in_progress_ms);
                let weighted_time_ms = cur
                    .weighted_time_in_progress_ms
                    .saturating_sub(prv.weighted_time_in_progress_ms);

                out.disk_read_bytes_delta
                    .insert(cur.name.clone(), read_bytes as u64);
                out.disk_write_bytes_delta
                    .insert(cur.name.clone(), write_bytes as u64);
                out.disk_read_bytes_per_sec
                    .insert(cur.name.clone(), read_bytes / secs);
                out.disk_write_bytes_per_sec
                    .insert(cur.name.clone(), write_bytes / secs);
                out.disk_total_bytes_per_sec
                    .insert(cur.name.clone(), (read_bytes + write_bytes) / secs);
                out.disk_reads_per_sec
                    .insert(cur.name.clone(), per_sec(reads, secs));
                out.disk_reads_delta.insert(cur.name.clone(), reads);
                out.disk_writes_per_sec
                    .insert(cur.name.clone(), per_sec(writes, secs));
                out.disk_writes_delta.insert(cur.name.clone(), writes);
                out.disk_total_iops
                    .insert(cur.name.clone(), per_sec(reads + writes, secs));
                out.disk_read_await_ms
                    .insert(cur.name.clone(), ratio(read_time_ms, reads) * 1.0);
                out.disk_write_await_ms
                    .insert(cur.name.clone(), ratio(write_time_ms, writes) * 1.0);
                out.disk_read_time_delta_secs
                    .insert(cur.name.clone(), read_time_ms as f64 / 1000.0);
                out.disk_write_time_delta_secs
                    .insert(cur.name.clone(), write_time_ms as f64 / 1000.0);
                out.disk_io_time_delta_secs
                    .insert(cur.name.clone(), busy_time_ms as f64 / 1000.0);
                out.disk_avg_read_size_bytes.insert(
                    cur.name.clone(),
                    if reads > 0 {
                        read_bytes / reads as f64
                    } else {
                        0.0
                    },
                );
                out.disk_avg_write_size_bytes.insert(
                    cur.name.clone(),
                    if writes > 0 {
                        write_bytes / writes as f64
                    } else {
                        0.0
                    },
                );
                out.disk_utilization_ratio.insert(
                    cur.name.clone(),
                    (busy_time_ms as f64 / (secs * 1000.0)).clamp(0.0, 1.0),
                );
                out.disk_queue_depth
                    .insert(cur.name.clone(), weighted_time_ms as f64 / (secs * 1000.0));
            }
        }

        for cur in &current.net {
            if let Some(prv) = prev.net.iter().find(|n| n.name == cur.name) {
                let rx_packets = cur.rx_packets.saturating_sub(prv.rx_packets);
                let tx_packets = cur.tx_packets.saturating_sub(prv.tx_packets);
                let rx_errs = cur.rx_errs.saturating_sub(prv.rx_errs);
                let tx_errs = cur.tx_errs.saturating_sub(prv.tx_errs);
                let rx_drop = cur.rx_drop.saturating_sub(prv.rx_drop);
                let tx_drop = cur.tx_drop.saturating_sub(prv.tx_drop);

                out.net_rx_bytes_per_sec.insert(
                    cur.name.clone(),
                    per_sec(cur.rx_bytes.saturating_sub(prv.rx_bytes), secs),
                );
                out.net_rx_bytes_delta
                    .insert(cur.name.clone(), cur.rx_bytes.saturating_sub(prv.rx_bytes));
                out.net_tx_bytes_per_sec.insert(
                    cur.name.clone(),
                    per_sec(cur.tx_bytes.saturating_sub(prv.tx_bytes), secs),
                );
                out.net_tx_bytes_delta
                    .insert(cur.name.clone(), cur.tx_bytes.saturating_sub(prv.tx_bytes));
                out.net_total_bytes_per_sec.insert(
                    cur.name.clone(),
                    per_sec(
                        cur.rx_bytes.saturating_sub(prv.rx_bytes)
                            + cur.tx_bytes.saturating_sub(prv.tx_bytes),
                        secs,
                    ),
                );
                out.net_rx_packets_per_sec
                    .insert(cur.name.clone(), per_sec(rx_packets, secs));
                out.net_rx_packets_delta
                    .insert(cur.name.clone(), rx_packets);
                out.net_tx_packets_per_sec
                    .insert(cur.name.clone(), per_sec(tx_packets, secs));
                out.net_tx_packets_delta
                    .insert(cur.name.clone(), tx_packets);
                out.net_rx_errs_per_sec
                    .insert(cur.name.clone(), per_sec(rx_errs, secs));
                out.net_rx_errs_delta.insert(cur.name.clone(), rx_errs);
                out.net_tx_errs_per_sec
                    .insert(cur.name.clone(), per_sec(tx_errs, secs));
                out.net_tx_errs_delta.insert(cur.name.clone(), tx_errs);
                out.net_rx_drop_per_sec
                    .insert(cur.name.clone(), per_sec(rx_drop, secs));
                out.net_rx_drop_delta.insert(cur.name.clone(), rx_drop);
                out.net_tx_drop_per_sec
                    .insert(cur.name.clone(), per_sec(tx_drop, secs));
                out.net_tx_drop_delta.insert(cur.name.clone(), tx_drop);
                out.net_rx_loss_ratio.insert(
                    cur.name.clone(),
                    ratio(rx_errs + rx_drop, rx_packets + rx_errs + rx_drop),
                );
                out.net_tx_loss_ratio.insert(
                    cur.name.clone(),
                    ratio(tx_errs + tx_drop, tx_packets + tx_errs + tx_drop),
                );
            }
        }

        let cpu_count = current.system.per_cpu.len().max(1) as f64;

        for cur in &current.processes {
            if let Some(prv) = prev.processes.iter().find(|p| p.pid == cur.pid) {
                let cur_total = cur.utime_ticks + cur.stime_ticks;
                let prv_total = prv.utime_ticks + prv.stime_ticks;
                let dticks = cur_total.saturating_sub(prv_total) as f64;
                let ratio = (dticks / hz) / secs / cpu_count;
                out.process_cpu_ratio.insert(cur.pid, ratio.max(0.0));
                out.process_cpu_user_delta_secs.insert(
                    cur.pid,
                    cur.utime_ticks.saturating_sub(prv.utime_ticks) as f64 / hz,
                );
                out.process_cpu_system_delta_secs.insert(
                    cur.pid,
                    cur.stime_ticks.saturating_sub(prv.stime_ticks) as f64 / hz,
                );
                out.process_minor_faults_delta
                    .insert(cur.pid, cur.minflt.saturating_sub(prv.minflt));
                out.process_major_faults_delta
                    .insert(cur.pid, cur.majflt.saturating_sub(prv.majflt));

                if let (Some(cur_value), Some(prev_value)) = (cur.read_bytes, prv.read_bytes) {
                    out.process_read_bytes_delta
                        .insert(cur.pid, cur_value.saturating_sub(prev_value));
                }
                if let (Some(cur_value), Some(prev_value)) = (cur.write_bytes, prv.write_bytes) {
                    out.process_write_bytes_delta
                        .insert(cur.pid, cur_value.saturating_sub(prev_value));
                }
                if let (Some(cur_value), Some(prev_value)) = (cur.read_chars, prv.read_chars) {
                    out.process_read_chars_delta
                        .insert(cur.pid, cur_value.saturating_sub(prev_value));
                }
                if let (Some(cur_value), Some(prev_value)) = (cur.write_chars, prv.write_chars) {
                    out.process_write_chars_delta
                        .insert(cur.pid, cur_value.saturating_sub(prev_value));
                }
                if let (Some(cur_value), Some(prev_value)) = (cur.syscr, prv.syscr) {
                    out.process_syscr_delta
                        .insert(cur.pid, cur_value.saturating_sub(prev_value));
                }
                if let (Some(cur_value), Some(prev_value)) = (cur.syscw, prv.syscw) {
                    out.process_syscw_delta
                        .insert(cur.pid, cur_value.saturating_sub(prev_value));
                }
                if let (Some(cur_value), Some(prev_value)) =
                    (cur.voluntary_ctxt_switches, prv.voluntary_ctxt_switches)
                {
                    out.process_voluntary_ctxt_delta
                        .insert(cur.pid, cur_value.saturating_sub(prev_value));
                }
                if let (Some(cur_value), Some(prev_value)) = (
                    cur.nonvoluntary_ctxt_switches,
                    prv.nonvoluntary_ctxt_switches,
                ) {
                    out.process_nonvoluntary_ctxt_delta
                        .insert(cur.pid, cur_value.saturating_sub(prev_value));
                }
            }
        }

        self.last = Some(current.clone());
        out
    }
}
