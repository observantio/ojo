use crate::model::{DiskSnapshot, NetDevSnapshot, ProcessSnapshot, Snapshot, SoftnetCpuSnapshot};
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

#[inline]
fn non_negative_i64_delta(cur: Option<i64>, prev: Option<i64>) -> u64 {
    match (cur, prev) {
        (Some(cur), Some(prev)) if cur >= 0 && prev >= 0 => cur.saturating_sub(prev) as u64,
        _ => 0,
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

        if let Some(ratio) = current.system.cpu_cycle_utilization {
            out.cpu_utilization_ratio = ratio;
        } else if total_delta > 0 {
            out.cpu_utilization_ratio = busy_delta as f64 / total_delta as f64;
        }

        let hz = current.system.ticks_per_second.max(1) as f64;
        for (name, (cur_val, prev_val)) in [
            (
                "user",
                (current.system.cpu_total.user, prev.system.cpu_total.user),
            ),
            (
                "nice",
                (current.system.cpu_total.nice, prev.system.cpu_total.nice),
            ),
            (
                "system",
                (
                    current.system.cpu_total.system,
                    prev.system.cpu_total.system,
                ),
            ),
            (
                "idle",
                (current.system.cpu_total.idle, prev.system.cpu_total.idle),
            ),
            (
                "iowait",
                (
                    current.system.cpu_total.iowait,
                    prev.system.cpu_total.iowait,
                ),
            ),
            (
                "irq",
                (current.system.cpu_total.irq, prev.system.cpu_total.irq),
            ),
            (
                "softirq",
                (
                    current.system.cpu_total.softirq,
                    prev.system.cpu_total.softirq,
                ),
            ),
            (
                "steal",
                (current.system.cpu_total.steal, prev.system.cpu_total.steal),
            ),
            (
                "guest",
                (current.system.cpu_total.guest, prev.system.cpu_total.guest),
            ),
            (
                "guest_nice",
                (
                    current.system.cpu_total.guest_nice,
                    prev.system.cpu_total.guest_nice,
                ),
            ),
        ] {
            out.cpu_time_delta_secs
                .insert(name, cur_val.saturating_sub(prev_val) as f64 / hz);
        }

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
            .unwrap_or(0)
            .saturating_sub(prev.system.forks_since_boot.unwrap_or(0));

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
        let dirty_total =
            current.memory.dirty_bytes.unwrap_or(0) + current.memory.writeback_bytes.unwrap_or(0);
        if current.memory.mem_total_bytes > 0 {
            out.dirty_writeback_ratio = dirty_total as f64 / current.memory.mem_total_bytes as f64;
        }

        let vmstat_delta = |key: &str| -> u64 {
            non_negative_i64_delta(
                current.vmstat.get(key).copied(),
                prev.vmstat.get(key).copied(),
            )
        };
        out.page_faults_delta = vmstat_delta("pgfault");
        out.page_faults_per_sec = out.page_faults_delta as f64 / secs;
        out.major_page_faults_delta = vmstat_delta("pgmajfault");
        out.major_page_faults_per_sec = out.major_page_faults_delta as f64 / secs;
        out.page_ins_delta = vmstat_delta("pgpgin");
        out.page_ins_per_sec = out.page_ins_delta as f64 / secs;
        out.page_outs_delta = vmstat_delta("pgpgout");
        out.page_outs_per_sec = out.page_outs_delta as f64 / secs;
        out.swap_ins_delta = vmstat_delta("pswpin");
        out.swap_ins_per_sec = out.swap_ins_delta as f64 / secs;
        out.swap_outs_delta = vmstat_delta("pswpout");
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

        let prev_softnet_by_cpu: HashMap<usize, &SoftnetCpuSnapshot> =
            prev.softnet.iter().map(|cpu| (cpu.cpu, cpu)).collect();

        let mut softnet_processed = 0u64;
        let mut softnet_dropped = 0u64;
        let mut softnet_time_squeezed = 0u64;
        for cur in &current.softnet {
            if let Some(prv) = prev_softnet_by_cpu.get(&cur.cpu) {
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
            let util = if td > 0 { bd as f64 / td as f64 } else { 0.0 };
            out.per_cpu_utilization_ratio.push((idx, util));
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

        let prev_disks_by_name: HashMap<&str, &DiskSnapshot> =
            prev.disks.iter().map(|d| (d.name.as_str(), d)).collect();
        for cur in &current.disks {
            if let Some(prv) = prev_disks_by_name.get(cur.name.as_str()) {
                if !cur.has_counters || !prv.has_counters {
                    continue;
                }
                let sector_bytes = cur.logical_block_size.unwrap_or(512) as f64;
                let read_sectors = cur.sectors_read.saturating_sub(prv.sectors_read);
                let write_sectors = cur.sectors_written.saturating_sub(prv.sectors_written);
                let read_bytes = read_sectors as f64 * sector_bytes;
                let write_bytes = write_sectors as f64 * sector_bytes;
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
                    .insert(cur.name.clone(), ratio(read_time_ms, reads));
                out.disk_write_await_ms
                    .insert(cur.name.clone(), ratio(write_time_ms, writes));
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

        let prev_net_by_name: HashMap<&str, &NetDevSnapshot> =
            prev.net.iter().map(|n| (n.name.as_str(), n)).collect();
        for cur in &current.net {
            if let Some(prv) = prev_net_by_name.get(cur.name.as_str()) {
                let rx_bytes = cur.rx_bytes.saturating_sub(prv.rx_bytes);
                let tx_bytes = cur.tx_bytes.saturating_sub(prv.tx_bytes);
                let rx_packets = cur.rx_packets.saturating_sub(prv.rx_packets);
                let tx_packets = cur.tx_packets.saturating_sub(prv.tx_packets);
                let rx_errs = cur.rx_errs.saturating_sub(prv.rx_errs);
                let tx_errs = cur.tx_errs.saturating_sub(prv.tx_errs);
                let rx_drop = cur.rx_drop.saturating_sub(prv.rx_drop);
                let tx_drop = cur.tx_drop.saturating_sub(prv.tx_drop);

                out.net_rx_bytes_per_sec
                    .insert(cur.name.clone(), per_sec(rx_bytes, secs));
                out.net_rx_bytes_delta.insert(cur.name.clone(), rx_bytes);
                out.net_tx_bytes_per_sec
                    .insert(cur.name.clone(), per_sec(tx_bytes, secs));
                out.net_tx_bytes_delta.insert(cur.name.clone(), tx_bytes);
                out.net_total_bytes_per_sec
                    .insert(cur.name.clone(), per_sec(rx_bytes + tx_bytes, secs));
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

        let prev_processes_by_pid: HashMap<i32, &ProcessSnapshot> =
            prev.processes.iter().map(|p| (p.pid, p)).collect();
        for cur in &current.processes {
            if let Some(prv) = prev_processes_by_pid.get(&cur.pid) {
                let cur_total = cur.utime_ticks + cur.stime_ticks;
                let prv_total = prv.utime_ticks + prv.stime_ticks;
                let dticks = cur_total.saturating_sub(prv_total) as f64;
                let cpu_ratio = (dticks / hz) / secs / cpu_count;
                out.process_cpu_ratio.insert(cur.pid, cpu_ratio.max(0.0));
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

                if let (Some(cv), Some(pv)) = (cur.read_bytes, prv.read_bytes) {
                    out.process_read_bytes_delta
                        .insert(cur.pid, cv.saturating_sub(pv));
                }
                if let (Some(cv), Some(pv)) = (cur.write_bytes, prv.write_bytes) {
                    out.process_write_bytes_delta
                        .insert(cur.pid, cv.saturating_sub(pv));
                }
                if let (Some(cv), Some(pv)) = (cur.read_chars, prv.read_chars) {
                    out.process_read_chars_delta
                        .insert(cur.pid, cv.saturating_sub(pv));
                }
                if let (Some(cv), Some(pv)) = (cur.write_chars, prv.write_chars) {
                    out.process_write_chars_delta
                        .insert(cur.pid, cv.saturating_sub(pv));
                }
                if let (Some(cv), Some(pv)) = (cur.syscr, prv.syscr) {
                    out.process_syscr_delta
                        .insert(cur.pid, cv.saturating_sub(pv));
                }
                if let (Some(cv), Some(pv)) = (cur.syscw, prv.syscw) {
                    out.process_syscw_delta
                        .insert(cur.pid, cv.saturating_sub(pv));
                }
                if let (Some(cv), Some(pv)) =
                    (cur.voluntary_ctxt_switches, prv.voluntary_ctxt_switches)
                {
                    out.process_voluntary_ctxt_delta
                        .insert(cur.pid, cv.saturating_sub(pv));
                }
                if let (Some(cv), Some(pv)) = (
                    cur.nonvoluntary_ctxt_switches,
                    prv.nonvoluntary_ctxt_switches,
                ) {
                    out.process_nonvoluntary_ctxt_delta
                        .insert(cur.pid, cv.saturating_sub(pv));
                }
            }
        }

        self.last = Some(current.clone());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::PrevState;
    use crate::model::{
        CpuTimes, DiskSnapshot, NetDevSnapshot, ProcessSnapshot, Snapshot, SoftnetCpuSnapshot,
    };
    use std::time::Duration;

    fn approx_eq(left: f64, right: f64) {
        let diff = (left - right).abs();
        assert!(diff < 1e-9, "left={left}, right={right}, diff={diff}");
    }

    #[test]
    fn first_sample_returns_empty_derived_and_sets_previous_state() {
        let mut state = PrevState::default();
        let current = Snapshot::default();

        let out = state.derive(&current, Duration::from_secs(1));

        assert_eq!(out.cpu_utilization_ratio, 0.0);
        assert!(state.last.is_some());
    }

    #[test]
    fn cpu_cycle_utilization_overrides_tick_based_ratio() {
        let mut state = PrevState {
            last: Some(Snapshot::default()),
        };

        let mut current = Snapshot::default();
        current.system.cpu_cycle_utilization = Some(0.73);
        current.system.cpu_total = CpuTimes {
            user: 100,
            nice: 0,
            system: 0,
            idle: 100,
            iowait: 0,
            irq: 0,
            softirq: 0,
            steal: 0,
            guest: 0,
            guest_nice: 0,
        };

        let out = state.derive(&current, Duration::from_secs(1));
        approx_eq(out.cpu_utilization_ratio, 0.73);
    }

    #[test]
    fn cpu_utilization_falls_back_to_delta_ratio_when_cycle_utilization_missing() {
        let mut prev = Snapshot::default();
        prev.system.cpu_total = CpuTimes {
            user: 10,
            nice: 0,
            system: 10,
            idle: 80,
            iowait: 0,
            irq: 0,
            softirq: 0,
            steal: 0,
            guest: 0,
            guest_nice: 0,
        };

        let mut current = Snapshot::default();
        current.system.ticks_per_second = 100;
        current.system.cpu_total = CpuTimes {
            user: 20,
            nice: 0,
            system: 20,
            idle: 100,
            iowait: 0,
            irq: 0,
            softirq: 0,
            steal: 0,
            guest: 0,
            guest_nice: 0,
        };

        let mut state = PrevState { last: Some(prev) };

        let out = state.derive(&current, Duration::from_secs(2));
        approx_eq(out.cpu_utilization_ratio, 0.5);
        assert_eq!(out.cpu_time_delta_secs.get("user").copied(), Some(0.1));
    }

    #[test]
    fn vmstat_delta_ignores_negative_values() {
        let mut prev = Snapshot::default();
        prev.vmstat.insert("pgfault".to_string(), 100);

        let mut current = Snapshot::default();
        current.system.ticks_per_second = 100;
        current.vmstat.insert("pgfault".to_string(), -1);

        let mut state = PrevState { last: Some(prev) };
        let out = state.derive(&current, Duration::from_secs(1));

        assert_eq!(out.page_faults_delta, 0);
        approx_eq(out.page_faults_per_sec, 0.0);
    }

    #[test]
    fn derive_emits_disk_network_process_and_kernel_deltas() {
        let mut prev = Snapshot::default();
        prev.system.ticks_per_second = 100;
        prev.system.cpu_total = CpuTimes {
            user: 100,
            nice: 10,
            system: 20,
            idle: 200,
            iowait: 10,
            irq: 5,
            softirq: 5,
            steal: 0,
            guest: 0,
            guest_nice: 0,
        };
        prev.system.per_cpu = vec![prev.system.cpu_total.clone()];
        prev.system.interrupts_total = 1_000;
        prev.system.softirqs_total = 500;
        prev.system.context_switches = 300;
        prev.system.forks_since_boot = Some(10);
        prev.interrupts.insert("irq0".to_string(), 10);
        prev.softirqs.insert("soft0".to_string(), 20);
        prev.memory.mem_total_bytes = 1_000;
        prev.memory.mem_available_bytes = 700;
        prev.memory.swap_total_bytes = 1_000;
        prev.memory.swap_free_bytes = 800;
        prev.memory.dirty_bytes = Some(20);
        prev.memory.writeback_bytes = Some(30);
        prev.vmstat.insert("pgfault".to_string(), 100);
        prev.vmstat.insert("pgmajfault".to_string(), 20);
        prev.vmstat.insert("pgpgin".to_string(), 50);
        prev.vmstat.insert("pgpgout".to_string(), 70);
        prev.vmstat.insert("pswpin".to_string(), 5);
        prev.vmstat.insert("pswpout".to_string(), 7);
        prev.pressure_totals_us
            .insert("cpu.some".to_string(), 1_000_000);
        prev.net_snmp.insert("Ip.InDiscards".to_string(), 10);
        prev.net_snmp.insert("Ip.OutDiscards".to_string(), 12);
        prev.net_snmp.insert("Tcp.RetransSegs".to_string(), 14);
        prev.net_snmp.insert("Udp.InErrors".to_string(), 16);
        prev.net_snmp.insert("Udp.RcvbufErrors".to_string(), 18);
        prev.softnet.push(SoftnetCpuSnapshot {
            cpu: 0,
            processed: 100,
            dropped: 20,
            time_squeezed: 6,
            ..SoftnetCpuSnapshot::default()
        });
        prev.disks.push(DiskSnapshot {
            name: "sda".to_string(),
            has_counters: true,
            reads: 100,
            writes: 60,
            sectors_read: 2_000,
            sectors_written: 4_000,
            time_reading_ms: 500,
            time_writing_ms: 800,
            time_in_progress_ms: 1_000,
            weighted_time_in_progress_ms: 2_000,
            logical_block_size: Some(512),
            ..DiskSnapshot::default()
        });
        prev.disks.push(DiskSnapshot {
            name: "loop0".to_string(),
            has_counters: false,
            ..DiskSnapshot::default()
        });
        prev.net.push(NetDevSnapshot {
            name: "eth0".to_string(),
            rx_bytes: 1_000,
            tx_bytes: 2_000,
            rx_packets: 100,
            tx_packets: 200,
            rx_errs: 2,
            tx_errs: 3,
            rx_drop: 4,
            tx_drop: 5,
            ..NetDevSnapshot::default()
        });
        prev.processes.push(ProcessSnapshot {
            pid: 100,
            utime_ticks: 100,
            stime_ticks: 50,
            minflt: 10,
            majflt: 1,
            read_bytes: Some(1_000),
            write_bytes: Some(2_000),
            read_chars: Some(3_000),
            write_chars: Some(4_000),
            syscr: Some(100),
            syscw: Some(200),
            voluntary_ctxt_switches: Some(10),
            nonvoluntary_ctxt_switches: Some(20),
            ..ProcessSnapshot::default()
        });

        let mut current = prev.clone();
        current.system.cpu_cycle_utilization = None;
        current.system.cpu_total.user += 100;
        current.system.cpu_total.system += 20;
        current.system.cpu_total.idle += 80;
        current.system.cpu_total.iowait += 10;
        current.system.per_cpu[0] = current.system.cpu_total.clone();
        current.system.interrupts_total += 60;
        current.system.softirqs_total += 40;
        current.system.context_switches += 20;
        current.system.forks_since_boot = Some(13);
        current.interrupts.insert("irq0".to_string(), 15);
        current.softirqs.insert("soft0".to_string(), 21);
        current.memory.mem_available_bytes = 600;
        current.memory.swap_free_bytes = 600;
        current.memory.dirty_bytes = Some(50);
        current.memory.writeback_bytes = Some(10);
        current.vmstat.insert("pgfault".to_string(), 160);
        current.vmstat.insert("pgmajfault".to_string(), 24);
        current.vmstat.insert("pgpgin".to_string(), 60);
        current.vmstat.insert("pgpgout".to_string(), 80);
        current.vmstat.insert("pswpin".to_string(), 8);
        current.vmstat.insert("pswpout".to_string(), 11);
        current
            .pressure_totals_us
            .insert("cpu.some".to_string(), 2_500_000);
        current.net_snmp.insert("Ip.InDiscards".to_string(), 14);
        current.net_snmp.insert("Ip.OutDiscards".to_string(), 13);
        current.net_snmp.insert("Tcp.RetransSegs".to_string(), 19);
        current.net_snmp.insert("Udp.InErrors".to_string(), 20);
        current.net_snmp.insert("Udp.RcvbufErrors".to_string(), 22);
        current.softnet[0].processed = 160;
        current.softnet[0].dropped = 24;
        current.softnet[0].time_squeezed = 10;
        current.softnet.push(SoftnetCpuSnapshot {
            cpu: 1,
            processed: 50,
            dropped: 5,
            time_squeezed: 2,
            ..SoftnetCpuSnapshot::default()
        });
        current.disks[0].reads += 20;
        current.disks[0].writes += 10;
        current.disks[0].sectors_read += 400;
        current.disks[0].sectors_written += 600;
        current.disks[0].time_reading_ms += 200;
        current.disks[0].time_writing_ms += 150;
        current.disks[0].time_in_progress_ms += 300;
        current.disks[0].weighted_time_in_progress_ms += 500;
        current.disks[1].reads += 10;
        current.net[0].rx_bytes += 300;
        current.net[0].tx_bytes += 500;
        current.net[0].rx_packets += 30;
        current.net[0].tx_packets += 20;
        current.net[0].rx_errs += 1;
        current.net[0].tx_errs += 2;
        current.net[0].rx_drop += 2;
        current.net[0].tx_drop += 3;
        current.processes[0].utime_ticks += 30;
        current.processes[0].stime_ticks += 10;
        current.processes[0].minflt += 6;
        current.processes[0].majflt += 2;
        current.processes[0].read_bytes = Some(1_500);
        current.processes[0].write_bytes = Some(2_800);
        current.processes[0].read_chars = Some(3_700);
        current.processes[0].write_chars = Some(4_900);
        current.processes[0].syscr = Some(110);
        current.processes[0].syscw = Some(209);
        current.processes[0].voluntary_ctxt_switches = Some(15);
        current.processes[0].nonvoluntary_ctxt_switches = Some(23);

        let mut state = PrevState { last: Some(prev) };
        let out = state.derive(&current, Duration::from_secs(2));

        approx_eq(out.cpu_utilization_ratio, 0.5714285714285714);
        assert_eq!(out.interrupts_delta, 60);
        assert_eq!(out.softirqs_delta, 40);
        assert_eq!(out.context_switches_delta, 20);
        assert_eq!(out.forks_delta, 3);
        approx_eq(out.memory_used_ratio, 0.4);
        approx_eq(out.swap_used_ratio, 0.4);
        approx_eq(out.dirty_writeback_ratio, 0.06);
        assert_eq!(out.page_faults_delta, 60);
        approx_eq(out.page_faults_per_sec, 30.0);
        assert_eq!(out.major_page_faults_delta, 4);
        assert_eq!(out.page_ins_delta, 10);
        assert_eq!(out.page_outs_delta, 10);
        assert_eq!(out.swap_ins_delta, 3);
        assert_eq!(out.swap_outs_delta, 4);
        approx_eq(out.pressure_total_delta_secs["cpu.some"], 1.5);
        approx_eq(out.kernel_ip_in_discards_per_sec, 2.0);
        approx_eq(out.kernel_ip_out_discards_per_sec, 0.5);
        approx_eq(out.kernel_tcp_retrans_segs_per_sec, 2.5);
        approx_eq(out.kernel_udp_in_errors_per_sec, 2.0);
        approx_eq(out.kernel_udp_rcvbuf_errors_per_sec, 2.0);
        approx_eq(out.softnet_processed_per_sec, 30.0);
        approx_eq(out.softnet_dropped_per_sec, 2.0);
        approx_eq(out.softnet_time_squeezed_per_sec, 2.0);
        approx_eq(out.softnet_drop_ratio, 0.0625);
        assert_eq!(out.per_cpu_utilization_ratio.len(), 1);
        assert_eq!(out.disk_reads_delta["sda"], 20);
        approx_eq(out.disk_total_iops["sda"], 15.0);
        assert!(!out.disk_reads_delta.contains_key("loop0"));
        assert_eq!(out.net_rx_bytes_delta["eth0"], 300);
        assert_eq!(out.net_tx_bytes_delta["eth0"], 500);
        approx_eq(out.net_total_bytes_per_sec["eth0"], 400.0);
        assert_eq!(out.process_minor_faults_delta[&100], 6);
        assert_eq!(out.process_major_faults_delta[&100], 2);
        assert_eq!(out.process_read_bytes_delta[&100], 500);
        assert_eq!(out.process_write_bytes_delta[&100], 800);
        assert_eq!(out.process_syscr_delta[&100], 10);
        assert_eq!(out.process_syscw_delta[&100], 9);
        assert_eq!(out.process_voluntary_ctxt_delta[&100], 5);
        assert_eq!(out.process_nonvoluntary_ctxt_delta[&100], 3);
        assert!(state.last.is_some());
    }

    #[test]
    fn derive_clamps_zero_elapsed_duration_to_small_positive_value() {
        let mut prev = Snapshot::default();
        prev.vmstat.insert("pgfault".to_string(), 10);

        let mut current = Snapshot::default();
        current.vmstat.insert("pgfault".to_string(), 12);

        let mut state = PrevState { last: Some(prev) };
        let out = state.derive(&current, Duration::from_secs(0));

        assert_eq!(out.page_faults_delta, 2);
        approx_eq(out.page_faults_per_sec, 2000.0);
    }
}
