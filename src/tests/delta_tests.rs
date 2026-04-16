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

#[test]
fn derive_covers_zero_totals_and_zero_io_else_branches() {
    let mut prev = Snapshot::default();
    prev.system.ticks_per_second = 100;
    prev.system.per_cpu = vec![CpuTimes {
        user: 10,
        system: 5,
        idle: 20,
        iowait: 2,
        ..CpuTimes::default()
    }];
    prev.disks.push(DiskSnapshot {
        name: "sda".to_string(),
        has_counters: true,
        reads: 10,
        writes: 20,
        sectors_read: 100,
        sectors_written: 200,
        logical_block_size: Some(512),
        ..DiskSnapshot::default()
    });
    prev.processes.push(ProcessSnapshot {
        pid: 42,
        utime_ticks: 5,
        stime_ticks: 5,
        ..ProcessSnapshot::default()
    });

    let mut current = prev.clone();
    current.disks.push(DiskSnapshot {
        name: "nvme0n1".to_string(),
        has_counters: true,
        ..DiskSnapshot::default()
    });
    current.processes.push(ProcessSnapshot {
        pid: 7,
        ..ProcessSnapshot::default()
    });

    let mut state = PrevState { last: Some(prev) };
    let out = state.derive(&current, Duration::from_secs(1));

    assert_eq!(out.per_cpu_iowait_ratio[0].1, 0.0);
    assert_eq!(out.per_cpu_system_ratio[0].1, 0.0);
    assert_eq!(out.disk_avg_read_size_bytes["sda"], 0.0);
    assert_eq!(out.disk_avg_write_size_bytes["sda"], 0.0);
    assert!(out.process_cpu_ratio.contains_key(&42));
}

#[test]
fn derive_skips_non_positive_irq_deltas_and_handles_zero_packet_loss_ratio() {
    let mut prev = Snapshot::default();
    prev.system.ticks_per_second = 100;
    prev.interrupts.insert("IRQ0|cpu0".to_string(), 10);
    prev.softirqs.insert("NET_RX|cpu0".to_string(), 20);
    prev.net.push(NetDevSnapshot {
        name: "eth0".to_string(),
        rx_packets: 0,
        tx_packets: 0,
        rx_errs: 0,
        tx_errs: 0,
        rx_drop: 0,
        tx_drop: 0,
        ..NetDevSnapshot::default()
    });

    let mut current = prev.clone();
    current.interrupts.insert("IRQ0|cpu0".to_string(), 10);
    current.softirqs.insert("NET_RX|cpu0".to_string(), 20);
    current.net[0].rx_packets = 0;
    current.net[0].tx_packets = 0;
    current.net[0].rx_errs = 0;
    current.net[0].tx_errs = 0;
    current.net[0].rx_drop = 0;
    current.net[0].tx_drop = 0;

    let mut state = PrevState { last: Some(prev) };
    let out = state.derive(&current, Duration::from_secs(1));

    assert!(!out.linux_interrupts_delta.contains_key("IRQ0|cpu0"));
    assert!(!out.linux_softirqs_delta.contains_key("NET_RX|cpu0"));
    assert_eq!(out.net_rx_loss_ratio.get("eth0").copied(), Some(0.0));
    assert_eq!(out.net_tx_loss_ratio.get("eth0").copied(), Some(0.0));
}
