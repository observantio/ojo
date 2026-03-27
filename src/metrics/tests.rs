#[cfg(test)]
mod tests {
    use crate::metrics::{MetricFilter, ProcessLabelConfig};
    use crate::delta::DerivedMetrics;
    use crate::model::{
        CpuInfoSnapshot, MountSnapshot, ProcessSnapshot, Snapshot, SoftnetCpuSnapshot,
        SwapDeviceSnapshot, WindowsSnapshot,
    };
    use std::time::Duration;

    #[test]
    fn metric_filter_allows_all_when_no_rules() {
        let filter = MetricFilter::new(vec![], vec![]);
        assert!(filter.enabled("system.cpu.time"));
        assert!(filter.enabled("process.memory.usage"));
    }

    #[test]
    fn metric_filter_respects_include_prefixes() {
        let filter = MetricFilter::new(vec!["system.".to_string()], vec![]);
        assert!(filter.enabled("system.cpu.time"));
        assert!(!filter.enabled("process.cpu.time"));
    }

    #[test]
    fn metric_filter_exclude_wins_over_include() {
        let filter = MetricFilter::new(
            vec!["system.".to_string(), "process.".to_string()],
            vec!["process.".to_string()],
        );
        assert!(filter.enabled("system.cpu.time"));
        assert!(!filter.enabled("process.cpu.time"));
    }

    #[test]
    fn metric_filter_matches_exact_and_group_roots() {
        let filter = MetricFilter::new(vec!["system.cpu".to_string()], vec![]);
        assert!(filter.enabled("system.cpu.time"));
        assert!(!filter.enabled("system.memory.total"));

        let prefix_filter = MetricFilter::new(vec!["system.cpu.".to_string()], vec![]);
        assert!(prefix_filter.enabled("system.cpu"));
        assert!(prefix_filter.enabled("system.cpu.utilization"));
    }

    #[test]
    fn process_label_config_defaults_to_low_cardinality() {
        let cfg = ProcessLabelConfig::default();
        assert!(!cfg.include_pid);
        assert!(cfg.include_command);
        assert!(cfg.include_state);
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[test]
    fn proc_metrics_record_smoke_linux_like() {
        let snap = crate::collector::collect_snapshot(true).expect("collect snapshot");

        let mut prev = crate::delta::PrevState::default();
        let _first = prev.derive(&snap, Duration::from_secs(1));
        let derived = prev.derive(&snap, Duration::from_secs(1));

        let meter = opentelemetry::global::meter("metrics-test");
        let metrics = crate::metrics::ProcMetrics::new(
            meter,
            MetricFilter::new(vec![], vec![]),
            ProcessLabelConfig::default(),
        );
        metrics.record(&snap, &derived, true);
    }

    #[test]
    fn core_helpers_cover_edge_inputs() {
        let proc = ProcessSnapshot {
            rss_pages: -1,
            resident_bytes: None,
            working_set_bytes: Some(4096),
            ..ProcessSnapshot::default()
        };
        assert_eq!(super::non_negative_u64(-1_i64), 0);
        assert_eq!(super::non_negative_u64(7_i64), 7);
        assert_eq!(super::pages_to_bytes_4k(-1), 0);
        assert_eq!(super::pages_to_bytes_4k(2), 8192);
        assert_eq!(super::process_rss_bytes(&proc, true), Some(4096));
        assert_eq!(super::process_rss_bytes(&proc, false), None);
        assert_eq!(super::kib_to_bytes(2), 2048);

        let mut snap = Snapshot::default();
        snap.system.os_type = "linux".to_string();
        assert!(super::is_linux_like(&snap));
        snap.system.os_type = "windows".to_string();
        assert!(!super::is_linux_like(&snap));
    }

    #[test]
    fn proc_metrics_record_exercises_linux_and_windows_branches() {
        let meter = opentelemetry::global::meter("metrics-branch-coverage");
        let metrics = crate::metrics::ProcMetrics::new(
            meter,
            MetricFilter::new(vec![], vec![]),
            ProcessLabelConfig {
                include_pid: true,
                include_command: true,
                include_state: true,
            },
        );

        let mut linux_snap = Snapshot::default();
        linux_snap.system.os_type = "linux".to_string();
        linux_snap.system.ticks_per_second = 100;
        linux_snap.system.boot_time_epoch_secs = 1_000;
        linux_snap.system.forks_since_boot = Some(9);
        linux_snap.system.procs_running = 2;
        linux_snap.system.procs_blocked = 1;
        linux_snap.pressure.insert("cpu.some.avg10".to_string(), 0.2);
        linux_snap.pressure.insert("invalid".to_string(), 0.1);
        linux_snap
            .pressure_totals_us
            .insert("cpu.some".to_string(), 1000);
        linux_snap
            .pressure_totals_us
            .insert("bad.key.extra".to_string(), 2000);
        linux_snap.vmstat.insert("pgfault".to_string(), 10);
        linux_snap.net_snmp.insert("Ip.InDiscards".to_string(), 2);
        linux_snap
            .net_stat
            .insert("Tcp.RetransSegs".to_string(), 3);
        linux_snap.net_stat.insert("nosplit".to_string(), 1);
        linux_snap.sockets.insert("tcp".to_string(), 4);
        linux_snap
            .schedstat
            .insert("cpu|running|0".to_string(), 5);
        linux_snap
            .schedstat
            .insert("bad|running|0".to_string(), 6);
        linux_snap.runqueue_depth.insert("cpu|0".to_string(), 0.5);
        linux_snap.runqueue_depth.insert("all|0".to_string(), 0.3);
        linux_snap
            .slabinfo
            .insert("kmalloc-64|active_objs|value".to_string(), 7);
        linux_snap.slabinfo.insert("broken".to_string(), 1);
        linux_snap
            .cgroup
            .insert("v2/root|memory.current|value".to_string(), 8);
        linux_snap
            .cgroup
            .insert("v2/root|io.stat|8:0|rbytes".to_string(), 9);
        linux_snap.filesystem.insert("/|total_bytes|value".to_string(), 10);
        linux_snap.swaps.push(SwapDeviceSnapshot {
            device: "/dev/sda2".to_string(),
            swap_type: "partition".to_string(),
            size_bytes: 100,
            used_bytes: 50,
            priority: -2,
        });
        linux_snap
            .zoneinfo
            .insert("node0|DMA|nr_free_pages".to_string(), 11);
        linux_snap
            .buddyinfo
            .insert("node0|DMA|0".to_string(), 12);
        linux_snap.mounts.push(MountSnapshot {
            device: "/dev/sda1".to_string(),
            mountpoint: "/".to_string(),
            fs_type: "ext4".to_string(),
            read_only: false,
        });
        linux_snap.cpuinfo.push(CpuInfoSnapshot {
            cpu: 0,
            vendor_id: Some("GenuineIntel".to_string()),
            model_name: Some("x".to_string()),
            mhz: Some(1000.0),
            cache_size_bytes: Some(1024),
        });
        linux_snap.softnet.push(SoftnetCpuSnapshot {
            cpu: 0,
            processed: 20,
            dropped: 1,
            time_squeezed: 2,
            ..SoftnetCpuSnapshot::default()
        });
        linux_snap.processes.push(ProcessSnapshot {
            pid: 10,
            ppid: 1,
            comm: "proc".to_string(),
            state: "R".to_string(),
            num_threads: 2,
            priority: 20,
            nice: 0,
            vsize_bytes: 1000,
            rss_pages: 2,
            utime_ticks: 10,
            stime_ticks: 5,
            start_time_ticks: 100,
            fd_count: Some(4),
            oom_score: Some(100),
            processor: Some(1),
            rt_priority: Some(1),
            policy: Some(0),
            read_bytes: Some(20),
            write_bytes: Some(30),
            read_chars: Some(40),
            write_chars: Some(50),
            syscr: Some(6),
            syscw: Some(7),
            cancelled_write_bytes: Some(3),
            vm_size_kib: Some(8),
            vm_rss_kib: Some(9),
            vm_data_kib: Some(10),
            vm_stack_kib: Some(11),
            vm_exe_kib: Some(12),
            vm_lib_kib: Some(13),
            vm_swap_kib: Some(14),
            vm_pte_kib: Some(15),
            vm_hwm_kib: Some(16),
            voluntary_ctxt_switches: Some(2),
            nonvoluntary_ctxt_switches: Some(1),
            minflt: 5,
            majflt: 1,
            ..ProcessSnapshot::default()
        });
        linux_snap.processes.push(ProcessSnapshot {
            comm: "".to_string(),
            ..ProcessSnapshot::default()
        });

        let mut linux_derived = DerivedMetrics::default();
        linux_derived
            .pressure_total_delta_secs
            .insert("cpu.some".to_string(), 0.1);
        linux_derived
            .pressure_total_delta_secs
            .insert("bad.extra.key".to_string(), 0.2);
        linux_derived
            .linux_interrupts_delta
            .insert("IRQ0|cpu0".to_string(), 3);
        linux_derived
            .linux_interrupts_delta
            .insert("broken".to_string(), 2);
        linux_derived
            .linux_softirqs_delta
            .insert("NET_RX|cpu0".to_string(), 4);
        linux_derived
            .linux_softirqs_delta
            .insert("broken".to_string(), 1);
        linux_derived.kernel_ip_in_discards_per_sec = 1.0;
        linux_derived.kernel_ip_out_discards_per_sec = 2.0;
        linux_derived.kernel_tcp_retrans_segs_per_sec = 3.0;
        linux_derived.kernel_udp_in_errors_per_sec = 4.0;
        linux_derived.kernel_udp_rcvbuf_errors_per_sec = 5.0;
        linux_derived.softnet_processed_per_sec = 6.0;
        linux_derived.softnet_dropped_per_sec = 7.0;
        linux_derived.softnet_time_squeezed_per_sec = 8.0;
        linux_derived.softnet_drop_ratio = 0.1;
        linux_derived.process_cpu_ratio.insert(10, 0.5);
        linux_derived.process_cpu_user_delta_secs.insert(10, 0.2);
        linux_derived.process_cpu_system_delta_secs.insert(10, 0.1);
        linux_derived.process_read_bytes_delta.insert(10, 3);
        linux_derived.process_write_bytes_delta.insert(10, 4);
        linux_derived.process_read_chars_delta.insert(10, 5);
        linux_derived.process_write_chars_delta.insert(10, 6);
        linux_derived.process_syscr_delta.insert(10, 1);
        linux_derived.process_syscw_delta.insert(10, 1);
        linux_derived.process_voluntary_ctxt_delta.insert(10, 1);
        linux_derived.process_nonvoluntary_ctxt_delta.insert(10, 1);
        linux_derived.process_minor_faults_delta.insert(10, 2);
        linux_derived.process_major_faults_delta.insert(10, 1);

        metrics.record(&linux_snap, &linux_derived, true);

        let mut windows_snap = Snapshot::default();
        windows_snap.system.os_type = "windows".to_string();
        windows_snap.system.is_windows = true;
        windows_snap.system.ticks_per_second = 100;
        windows_snap.system.boot_time_epoch_secs = 2_000;
        windows_snap.vmstat.insert("pgfault".to_string(), 2);
        windows_snap.net_snmp.insert("Ip.InDiscards".to_string(), 1);
        windows_snap.sockets.insert("tcp".to_string(), 2);
        windows_snap.windows = Some(WindowsSnapshot {
            vmstat: [("hard_faults".to_string(), 3)].into_iter().collect(),
            interrupts: [("isr".to_string(), 4)].into_iter().collect(),
            dpc: [("dpc".to_string(), 5)].into_iter().collect(),
            ..WindowsSnapshot::default()
        });
        windows_snap.processes.push(ProcessSnapshot {
            pid: 20,
            ppid: 2,
            comm: "winproc".to_string(),
            state: "R".to_string(),
            num_threads: 3,
            priority: 8,
            start_time_ticks: 200,
            read_bytes: Some(10),
            write_bytes: Some(11),
            virtual_size_bytes: Some(12),
            working_set_bytes: Some(13),
            peak_working_set_bytes: Some(14),
            pagefile_usage_bytes: Some(15),
            private_bytes: Some(16),
            commit_charge_bytes: Some(17),
            ..ProcessSnapshot::default()
        });

        let mut windows_derived = DerivedMetrics::default();
        windows_derived.process_cpu_ratio.insert(20, 0.25);
        metrics.record(&windows_snap, &windows_derived, true);
    }
}
