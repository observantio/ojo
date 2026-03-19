pub fn collect_snapshot(include_process_metrics: bool) -> Result<Snapshot> {
    debug!("wincollect: collect_snapshot start");
    let mut support_state = windows_support_state();

    let process_info_buf = match query_system_information(SYSTEM_PROCESS_INFORMATION_CLASS) {
        Ok(buf) => Some(buf),
        Err(e) => {
            support_state.insert(
                "snapshot.windows.process_info.error".to_string(),
                e.to_string(),
            );
            None
        }
    };

    let (system, system_ok) = match collect_system(process_info_buf.as_deref()) {
        Ok(s) => (s, true),
        Err(e) => {
            support_state.insert("snapshot.windows.system.error".to_string(), e.to_string());
            (SystemSnapshot::default(), false)
        }
    };
    support_state.insert(
        "snapshot.core.system".to_string(),
        if system_ok {
            "collected".to_string()
        } else {
            "fallback_default_due_to_collection_failure".to_string()
        },
    );

    debug!("wincollect: collect_system done");
    let perf = system_performance_info();
    let memory = collect_memory(perf.as_ref())?;
    debug!("wincollect: collect_memory done");
    let synthetic_load = with_wincollect_state(|state| {
        collect_synthetic_load(&system.cpu_total, system.procs_running, state)
    })?;
    debug!("wincollect: collect_synthetic_load done");
    let disks = with_wincollect_state(|state| collect_disks(state))?;
    let disks_with_counters = disks.iter().filter(|d| d.has_counters).count();
    let disks_without_counters: Vec<String> = disks
        .iter()
        .filter(|d| !d.has_counters)
        .map(|d| d.name.clone())
        .collect();
    debug!(
        disk_count = disks.len(),
        disks_with_counters,
        disks_without_counters = ?disks_without_counters,
        "wincollect: collect_disks done"
    );
    let net = collect_net()?;
    debug!(iface_count = net.len(), "wincollect: collect_net done");
    let vmstat = collect_vmstat(perf.as_ref());
    debug!(
        vmstat_keys = vmstat.len(),
        "wincollect: collect_vmstat done"
    );
    let net_snmp = collect_net_snmp();
    debug!(
        snmp_keys = net_snmp.len(),
        "wincollect: collect_net_snmp done"
    );
    let sockets = collect_socket_counts();
    debug!(
        socket_keys = sockets.len(),
        "wincollect: collect_sockets done"
    );
    let windows_interrupts = collect_interrupts_detail(&system.per_cpu);
    let windows_dpc = collect_softirqs_detail(&system.per_cpu);
    let interrupts = BTreeMap::new();
    let softirqs = BTreeMap::new();
    let cpuinfo = collect_cpuinfo();
    let mounts = collect_mounts();
    let filesystem = collect_filesystem_stats(&mounts);
    let disk_volume_correlation = collect_disk_volume_correlation(&mounts);
    let swaps = collect_swaps(&memory);
    let process_mode = if include_process_metrics {
        ProcessMode::Detailed
    } else {
        ProcessMode::Fast
    };

    let processes = collect_processes_from_nt(process_mode, process_info_buf.as_deref())?;
    debug!(
        process_count = processes.len(),
        "wincollect: collect_processes done"
    );
    let mut windows_vmstat = BTreeMap::new();
    let mut vmstat_generic = BTreeMap::new();
    for (k, v) in vmstat {
        if let Some(stripped) = k.strip_prefix("windows.") {
            windows_vmstat.insert(stripped.to_string(), v);
        } else {
            vmstat_generic.insert(k, v);
        }
    }
    for (k, v) in collect_thread_state_vmstat(process_info_buf.as_deref().unwrap_or(&[])) {
        if let Some(stripped) = k.strip_prefix("windows.") {
            windows_vmstat.insert(stripped.to_string(), v);
        }
    }
    for (k, v) in collect_numa_vmstat() {
        if let Some(stripped) = k.strip_prefix("windows.") {
            windows_vmstat.insert(stripped.to_string(), v);
        }
    }
    let isr_total_time_seconds = system.cpu_total.irq as f64 / 10_000_000.0;
    let dpc_total_time_seconds = system.cpu_total.softirq as f64 / 10_000_000.0;
    let hard_fault_total = perf.as_ref().map(|p| p.page_read_count as u64).unwrap_or(0);
    let page_reads_total = perf
        .as_ref()
        .map(|p| p.page_read_io_count as u64)
        .unwrap_or(0);
    let page_writes_total = perf
        .as_ref()
        .map(|p| p.dirty_write_io_count as u64 + p.mapped_write_io_count as u64)
        .unwrap_or(0);
    let windows_pagefiles = collect_windows_pagefiles(&memory);
    let windows_memory_pressure = with_wincollect_state(|state| {
        Ok(collect_windows_memory_pressure(
            &memory,
            hard_fault_total,
            page_reads_total,
            page_writes_total,
            state,
        ))
    })?;
    let windows_commit = collect_windows_commit(&memory);
    let windows_pools = collect_windows_memory_pools(perf.as_ref());
    let load = LoadSnapshot {
        one: synthetic_load.one,
        five: synthetic_load.five,
        fifteen: synthetic_load.fifteen,
        runnable: synthetic_load.runnable_threads,
        entities: synthetic_load.entities,
        latest_pid: 0,
    };

    Ok(Snapshot {
        system,
        memory,
        load: Some(load),
        pressure: BTreeMap::new(),
        pressure_totals_us: BTreeMap::new(),
        vmstat: vmstat_generic,
        interrupts,
        softirqs,
        net_snmp,
        net_stat: BTreeMap::new(),
        sockets,
        schedstat: BTreeMap::new(),
        runqueue_depth: BTreeMap::new(),
        slabinfo: BTreeMap::new(),
        filesystem,
        cgroup: BTreeMap::new(),
        softnet: Vec::new(),
        swaps,
        mounts,
        cpuinfo,
        zoneinfo: BTreeMap::new(),
        buddyinfo: BTreeMap::new(),
        disks,
        net,
        processes,
        support_state,
        metric_classification: windows_metric_classification(),
        windows: Some(WindowsSnapshot {
            vmstat: windows_vmstat,
            interrupts: windows_interrupts,
            dpc: windows_dpc,
            isr_total_time_seconds,
            dpc_total_time_seconds,
            load: Some(WindowsLoadSnapshot {
                synthetic: synthetic_load,
            }),
            pagefiles: windows_pagefiles,
            memory: WindowsMemorySnapshot {
                commit: windows_commit,
                pools: windows_pools,
                pressure: windows_memory_pressure,
            },
            disk_volume_correlation,
        }),
    })
}
