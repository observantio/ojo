pub fn collect_snapshot(include_process_metrics: bool) -> Result<Snapshot> {
    let mut cache = ReadCache::default();

    let (processes, process_count_hint, process_meta, process_ok, process_err) =
        if include_process_metrics {
            match collect_processes(&mut cache) {
                Ok((processes, meta)) => {
                    let count = processes.len() as u64;
                    (processes, Some(count), Some(meta), true, None)
                }
                Err(e) => (
                    Vec::new(),
                    None,
                    Some(ProcessCollectionMeta {
                        fd_scan_enabled: false,
                    }),
                    false,
                    Some(e.to_string()),
                ),
            }
        } else {
            (Vec::new(), None, None, true, None)
        };

    let mounts = collect_mounts().unwrap_or_default();
    let filesystem = collect_filesystem_stats(&mounts);
    let (system, system_ok, system_err) = match collect_system(&mut cache, process_count_hint) {
        Ok(v) => (v, true, None),
        Err(e) => (SystemSnapshot::default(), false, Some(e.to_string())),
    };
    let load = collect_load().ok();
    let runnable = load.as_ref().map(|v| v.runnable as f64).unwrap_or(0.0);
    let (schedstat, runqueue_depth) =
        collect_schedstat_and_runqueue(system.per_cpu.len(), runnable)
            .unwrap_or_else(|_| (BTreeMap::new(), BTreeMap::new()));

    let pressure = collect_pressure().unwrap_or_default();
    let pressure_totals_us = collect_pressure_totals().unwrap_or_default();
    let cgroup = collect_cgroup().unwrap_or_else(|_| (BTreeMap::new(), CgroupMode::None));
    let cgroup_metrics = cgroup.0;
    let cgroup_mode = cgroup.1;
    let (memory, memory_ok, memory_err) = match collect_memory() {
        Ok(v) => (v, true, None),
        Err(e) => (MemorySnapshot::default(), false, Some(e.to_string())),
    };

    let psi_supported = !pressure.is_empty() || !pressure_totals_us.is_empty();
    let psi_irq_supported = pressure.keys().any(|k| k.starts_with("irq."))
        || pressure_totals_us.keys().any(|k| k.starts_with("irq."));

    let mut support_state = linux_support_state(LinuxSupportInputs {
        cgroup_mode,
        psi_supported,
        psi_irq_supported,
        schedstat_supported: !schedstat.is_empty(),
        process_meta: process_meta.as_ref(),
        in_container: is_likely_containerized(),
        system_ok,
        memory_ok,
        process_ok,
    });

    if let Some(err) = system_err {
        support_state.insert("snapshot.core.system.error".to_string(), err);
    }
    if let Some(err) = memory_err {
        support_state.insert("snapshot.core.memory.error".to_string(), err);
    }
    if let Some(err) = process_err {
        support_state.insert("snapshot.processes.error".to_string(), err);
    }

    Ok(Snapshot {
        system,
        memory,
        load,
        pressure,
        pressure_totals_us,
        vmstat: procfs::vmstat().unwrap_or_default().into_iter().collect(),
        interrupts: collect_interrupts().unwrap_or_default(),
        softirqs: collect_softirqs().unwrap_or_default(),
        net_snmp: collect_net_snmp().unwrap_or_default(),
        net_stat: collect_netstat().unwrap_or_default(),
        sockets: collect_sockets().unwrap_or_default(),
        schedstat,
        runqueue_depth,
        slabinfo: collect_slabinfo().unwrap_or_default(),
        filesystem,
        cgroup: cgroup_metrics,
        softnet: collect_softnet().unwrap_or_default(),
        swaps: collect_swaps().unwrap_or_default(),
        mounts,
        cpuinfo: collect_cpuinfo(&mut cache).unwrap_or_default(),
        zoneinfo: collect_zoneinfo().unwrap_or_default(),
        buddyinfo: collect_buddyinfo().unwrap_or_default(),
        disks: collect_disks(&mut cache).unwrap_or_default(),
        net: collect_net(&mut cache).unwrap_or_default(),
        processes,
        support_state,
        metric_classification: linux_metric_classification(),
        windows: None,
    })
}
