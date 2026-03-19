pub fn collect_snapshot(include_process_metrics: bool) -> Result<Snapshot> {
    let kstats = get_kstats()?;
    let page_size = read_page_size();
    let ticks_per_second = read_ticks_per_second();

    let processes = if include_process_metrics {
        collect_processes(page_size, ticks_per_second)?
    } else {
        Vec::new()
    };

    let process_count_hint = if include_process_metrics {
        Some(processes.len() as u64)
    } else {
        None
    };

    let swaps = collect_swaps()?;

    Ok(Snapshot {
        system: collect_system(&kstats, process_count_hint)?,
        memory: collect_memory(&kstats, &swaps, page_size)?,
        load: Some(collect_load(&kstats)?),
        pressure: BTreeMap::new(),
        pressure_totals_us: BTreeMap::new(),
        vmstat: collect_vmstat(&kstats),
        interrupts: BTreeMap::new(),
        softirqs: BTreeMap::new(),
        net_snmp: BTreeMap::new(),
        net_stat: BTreeMap::new(),
        sockets: BTreeMap::new(),
        schedstat: BTreeMap::new(),
        runqueue_depth: BTreeMap::new(),
        slabinfo: BTreeMap::new(),
        filesystem: BTreeMap::new(),
        cgroup: BTreeMap::new(),
        softnet: Vec::<SoftnetCpuSnapshot>::new(),
        swaps,
        mounts: collect_mounts()?,
        cpuinfo: collect_cpuinfo(&kstats)?,
        zoneinfo: BTreeMap::new(),
        buddyinfo: BTreeMap::new(),
        disks: collect_disks(&kstats)?,
        net: collect_net(&kstats)?,
        processes,
        support_state: solaris_support_state(),
        metric_classification: solaris_metric_classification(),
        windows: None,
    })
}

