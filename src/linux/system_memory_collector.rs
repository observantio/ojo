fn cpu_times_from_stat(cpu: &procfs::CpuTime) -> CpuTimes {
    CpuTimes {
        user: cpu.user,
        nice: cpu.nice,
        system: cpu.system,
        idle: cpu.idle,
        iowait: cpu.iowait.unwrap_or(0),
        irq: cpu.irq.unwrap_or(0),
        softirq: cpu.softirq.unwrap_or(0),
        steal: cpu.steal.unwrap_or(0),
        guest: cpu.guest.unwrap_or(0),
        guest_nice: cpu.guest_nice.unwrap_or(0),
    }
}

fn cpu_times_seconds_from_stat(cpu: &procfs::CpuTime, hz: f64) -> CpuTimesSeconds {
    CpuTimesSeconds {
        user: cpu.user as f64 / hz,
        nice: cpu.nice as f64 / hz,
        system: cpu.system as f64 / hz,
        idle: cpu.idle as f64 / hz,
        iowait: cpu.iowait.unwrap_or(0) as f64 / hz,
        irq: cpu.irq.unwrap_or(0) as f64 / hz,
        softirq: cpu.softirq.unwrap_or(0) as f64 / hz,
        steal: cpu.steal.unwrap_or(0) as f64 / hz,
        guest: cpu.guest.unwrap_or(0) as f64 / hz,
        guest_nice: cpu.guest_nice.unwrap_or(0) as f64 / hz,
    }
}

fn collect_system(cache: &mut ReadCache, process_count: Option<u64>) -> Result<SystemSnapshot> {
    let stat = procfs::KernelStats::current()?;
    let (interrupts_total, softirqs_total) = collect_proc_stat_totals()?;
    let uptime_secs = collect_uptime_secs()?;
    let hz = procfs::ticks_per_second().max(1) as f64;

    let per_cpu = stat.cpu_time.iter().map(cpu_times_from_stat).collect();
    let cpu_total = cpu_times_from_stat(&stat.total);
    let cpu_total_seconds = cpu_times_seconds_from_stat(&stat.total, hz);
    let per_cpu_seconds = stat
        .cpu_time
        .iter()
        .map(|cpu| cpu_times_seconds_from_stat(cpu, hz))
        .collect();

    Ok(SystemSnapshot {
        is_windows: false,
        os_type: std::env::consts::OS.to_string(),
        ticks_per_second: procfs::ticks_per_second(),
        cpu_cycle_utilization: None,
        boot_time_epoch_secs: stat.btime,
        uptime_secs,
        context_switches: stat.ctxt,
        forks_since_boot: Some(stat.processes),
        interrupts_total,
        softirqs_total,
        process_count: process_count
            .unwrap_or_else(|| all_processes().map(|p| p.count() as u64).unwrap_or(0)),
        pid_max: read_proc_u64(cache, "/proc/sys/kernel/pid_max"),
        entropy_available_bits: read_proc_u64(cache, "/proc/sys/kernel/random/entropy_avail"),
        entropy_pool_size_bits: read_proc_u64(cache, "/proc/sys/kernel/random/poolsize"),
        procs_running: stat.procs_running.unwrap_or(0),
        procs_blocked: stat.procs_blocked.unwrap_or(0),
        cpu_total,
        cpu_total_seconds,
        per_cpu,
        per_cpu_seconds,
    })
}

fn collect_memory() -> Result<MemorySnapshot> {
    let mem = procfs::Meminfo::current()?;
    Ok(MemorySnapshot {
        mem_total_bytes: mem.mem_total,
        mem_free_bytes: mem.mem_free,
        mem_available_bytes: mem.mem_available.unwrap_or(0),
        buffers_bytes: Some(mem.buffers),
        cached_bytes: mem.cached,
        active_bytes: Some(mem.active),
        inactive_bytes: Some(mem.inactive),
        anon_pages_bytes: Some(mem.anon_pages.unwrap_or(0)),
        mapped_bytes: Some(mem.mapped),
        shmem_bytes: Some(mem.shmem.unwrap_or(0)),
        swap_total_bytes: mem.swap_total,
        swap_free_bytes: mem.swap_free,
        swap_cached_bytes: Some(mem.swap_cached),
        dirty_bytes: Some(mem.dirty),
        writeback_bytes: Some(mem.writeback),
        slab_bytes: Some(mem.slab),
        sreclaimable_bytes: Some(mem.s_reclaimable.unwrap_or(0)),
        sunreclaim_bytes: Some(mem.s_unreclaim.unwrap_or(0)),
        page_tables_bytes: Some(mem.page_tables.unwrap_or(0)),
        committed_as_bytes: mem.committed_as,
        commit_limit_bytes: mem.commit_limit.unwrap_or(0),
        kernel_stack_bytes: Some(mem.kernel_stack.unwrap_or(0)),
        hugepages_total: Some(mem.hugepages_total.unwrap_or(0)),
        hugepages_free: Some(mem.hugepages_free.unwrap_or(0)),
        hugepage_size_bytes: Some(mem.hugepagesize.unwrap_or(0)),
        anon_hugepages_bytes: Some(mem.anon_hugepages.unwrap_or(0)),
    })
}

fn collect_load() -> Result<LoadSnapshot> {
    let load = procfs::LoadAverage::current()?;
    Ok(LoadSnapshot {
        one: load.one as f64,
        five: load.five as f64,
        fifteen: load.fifteen as f64,
        runnable: load.cur,
        entities: load.max,
        latest_pid: load.latest_pid,
    })
}
