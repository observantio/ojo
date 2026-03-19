fn collect_system(kstats: &KstatMap, process_count: Option<u64>) -> Result<SystemSnapshot> {
    let ticks_per_second = read_ticks_per_second();
    let boot_time = kstat_u64(kstats, "unix:0:system_misc:boot_time");

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time before unix epoch")?
        .as_secs_f64();

    let uptime_secs = if boot_time > 0 {
        (now - boot_time as f64).max(0.0)
    } else {
        0.0
    };

    let mut per_cpu_map: BTreeMap<usize, CpuTimes> = BTreeMap::new();
    let mut context_switches = 0u64;
    let mut interrupts_total = 0u64;

    for (key, value) in kstats {
        let Some((module, instance, name, stat)) = split_kstat_key(key) else {
            continue;
        };

        if module == "cpu" && name == "sys" {
            let cpu_id = instance.parse::<usize>().unwrap_or(0);
            let cpu = per_cpu_map.entry(cpu_id).or_insert_with(empty_cpu_times);
            let parsed = value.parse::<u64>().unwrap_or(0);

            match stat {
                "cpu_ticks_user" => cpu.user = parsed,
                "cpu_ticks_kernel" => cpu.system = parsed,
                "cpu_ticks_idle" => cpu.idle = parsed,
                "cpu_ticks_wait" => cpu.iowait = parsed,
                _ => {}
            }
        }

        if module == "cpu_stat" && name == "sys" {
            let parsed = value.parse::<u64>().unwrap_or(0);
            match stat {
                "pswitch" | "inv_swtch" => {
                    context_switches = context_switches.saturating_add(parsed);
                }
                "intr" => {
                    interrupts_total = interrupts_total.saturating_add(parsed);
                }
                _ => {}
            }
        }
    }

    let per_cpu = per_cpu_map.into_values().collect::<Vec<_>>();
    let cpu_total = per_cpu.iter().fold(empty_cpu_times(), |mut acc, cpu| {
        acc.user = acc.user.saturating_add(cpu.user);
        acc.nice = acc.nice.saturating_add(cpu.nice);
        acc.system = acc.system.saturating_add(cpu.system);
        acc.idle = acc.idle.saturating_add(cpu.idle);
        acc.iowait = acc.iowait.saturating_add(cpu.iowait);
        acc.irq = acc.irq.saturating_add(cpu.irq);
        acc.softirq = acc.softirq.saturating_add(cpu.softirq);
        acc.steal = acc.steal.saturating_add(cpu.steal);
        acc.guest = acc.guest.saturating_add(cpu.guest);
        acc.guest_nice = acc.guest_nice.saturating_add(cpu.guest_nice);
        acc
    });

    let cpu_total_seconds = cpu_times_to_seconds(&cpu_total, ticks_per_second);
    let per_cpu_seconds = per_cpu
        .iter()
        .map(|cpu| cpu_times_to_seconds(cpu, ticks_per_second))
        .collect::<Vec<_>>();

    let process_count = process_count
        .or_else(|| {
            kstat_str(kstats, "unix:0:system_misc:nproc").and_then(|v| v.parse::<u64>().ok())
        })
        .unwrap_or_else(|| count_numeric_dirs("/proc"));

    Ok(SystemSnapshot {
        is_windows: false,
        ticks_per_second,
        cpu_cycle_utilization: None,
        boot_time_epoch_secs: boot_time,
        uptime_secs,
        context_switches,
        forks_since_boot: None,
        interrupts_total,
        softirqs_total: 0,
        process_count,
        pid_max: None,
        entropy_available_bits: None,
        entropy_pool_size_bits: None,
        procs_running: 0,
        procs_blocked: 0,
        cpu_total,
        cpu_total_seconds,
        per_cpu,
        per_cpu_seconds,
    })
}

fn collect_memory(
    kstats: &KstatMap,
    swaps: &[SwapDeviceSnapshot],
    page_size: u64,
) -> Result<MemorySnapshot> {
    let physmem_pages = kstat_u64(kstats, "unix:0:system_pages:physmem");
    let freemem_pages = kstat_u64(kstats, "unix:0:system_pages:freemem");
    let availrmem_pages = kstat_u64(kstats, "unix:0:system_pages:availrmem");

    let swap_total_bytes = swaps.iter().map(|s| s.size_bytes).sum::<u64>();
    let swap_free_bytes = swaps
        .iter()
        .map(|s| s.size_bytes.saturating_sub(s.used_bytes))
        .sum::<u64>();

    Ok(MemorySnapshot {
        mem_total_bytes: physmem_pages.saturating_mul(page_size),
        mem_free_bytes: freemem_pages.saturating_mul(page_size),
        mem_available_bytes: availrmem_pages.saturating_mul(page_size),
        buffers_bytes: None,
        cached_bytes: 0,
        active_bytes: None,
        inactive_bytes: None,
        anon_pages_bytes: None,
        mapped_bytes: None,
        shmem_bytes: None,
        swap_total_bytes,
        swap_free_bytes,
        swap_cached_bytes: None,
        dirty_bytes: None,
        writeback_bytes: None,
        slab_bytes: None,
        sreclaimable_bytes: None,
        sunreclaim_bytes: None,
        page_tables_bytes: None,
        committed_as_bytes: 0,
        commit_limit_bytes: 0,
        kernel_stack_bytes: None,
        hugepages_total: None,
        hugepages_free: None,
        hugepage_size_bytes: None,
        anon_hugepages_bytes: None,
    })
}

fn collect_load(kstats: &KstatMap) -> Result<LoadSnapshot> {
    let one = kstat_u64(kstats, "unix:0:system_misc:avenrun_1min") as f64 / 256.0;
    let five = kstat_u64(kstats, "unix:0:system_misc:avenrun_5min") as f64 / 256.0;
    let fifteen = kstat_u64(kstats, "unix:0:system_misc:avenrun_15min") as f64 / 256.0;

    Ok(LoadSnapshot {
        one,
        five,
        fifteen,
        runnable: 0,
        entities: 0,
        latest_pid: 0,
    })
}

fn collect_vmstat(kstats: &KstatMap) -> BTreeMap<String, i64> {
    let mut out = BTreeMap::new();

    for (key, value) in kstats {
        if !(key.contains(":vminfo:") || key.contains(":system_pages:")) {
            continue;
        }

        let Some((module, instance, name, stat)) = split_kstat_key(key) else {
            continue;
        };

        let parsed = value.parse::<i64>().ok().or_else(|| {
            value
                .parse::<u64>()
                .ok()
                .map(|v| v.min(i64::MAX as u64) as i64)
        });

        if let Some(parsed) = parsed {
            out.insert(format!("{module}.{instance}.{name}.{stat}"), parsed);
        }
    }

    out
}

