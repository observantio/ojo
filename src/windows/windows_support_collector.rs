fn collect_disk_volume_correlation(mounts: &[MountSnapshot]) -> Vec<DiskVolumeCorrelation> {
    let mut out = Vec::new();
    for mount in mounts {
        if !mount.mountpoint.contains(':') {
            continue;
        }
        out.push(DiskVolumeCorrelation {
            mountpoint: mount.mountpoint.clone(),
            volume_guid: query_volume_guid_for_mount(&mount.mountpoint),
            nt_device_path: query_nt_device_for_mount(&mount.mountpoint),
            physical_drive: query_physical_drive_for_mount(&mount.mountpoint),
        });
    }
    out
}

fn compute_windows_paging_rates(
    hard_fault_total: u64,
    page_reads_total: u64,
    page_writes_total: u64,
    state: &mut WinCollectState,
) -> (Option<f64>, Option<f64>, Option<f64>, Option<f64>) {
    let now = Instant::now();

    let result = if let Some(prev) = state.paging_rate {
        let dt = now.duration_since(prev.last).as_secs_f64();
        if dt > 0.0 {
            (
                Some(hard_fault_total.saturating_sub(prev.hard_fault_total) as f64 / dt),
                Some(page_reads_total.saturating_sub(prev.page_reads_total) as f64 / dt),
                Some(page_writes_total.saturating_sub(prev.page_writes_total) as f64 / dt),
                Some(dt),
            )
        } else {
            (None, None, None, None)
        }
    } else {
        (None, None, None, None)
    };

    state.paging_rate = Some(WindowsPagingRateState {
        hard_fault_total,
        page_reads_total,
        page_writes_total,
        last: now,
    });
    result
}

fn collect_windows_pagefiles(memory: &MemorySnapshot) -> Vec<WindowsPagefileSnapshot> {
    if memory.swap_total_bytes == 0 {
        return Vec::new();
    }
    let total = memory.swap_total_bytes;
    let free = memory.swap_free_bytes.min(total);
    let used = total.saturating_sub(free);
    vec![WindowsPagefileSnapshot {
        name: "system_pagefile".to_string(),
        total_bytes: total,
        used_bytes: used,
        free_bytes: free,
    }]
}

fn collect_windows_memory_pressure(
    memory: &MemorySnapshot,
    hard_fault_total: u64,
    page_reads_total: u64,
    page_writes_total: u64,
    state: &mut WinCollectState,
) -> WindowsMemoryPressureSnapshot {
    let commit_utilization_pct = if memory.commit_limit_bytes > 0 {
        (memory.committed_as_bytes as f64 * 100.0) / memory.commit_limit_bytes as f64
    } else {
        0.0
    };
    let available_memory_pct = if memory.mem_total_bytes > 0 {
        (memory.mem_available_bytes as f64 * 100.0) / memory.mem_total_bytes as f64
    } else {
        0.0
    };
    let pagefile_utilization_pct = if memory.swap_total_bytes > 0 {
        let used = memory
            .swap_total_bytes
            .saturating_sub(memory.swap_free_bytes);
        (used as f64 * 100.0) / memory.swap_total_bytes as f64
    } else {
        0.0
    };
    let (hard_fault_rate, page_reads_per_sec, page_writes_per_sec, sampled_interval_secs) =
        compute_windows_paging_rates(hard_fault_total, page_reads_total, page_writes_total, state);
    WindowsMemoryPressureSnapshot {
        commit_utilization_pct,
        available_memory_pct,
        pagefile_utilization_pct,
        hard_fault_rate,
        page_reads_per_sec,
        page_writes_per_sec,
        sampled_interval_secs,
    }
}

fn collect_windows_commit(memory: &MemorySnapshot) -> WindowsCommitSnapshot {
    let charge = memory.committed_as_bytes;
    let limit = memory.commit_limit_bytes;
    let available = limit.saturating_sub(charge);
    let reserve = available;
    let utilization_pct = if limit > 0 {
        (charge as f64 * 100.0) / limit as f64
    } else {
        0.0
    };
    WindowsCommitSnapshot {
        charge_bytes: charge,
        limit_bytes: limit,
        available_bytes: available,
        reserve_bytes: reserve,
        utilization_pct,
    }
}

fn collect_windows_memory_pools(
    perf: Option<&SystemPerformanceInformation>,
) -> WindowsMemoryPoolsSnapshot {
    let page = page_size_from_nt().max(1);
    let paged_pool_pages = perf.map(|p| p.paged_pool_pages as u64).unwrap_or(0);
    let nonpaged_pool_pages = perf.map(|p| p.non_paged_pool_pages as u64).unwrap_or(0);
    let system_cache_pages = perf
        .map(|p| p.resident_system_cache_page as u64)
        .unwrap_or(0);
    WindowsMemoryPoolsSnapshot {
        paged_pool_bytes: paged_pool_pages.saturating_mul(page),
        nonpaged_pool_bytes: nonpaged_pool_pages.saturating_mul(page),
        system_cache_bytes: system_cache_pages.saturating_mul(page),
    }
}

fn windows_support_state() -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    out.insert(
        "memory.inactive_bytes".to_string(),
        "unsupported".to_string(),
    );
    out.insert(
        "memory.anon_pages_bytes".to_string(),
        "unsupported".to_string(),
    );
    out.insert("memory.mapped_bytes".to_string(), "unsupported".to_string());
    out.insert("memory.shmem_bytes".to_string(), "unsupported".to_string());
    out.insert("memory.dirty_bytes".to_string(), "unsupported".to_string());
    out.insert(
        "memory.page_tables_bytes".to_string(),
        "unsupported".to_string(),
    );
    out.insert(
        "memory.kernel_stack_bytes".to_string(),
        "unsupported".to_string(),
    );
    out.insert(
        "system.forks_since_boot".to_string(),
        "unsupported".to_string(),
    );
    out.insert("system.pid_max".to_string(), "unsupported".to_string());
    out.insert(
        "system.entropy_available_bits".to_string(),
        "unsupported".to_string(),
    );
    out.insert(
        "system.entropy_pool_size_bits".to_string(),
        "unsupported".to_string(),
    );
    out.insert(
        "system.softirqs_total".to_string(),
        "windows_dpc_analogue_not_linux_softirq".to_string(),
    );
    out.insert(
        "process.vsize_bytes".to_string(),
        "compat_alias_use_process.virtual_size_bytes_on_windows".to_string(),
    );
    out.insert(
        "process.rss_pages".to_string(),
        "compat_alias_use_process.resident_bytes_on_windows".to_string(),
    );
    out.insert("section.pressure".to_string(), "unsupported".to_string());
    out.insert(
        "section.pressure_totals_us".to_string(),
        "unsupported".to_string(),
    );
    out.insert(
        "section.swaps".to_string(),
        "windows_pagefile_model".to_string(),
    );
    out.insert(
        "load.shared".to_string(),
        "unsupported_on_windows_use_windows.load.synthetic_not_linux_loadavg".to_string(),
    );
    out.insert(
        "process.unix.file_descriptor.count".to_string(),
        "mapped_to_windows_handle_count_not_posix_fd".to_string(),
    );
    out.insert(
        "process.linux.scheduler".to_string(),
        "mapped_to_windows_priority_class_not_linux_policy".to_string(),
    );
    out.insert(
        "snapshot.state.persistence".to_string(),
        "wincollect_state_persisted_across_snapshots".to_string(),
    );
    out.insert("section.softnet".to_string(), "unsupported".to_string());
    out.insert("section.zoneinfo".to_string(), "unsupported".to_string());
    out.insert("section.buddyinfo".to_string(), "unsupported".to_string());
    out.insert(
        "section.linux_softirqs".to_string(),
        "unsupported".to_string(),
    );
    out.insert(
        "section.linux_interrupts".to_string(),
        "unsupported".to_string(),
    );
    out
}

fn windows_metric_classification() -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    out.insert("system.cpu.time".to_string(), "derived".to_string());
    out.insert("system.cpu.utilization".to_string(), "derived".to_string());
    out.insert(
        "system.cpu.load_average.*".to_string(),
        "unsupported_on_windows".to_string(),
    );
    out.insert("system.disk.*".to_string(), "native".to_string());
    out.insert("system.network.*".to_string(), "native".to_string());
    out.insert("system.paging.*".to_string(), "derived".to_string());
    out.insert(
        "system.softirqs_total".to_string(),
        "native_windows_analogue".to_string(),
    );
    out.insert("system.windows.vmstat.*".to_string(), "native".to_string());
    out.insert(
        "system.memory.inactive".to_string(),
        "unsupported".to_string(),
    );
    out.insert("system.memory.anon".to_string(), "unsupported".to_string());
    out.insert(
        "system.memory.mapped".to_string(),
        "unsupported".to_string(),
    );
    out.insert("system.memory.shmem".to_string(), "unsupported".to_string());
    out.insert("system.socket.*".to_string(), "native".to_string());
    out.insert("system.filesystem.*".to_string(), "native".to_string());
    out.insert("system.swap.*".to_string(), "derived".to_string());
    out.insert("system.cpu.info".to_string(), "native".to_string());
    out.insert("windows.interrupts.*".to_string(), "native".to_string());
    out.insert("windows.dpc.*".to_string(), "native".to_string());
    out.insert("windows.vmstat.*".to_string(), "native".to_string());
    out.insert(
        "windows.load.synthetic.*".to_string(),
        "synthetic".to_string(),
    );
    out.insert("windows.pagefiles.*".to_string(), "native".to_string());
    out.insert("windows.memory.commit.*".to_string(), "native".to_string());
    out.insert("windows.memory.pools.*".to_string(), "native".to_string());
    out.insert(
        "windows.memory.pressure.*".to_string(),
        "derived".to_string(),
    );
    out.insert("windows.thread.*".to_string(), "native".to_string());
    out.insert("windows.numa.*".to_string(), "native".to_string());
    out.insert(
        "process.vsize_bytes".to_string(),
        "compatibility_alias".to_string(),
    );
    out.insert(
        "process.rss_pages".to_string(),
        "compatibility_alias".to_string(),
    );
    out.insert(
        "process.unix.file_descriptor.count".to_string(),
        "compatibility_alias_windows_handle_count".to_string(),
    );
    out.insert(
        "process.linux.scheduler".to_string(),
        "compatibility_alias_windows_priority_class".to_string(),
    );
    out.insert(
        "windows.load.synthetic.*".to_string(),
        "synthetic_not_linux_loadavg".to_string(),
    );
    out
}

pub fn collect_swaps(memory: &MemorySnapshot) -> Vec<SwapDeviceSnapshot> {
    if memory.swap_total_bytes == 0 {
        return Vec::new();
    }
    vec![SwapDeviceSnapshot {
        device: "system_pagefile".to_string(),
        swap_type: "windows_pagefile".to_string(),
        size_bytes: memory.swap_total_bytes,
        used_bytes: memory
            .swap_total_bytes
            .saturating_sub(memory.swap_free_bytes),
        priority: -1,
    }]
}

