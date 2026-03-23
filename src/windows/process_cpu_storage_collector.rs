fn query_seek_penalty(device_path: &str) -> Option<bool> {
    unsafe {
        let handle = open_storage_query_handle(device_path)?;
        let mut query = STORAGE_PROPERTY_QUERY {
            PropertyId: STORAGE_PROPERTY_ID(STORAGE_PROPERTY_ID_SEEK_PENALTY as i32),
            QueryType: STORAGE_QUERY_TYPE(0),
            AdditionalParameters: [0],
        };
        let mut desc = DeviceSeekPenaltyDescriptor::default();
        let mut returned = 0u32;
        let ok = DeviceIoControl(
            handle.as_raw(),
            IOCTL_STORAGE_QUERY_PROPERTY,
            Some(&mut query as *mut _ as *mut c_void),
            size_of::<STORAGE_PROPERTY_QUERY>() as u32,
            Some(&mut desc as *mut _ as *mut c_void),
            size_of::<DeviceSeekPenaltyDescriptor>() as u32,
            Some(&mut returned),
            None,
        )
        .is_ok();
        if ok && returned >= size_of::<DeviceSeekPenaltyDescriptor>() as u32 {
            Some(desc.incurs_seek_penalty != 0)
        } else {
            None
        }
    }
}

fn query_storage_alignment(device_path: &str) -> (Option<u64>, Option<u64>, Option<bool>) {
    let rotational = query_seek_penalty(device_path);
    unsafe {
        let handle = match open_storage_query_handle(device_path) {
            Some(h) => h,
            None => return (Some(512), Some(512), rotational),
        };
        let mut query = STORAGE_PROPERTY_QUERY {
            PropertyId: STORAGE_PROPERTY_ID(6),
            QueryType: STORAGE_QUERY_TYPE(0),
            AdditionalParameters: [0],
        };
        let mut out = vec![0u8; 1024];
        let mut returned = 0u32;
        let ok = DeviceIoControl(
            handle.as_raw(),
            IOCTL_STORAGE_QUERY_PROPERTY,
            Some(&mut query as *mut _ as *mut c_void),
            size_of::<STORAGE_PROPERTY_QUERY>() as u32,
            Some(out.as_mut_ptr() as *mut c_void),
            out.len() as u32,
            Some(&mut returned),
            None,
        )
        .is_ok();
        if ok && returned >= size_of::<STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR>() as u32 {
            let desc = &*(out.as_ptr() as *const STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR);
            let l = if desc.BytesPerLogicalSector > 0 {
                desc.BytesPerLogicalSector as u64
            } else {
                512
            };
            let p = if desc.BytesPerPhysicalSector > 0 {
                desc.BytesPerPhysicalSector as u64
            } else {
                l
            };
            (Some(l), Some(p), rotational)
        } else {
            (Some(512), Some(512), rotational)
        }
    }
}

fn open_storage_query_handle(path: &str) -> Option<OwnedHandle> {
    let path_w = wide_z(path);
    unsafe {
        let attempts = [
            (0u32, FILE_ATTRIBUTE_NORMAL),
            (FILE_GENERIC_READ.0, FILE_ATTRIBUTE_NORMAL),
            (FILE_GENERIC_READ.0, FILE_FLAG_NO_BUFFERING),
        ];
        for (desired_access, flags) in attempts {
            if let Ok(handle) = CreateFileW(
                PCWSTR(path_w.as_ptr()),
                desired_access,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                None,
                OPEN_EXISTING,
                flags,
                None,
            ) {
                return Some(OwnedHandle(handle));
            }
        }
    }
    None
}

fn query_disk_performance_for_path(
    path: &str,
    _state: &mut WinCollectState,
) -> Option<DiskPerfData> {
    unsafe {
        let handle = match open_storage_query_handle(path) {
            Some(h) => h,
            None => {
                warn!(
                    path = %path,
                    win32_error = GetLastError().0,
                    "wincollect: failed to open disk for performance counters"
                );
                return None;
            }
        };

        let try_ioctl_perf = |h: HANDLE| -> Option<DiskPerformance> {
            let mut perf = DiskPerformance::default();
            let mut returned = 0u32;
            let ok = DeviceIoControl(
                h,
                IOCTL_DISK_PERFORMANCE,
                None,
                0,
                Some(&mut perf as *mut _ as *mut c_void),
                size_of::<DiskPerformance>() as u32,
                Some(&mut returned),
                None,
            )
            .is_ok();
            if ok && returned >= size_of::<DiskPerformance>() as u32 {
                Some(perf)
            } else {
                warn!(
                    path = %path,
                    win32_error = GetLastError().0,
                    "wincollect: IOCTL_DISK_PERFORMANCE failed"
                );
                None
            }
        };

        let raw = try_ioctl_perf(handle.as_raw())?;

        let boot_100ns = boot_time_filetime_100ns();
        let query_100ns = nt_time_100ns(raw.query_time.quad_part);
        let idle_100ns = nt_time_100ns(raw.idle_time.quad_part);
        let time_in_progress_ms = query_100ns
            .saturating_sub(boot_100ns)
            .saturating_sub(idle_100ns)
            / 10_000;
        let bytes_read = nt_time_100ns(raw.bytes_read.quad_part);
        let bytes_written = nt_time_100ns(raw.bytes_written.quad_part);
        let read_time_ms = nt_time_100ns(raw.read_time.quad_part) / 10_000;
        let write_time_ms = nt_time_100ns(raw.write_time.quad_part) / 10_000;

        Some(DiskPerfData {
            reads: raw.read_count as u64,
            writes: raw.write_count as u64,
            bytes_read,
            bytes_written,
            time_reading_ms: read_time_ms,
            time_writing_ms: write_time_ms,
            queue_depth: raw.queue_depth as u64,
            time_in_progress_ms,
            weighted_time_in_progress_ms: read_time_ms.saturating_add(write_time_ms),
        })
    }
}

fn per_cpu_times_from_nt() -> Option<(Vec<CpuTimes>, u64)> {
    let entry_size = size_of::<SystemProcessorPerformanceInformation>();
    let ncpu = cpu_count_from_nt().max(1);
    let buf_size = (ncpu * entry_size * 2) as u32;
    let mut buf = vec![0u8; buf_size as usize];
    let mut ret_len = 0u32;
    let status = unsafe {
        NtQuerySystemInformation(
            SYSTEM_PROCESSOR_PERFORMANCE_INFORMATION_CLASS,
            buf.as_mut_ptr() as *mut c_void,
            buf_size,
            &mut ret_len,
        )
    };
    if !nt_success(status) || ret_len < entry_size as u32 {
        return None;
    }
    let count = (ret_len as usize) / entry_size;
    let mut out = Vec::with_capacity(count);
    let mut interrupts_total: u64 = 0;
    for i in 0..count {
        let entry = match read_unaligned_struct::<SystemProcessorPerformanceInformation>(
            &buf,
            i * entry_size,
        ) {
            Some(e) => e,
            None => break,
        };
        let idle = nt_time_100ns(entry.idle_time.quad_part);
        let kernel_total = nt_time_100ns(entry.kernel_time.quad_part);
        let user = nt_time_100ns(entry.user_time.quad_part);
        let dpc = nt_time_100ns(entry.dpc_time.quad_part);
        let irq = nt_time_100ns(entry.interrupt_time.quad_part);
        let system = kernel_total
            .saturating_sub(idle)
            .saturating_sub(dpc)
            .saturating_sub(irq);
        interrupts_total = interrupts_total.saturating_add(entry.interrupt_count as u64);
        out.push(CpuTimes {
            user,
            nice: 0,
            system,
            idle,
            iowait: 0,
            irq,
            softirq: dpc,
            steal: 0,
            guest: 0,
            guest_nice: 0,
        });
    }
    Some((out, interrupts_total))
}

fn cpu_times_aggregate(per_cpu: &[CpuTimes]) -> CpuTimes {
    let mut total = CpuTimes::default();
    for c in per_cpu {
        total.user = total.user.saturating_add(c.user);
        total.system = total.system.saturating_add(c.system);
        total.idle = total.idle.saturating_add(c.idle);
        total.irq = total.irq.saturating_add(c.irq);
        total.softirq = total.softirq.saturating_add(c.softirq);
    }
    total
}

fn cpu_times_seconds(times: &CpuTimes, hz: u64) -> CpuTimesSeconds {
    let hz = hz.max(1) as f64;
    CpuTimesSeconds {
        user: times.user as f64 / hz,
        nice: times.nice as f64 / hz,
        system: times.system as f64 / hz,
        idle: times.idle as f64 / hz,
        iowait: times.iowait as f64 / hz,
        irq: times.irq as f64 / hz,
        softirq: times.softirq as f64 / hz,
        steal: times.steal as f64 / hz,
        guest: times.guest as f64 / hz,
        guest_nice: times.guest_nice as f64 / hz,
    }
}

fn cpu_times_from_system_fallback() -> Result<(CpuTimes, Vec<CpuTimes>, u64)> {
    let mut idle = FILETIME::default();
    let mut kernel = FILETIME::default();
    let mut user = FILETIME::default();
    unsafe {
        GetSystemTimes(
            Some(&mut idle as *mut FILETIME),
            Some(&mut kernel as *mut FILETIME),
            Some(&mut user as *mut FILETIME),
        )
        .ok()
        .context("GetSystemTimes failed")?;
    }
    let idle_100ns = filetime_to_u64(idle);
    let kernel_total = filetime_to_u64(kernel);
    let user_100ns = filetime_to_u64(user);
    let system_100ns = kernel_total.saturating_sub(idle_100ns);
    let total = CpuTimes {
        user: user_100ns,
        nice: 0,
        system: system_100ns,
        idle: idle_100ns,
        iowait: 0,
        irq: 0,
        softirq: 0,
        steal: 0,
        guest: 0,
        guest_nice: 0,
    };
    let cores = cpu_count_from_nt().max(1) as u64;
    let per_cpu = (0..cores)
        .map(|_| CpuTimes {
            user: user_100ns / cores,
            nice: 0,
            system: system_100ns / cores,
            idle: idle_100ns / cores,
            iowait: 0,
            irq: 0,
            softirq: 0,
            steal: 0,
            guest: 0,
            guest_nice: 0,
        })
        .collect();
    Ok((total, per_cpu, 0))
}

fn cpu_times_from_nt() -> Result<(CpuTimes, Vec<CpuTimes>, u64)> {
    match per_cpu_times_from_nt() {
        Some((per_cpu, interrupts_total)) if !per_cpu.is_empty() => {
            let total = cpu_times_aggregate(&per_cpu);
            Ok((total, per_cpu, interrupts_total))
        }
        _ => {
            warn!("NtQuerySystemInformation per-CPU failed, falling back to GetSystemTimes");
            cpu_times_from_system_fallback()
        }
    }
}

fn compute_load_averages(
    cpu_total: &CpuTimes,
    entities: u32,
    state: &mut WinCollectState,
) -> (f64, f64, f64) {
    let current_busy = cpu_total.busy();
    let current_total = cpu_total.total();
    let now = Instant::now();

    let instant_load = |busy: u64, total: u64| -> f64 {
        if total == 0 {
            return 0.0;
        }
        (busy as f64 / total as f64 * entities as f64).clamp(0.0, entities as f64)
    };

    match state.load_avg {
        None => {
            let load = instant_load(current_busy, current_total);
            state.load_avg = Some(LoadAvgState {
                one: load,
                five: load,
                fifteen: load,
                prev_busy: current_busy,
                prev_total: current_total,
                last: now,
            });
            (load, load, load)
        }
        Some(prev) => {
            let delta_total = current_total.saturating_sub(prev.prev_total);
            let delta_busy = current_busy.saturating_sub(prev.prev_busy);
            let load = instant_load(delta_busy, delta_total);
            let dt = now.duration_since(prev.last).as_secs_f64().max(0.001);
            let a1 = (-dt / 60.0_f64).exp();
            let a5 = (-dt / 300.0_f64).exp();
            let a15 = (-dt / 900.0_f64).exp();
            let one = prev.one * a1 + load * (1.0 - a1);
            let five = prev.five * a5 + load * (1.0 - a5);
            let fifteen = prev.fifteen * a15 + load * (1.0 - a15);
            state.load_avg = Some(LoadAvgState {
                one,
                five,
                fifteen,
                prev_busy: current_busy,
                prev_total: current_total,
                last: now,
            });
            (one, five, fifteen)
        }
    }
}

pub fn collect_system(process_info_buffer: Option<&[u8]>) -> Result<SystemSnapshot> {
    debug!("wincollect: collect_system start");
    let uptime_secs = current_uptime_secs();
    let (cpu_total, per_cpu, interrupts_total) = cpu_times_from_nt()?;
    debug!("wincollect: cpu times done");
    let perf = system_performance_info();
    let boot_epoch = boot_time_epoch_secs();
    let owned_buf;
    let proc_buf = if let Some(buf) = process_info_buffer {
        buf
    } else {
        owned_buf = query_system_information(SYSTEM_PROCESS_INFORMATION_CLASS)?;
        owned_buf.as_slice()
    };
    let (process_count, procs_blocked, summaries) = extract_process_thread_summaries(&proc_buf);
    let procs_running = summaries.values().filter(|s| s.state == "R").count() as u32;
    let context_switches = perf
        .as_ref()
        .map(|p| p.context_switches as u64)
        .unwrap_or(0);
    let softirqs_total = per_cpu
        .iter()
        .map(|cpu| cpu.softirq)
        .fold(0u64, u64::saturating_add);
    let cpu_total_seconds = cpu_times_seconds(&cpu_total, 10_000_000);
    let per_cpu_seconds = per_cpu
        .iter()
        .map(|cpu| cpu_times_seconds(cpu, 10_000_000))
        .collect();
    debug!("wincollect: collect_system done");
    Ok(SystemSnapshot {
        is_windows: true,
        os_type: "windows".to_string(),
        ticks_per_second: 10_000_000,
        boot_time_epoch_secs: boot_epoch,
        uptime_secs,
        context_switches,
        forks_since_boot: None,
        interrupts_total,
        softirqs_total,
        process_count,
        pid_max: None,
        entropy_available_bits: None,
        entropy_pool_size_bits: None,
        procs_running,
        procs_blocked,
        cpu_total,
        cpu_total_seconds,
        per_cpu,
        per_cpu_seconds,
        cpu_cycle_utilization: None,
    })
}

fn collect_memory(perf: Option<&SystemPerformanceInformation>) -> Result<MemorySnapshot> {
    unsafe {
        let mut mem = MEMORYSTATUSEX::default();
        mem.dwLength = size_of::<MEMORYSTATUSEX>() as u32;
        GlobalMemoryStatusEx(&mut mem)
            .ok()
            .context("GlobalMemoryStatusEx failed")?;

        let page = page_size_from_nt();
        let total_phys = mem.ullTotalPhys;
        let avail_phys = mem.ullAvailPhys;
        let total_pagefile = mem.ullTotalPageFile;
        let avail_pagefile = mem.ullAvailPageFile;

        let (
            cached_bytes,
            commit_total,
            commit_limit,
            non_paged_pool,
            paged_pool,
            perf_avail_bytes,
        ) = match perf {
            Some(p) => (
                (p.resident_system_cache_page as u64).saturating_mul(page),
                (p.committed_pages as u64).saturating_mul(page),
                (p.commit_limit as u64).saturating_mul(page),
                (p.non_paged_pool_pages as u64).saturating_mul(page),
                (p.paged_pool_pages as u64).saturating_mul(page),
                (p.available_pages as u64).saturating_mul(page),
            ),
            None => {
                let mut perf = PERFORMANCE_INFORMATION::default();
                perf.cb = size_of::<PERFORMANCE_INFORMATION>() as u32;
                if GetPerformanceInfo(&mut perf, perf.cb).is_ok() {
                    (
                        (perf.SystemCache as u64).saturating_mul(page),
                        (perf.CommitTotal as u64).saturating_mul(page),
                        (perf.CommitLimit as u64).saturating_mul(page),
                        0,
                        0,
                        (perf.PhysicalAvailable as u64).saturating_mul(page),
                    )
                } else {
                    (0, 0, 0, 0, 0, avail_phys)
                }
            }
        };

        let used_phys = total_phys.saturating_sub(avail_phys);

        let mut swap_total = commit_limit.saturating_sub(total_phys);
        if swap_total == 0 {
            swap_total = total_pagefile.saturating_sub(total_phys);
        }

        let mut swap_used = commit_total.saturating_sub(total_phys);
        if swap_used == 0 && swap_total > 0 {
            let committed = total_pagefile.saturating_sub(avail_pagefile);
            let resident = total_phys.saturating_sub(avail_phys);
            swap_used = committed.saturating_sub(resident).min(swap_total);
        }
        let explicit_pagefile_used = total_pagefile
            .saturating_sub(avail_pagefile)
            .saturating_sub(total_phys.saturating_sub(avail_phys));
        swap_used = swap_used.max(explicit_pagefile_used).min(swap_total);
        let swap_avail = swap_total.saturating_sub(swap_used);
        let reclaimable_hint = cached_bytes.saturating_add(paged_pool);
        let mem_available = perf_avail_bytes
            .max(avail_phys)
            .saturating_add(reclaimable_hint / 8)
            .min(total_phys);

        Ok(MemorySnapshot {
            mem_total_bytes: total_phys,
            mem_free_bytes: avail_phys,
            mem_available_bytes: mem_available,
            buffers_bytes: None,
            cached_bytes,
            active_bytes: Some(used_phys.saturating_sub(cached_bytes)),
            inactive_bytes: None,
            anon_pages_bytes: None,
            mapped_bytes: None,
            shmem_bytes: None,
            swap_total_bytes: swap_total,
            swap_free_bytes: swap_avail,
            swap_cached_bytes: None,
            dirty_bytes: None,
            writeback_bytes: None,
            slab_bytes: Some(non_paged_pool.saturating_add(paged_pool)),
            sreclaimable_bytes: Some(paged_pool),
            sunreclaim_bytes: Some(non_paged_pool),
            page_tables_bytes: None,
            committed_as_bytes: commit_total,
            commit_limit_bytes: commit_limit,
            kernel_stack_bytes: None,
            hugepages_total: None,
            hugepages_free: None,
            hugepage_size_bytes: None,
            anon_hugepages_bytes: None,
        })
    }
}

fn collect_synthetic_load(
    cpu_total: &CpuTimes,
    runnable_threads: u32,
    state: &mut WinCollectState,
) -> Result<WindowsSyntheticLoadSnapshot> {
    if !state.warned_synth_load {
        warn!(
            "wincollect: windows.load.synthetic is derived from CPU busy-time EMA scaled by logical CPU entities; it is not Linux loadavg-equivalent."
        );
        state.warned_synth_load = true;
    }

    let entities = cpu_count_from_nt().max(1) as u32;
    let (one, five, fifteen) = compute_load_averages(cpu_total, entities, state);
    Ok(WindowsSyntheticLoadSnapshot {
        one,
        five,
        fifteen,
        entities,
        runnable_threads,
        source: "ema(cpu_busy_ratio * logical_cpu_entities)".to_string(),
    })
}

fn drive_strings() -> Result<Vec<String>> {
    unsafe {
        let mut buf = vec![0u16; 1024];
        let len = GetLogicalDriveStringsW(Some(&mut buf)) as usize;
        if len == 0 {
            return Err(anyhow!(
                "GetLogicalDriveStringsW failed: {}",
                GetLastError().0
            ));
        }
        if len > buf.len() {
            buf.resize(len + 2, 0);
            if GetLogicalDriveStringsW(Some(&mut buf)) == 0 {
                return Err(anyhow!("GetLogicalDriveStringsW retry failed"));
            }
        }
        let mut out = Vec::new();
        let mut start = 0usize;
        for i in 0..buf.len() {
            if buf[i] == 0 {
                if i > start {
                    out.push(String::from_utf16_lossy(&buf[start..i]));
                } else {
                    break;
                }
                start = i + 1;
            }
        }
        Ok(out)
    }
}

fn collect_disks(state: &mut WinCollectState) -> Result<Vec<DiskSnapshot>> {
    let mut out = Vec::new();
    for idx in 0u32..64 {
        let device_path = format!(r"\\.\PhysicalDrive{idx}");
        if open_storage_query_handle(&device_path).is_none() {
            continue;
        }

        let static_meta = if let Some(meta) = state.disk_static.get(&device_path).copied() {
            meta
        } else {
            let (logical, physical, rotational) = query_storage_alignment(&device_path);
            let meta = DiskStaticMeta {
                logical_block_size: logical,
                physical_block_size: physical,
                rotational,
            };
            state.disk_static.insert(device_path.clone(), meta);
            meta
        };

        let perf = query_disk_performance_for_path(&device_path, state);
        if perf.is_none() && !state.disk_perf_unavailable {
            warn!(
                "wincollect: disk throughput/IOPS counters unavailable (IOCTL_DISK_PERFORMANCE failed). common causes are missing privileges or disabled Windows disk performance counters; try running elevated and `diskperf -y`."
            );
            state.disk_perf_unavailable = true;
        }
        let name = format!("PhysicalDrive{idx}");
        out.push(DiskSnapshot {
            name,
            has_counters: perf.is_some(),
            reads: perf.as_ref().map(|v| v.reads).unwrap_or(0),
            writes: perf.as_ref().map(|v| v.writes).unwrap_or(0),
            sectors_read: perf
                .as_ref()
                .map(|v| v.bytes_read / SECTOR_SIZE.max(1))
                .unwrap_or(0),
            sectors_written: perf
                .as_ref()
                .map(|v| v.bytes_written / SECTOR_SIZE.max(1))
                .unwrap_or(0),
            time_reading_ms: perf.as_ref().map(|v| v.time_reading_ms).unwrap_or(0),
            time_writing_ms: perf.as_ref().map(|v| v.time_writing_ms).unwrap_or(0),
            in_progress: perf.as_ref().map(|v| v.queue_depth).unwrap_or(0),
            time_in_progress_ms: perf.as_ref().map(|v| v.time_in_progress_ms).unwrap_or(0),
            weighted_time_in_progress_ms: perf
                .as_ref()
                .map(|v| v.weighted_time_in_progress_ms)
                .unwrap_or(0),
            logical_block_size: static_meta.logical_block_size,
            physical_block_size: static_meta.physical_block_size,
            rotational: static_meta.rotational,
        });
    }
    Ok(out)
}

fn wchar_array_to_string<const N: usize>(arr: &[u16; N]) -> String {
    let len = arr.iter().position(|c| *c == 0).unwrap_or(N);
    String::from_utf16_lossy(&arr[..len])
}

fn guid_to_string(g: windows::core::GUID) -> String {
    format!(
        "{{{:08x}-{:04x}-{:04x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}}}",
        g.data1,
        g.data2,
        g.data3,
        g.data4[0],
        g.data4[1],
        g.data4[2],
        g.data4[3],
        g.data4[4],
        g.data4[5],
        g.data4[6],
        g.data4[7]
    )
}

