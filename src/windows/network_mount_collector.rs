pub fn collect_net() -> Result<Vec<NetDevSnapshot>> {
    unsafe {
        let mut table: *mut MIB_IF_TABLE2 = null_mut();
        GetIfTable2(&mut table).ok().context("GetIfTable2 failed")?;
        if table.is_null() {
            return Ok(Vec::new());
        }
        let num = (*table).NumEntries as usize;
        let rows = slice::from_raw_parts((*table).Table.as_ptr(), num);
        let mut out = Vec::with_capacity(num);
        for row in rows {
            let alias = wchar_array_to_string(&row.Alias);
            let desc = wchar_array_to_string(&row.Description);
            let name_lc = format!("{} {}", alias, desc).to_ascii_lowercase();
            let is_up = row.OperStatus.0 == 1;
            let has_traffic = row.InOctets > 0
                || row.OutOctets > 0
                || row.InUcastPkts > 0
                || row.OutUcastPkts > 0;
            let is_loopback = row.Type == MIB_IF_TYPE_LOOPBACK;
            if !is_up && !has_traffic {
                continue;
            }
            let is_virtual = is_loopback
                || name_lc.contains("hyper-v")
                || name_lc.contains("vswitch")
                || name_lc.contains("vethernet")
                || name_lc.contains("lightweight filter")
                || name_lc.contains("qos packet scheduler")
                || name_lc.contains("virtual")
                || name_lc.contains("loopback")
                || name_lc.contains("tunnel");
            let is_physical = !is_virtual;
            let name = if !alias.is_empty() { alias } else { desc };
            let speed_mbps = row.ReceiveLinkSpeed.max(row.TransmitLinkSpeed) / 1_000_000;
            let interface_guid = guid_to_string(row.InterfaceGuid);
            let interface_luid = row.InterfaceLuid.Value;
            out.push(NetDevSnapshot {
                name,
                stable_id: Some(format!("guid:{interface_guid}")),
                interface_index: Some(row.InterfaceIndex),
                interface_luid: Some(interface_luid),
                is_virtual: Some(is_virtual),
                is_loopback: Some(is_loopback),
                is_physical: Some(is_physical),
                is_primary: Some(false),
                mtu: Some(row.Mtu as u64),
                speed_mbps: if speed_mbps > 0 {
                    Some(speed_mbps)
                } else {
                    None
                },
                tx_queue_len: None,
                carrier_up: Some(row.OperStatus.0 == 1),
                rx_bytes: row.InOctets,
                rx_packets: row.InUcastPkts.saturating_add(row.InNUcastPkts),
                rx_errs: row.InErrors,
                rx_drop: row.InDiscards,
                rx_fifo: 0,
                rx_frame: 0,
                rx_compressed: 0,
                rx_multicast: row.InNUcastPkts,
                tx_bytes: row.OutOctets,
                tx_packets: row.OutUcastPkts.saturating_add(row.OutNUcastPkts),
                tx_errs: row.OutErrors,
                tx_drop: row.OutDiscards,
                tx_fifo: 0,
                tx_colls: 0,
                tx_carrier: 0,
                tx_compressed: 0,
            });
        }

        let primary_idx = out
            .iter()
            .enumerate()
            .filter(|(_, n)| n.is_loopback != Some(true))
            .max_by_key(|(_, n)| n.rx_bytes.saturating_add(n.tx_bytes))
            .map(|(idx, _)| idx);
        if let Some(idx) = primary_idx {
            if let Some(primary) = out.get_mut(idx) {
                primary.is_primary = Some(true);
            }
        }

        let include_virtual = std::env::var("PROC_WINDOWS_INCLUDE_VIRTUAL_INTERFACES")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        if !include_virtual {
            out.retain(|n| {
                n.is_primary == Some(true)
                    || n.is_physical == Some(true)
                    || n.is_loopback == Some(true)
            });
        }
        FreeMibTable(table as _);
        Ok(out)
    }
}

fn collect_processes_from_nt(
    mode: ProcessMode,
    process_info_buffer: Option<&[u8]>,
) -> Result<Vec<ProcessSnapshot>> {
    let owned_buf;
    let buf = if let Some(buffer) = process_info_buffer {
        buffer
    } else {
        owned_buf = query_system_information(SYSTEM_PROCESS_INFORMATION_CLASS)?;
        owned_buf.as_slice()
    };
    let (_, _, summaries) = extract_process_thread_summaries(&buf);
    let mut out = Vec::new();
    let page = page_size_from_nt().max(1);
    let boot_filetime = boot_time_filetime_100ns();

    for (_offset, spi) in walk_nt_list::<SystemProcessInformation>(buf) {
        let pid = spi.unique_process_id.0 as usize as i32;
        let ppid = spi.inherited_from_unique_process_id.0 as usize as i32;
        let raw_create = nt_time_100ns(spi.create_time.quad_part);
        let start_time_ticks = raw_create.saturating_sub(boot_filetime);

        let comm_from_spi = {
            let n = utf16_from_unicode_string(&spi.image_name);
            if n.is_empty() {
                if pid == 0 {
                    "System Idle Process".to_string()
                } else {
                    "System".to_string()
                }
            } else {
                n
            }
        };

        let (proc_state, last_cpu) = summaries
            .get(&pid)
            .map(|s| (s.state.clone(), s.last_cpu))
            .unwrap_or_else(|| ("unknown".to_string(), None));

        let mut process = ProcessSnapshot {
            pid,
            ppid,
            comm: comm_from_spi,
            state: proc_state,
            num_threads: spi.number_of_threads as i64,
            priority: spi.base_priority as i64,
            nice: 0,
            minflt: spi.page_fault_count as u64,
            majflt: spi.hard_fault_count as u64,
            vsize_bytes: spi.virtual_size as u64,
            rss_pages: (spi.working_set_size as u64 / page) as i64,
            virtual_size_bytes: Some(spi.virtual_size as u64),
            resident_bytes: Some(spi.working_set_size as u64),
            utime_ticks: nt_time_100ns(spi.user_time.quad_part),
            stime_ticks: nt_time_100ns(spi.kernel_time.quad_part),
            start_time_ticks,
            processor: last_cpu,
            rt_priority: None,
            policy: None,
            oom_score: None,
            fd_count: Some(spi.handle_count as u64),
            fd_table_size: None,
            read_chars: Some(nt_time_100ns(spi.read_transfer_count.quad_part)),
            write_chars: Some(nt_time_100ns(spi.write_transfer_count.quad_part)),
            syscr: Some(nt_time_100ns(spi.read_operation_count.quad_part)),
            syscw: Some(nt_time_100ns(spi.write_operation_count.quad_part)),
            read_bytes: Some(nt_time_100ns(spi.read_transfer_count.quad_part)),
            write_bytes: Some(nt_time_100ns(spi.write_transfer_count.quad_part)),
            cancelled_write_bytes: None,
            vm_size_kib: None,
            vm_rss_kib: None,
            vm_data_kib: None,
            vm_stack_kib: None,
            vm_exe_kib: None,
            vm_lib_kib: None,
            vm_swap_kib: None,
            vm_pte_kib: None,
            vm_hwm_kib: None,
            working_set_bytes: Some(spi.working_set_size as u64),
            private_bytes: Some(spi.private_page_count as u64),
            peak_working_set_bytes: Some(spi.peak_working_set_size as u64),
            pagefile_usage_bytes: Some(spi.pagefile_usage as u64),
            commit_charge_bytes: Some(spi.private_page_count as u64),
            voluntary_ctxt_switches: None,
            nonvoluntary_ctxt_switches: None,
        };

        if matches!(mode, ProcessMode::Detailed) && pid > 4 {
            if let Some(handle) = open_process_limited(pid as u32) {
                if let Some(full_path) = process_image_name(handle.as_raw()) {
                    let base = process_basename(&full_path).to_string();
                    if !base.is_empty() {
                        process.comm = base;
                    }
                }
                if let Some((utime, stime, ctime)) = get_process_times_100ns(handle.as_raw()) {
                    process.utime_ticks = utime;
                    process.stime_ticks = stime;
                    process.start_time_ticks = ctime.saturating_sub(boot_filetime);
                }
                if let Some(mem) = get_process_mem(handle.as_raw()) {
                    process.rss_pages = (mem.WorkingSetSize as u64 / page) as i64;
                    process.resident_bytes = Some(mem.WorkingSetSize as u64);
                    process.working_set_bytes = Some(mem.WorkingSetSize as u64);
                    process.private_bytes = Some(mem.PrivateUsage as u64);
                    process.commit_charge_bytes = Some(mem.PrivateUsage as u64);
                    process.pagefile_usage_bytes = Some(mem.PagefileUsage as u64);
                    process.peak_working_set_bytes = Some(mem.PeakWorkingSetSize as u64);
                }
                if let Some(io) = get_process_io(handle.as_raw()) {
                    process.read_chars = Some(io.ReadTransferCount);
                    process.write_chars = Some(io.WriteTransferCount);
                    process.syscr = Some(io.ReadOperationCount);
                    process.syscw = Some(io.WriteOperationCount);
                    process.read_bytes = Some(io.ReadTransferCount);
                    process.write_bytes = Some(io.WriteTransferCount);
                }
                if let Some(h) = get_process_handle_count_safe(handle.as_raw()) {
                    process.fd_count = Some(h);
                }
                process.policy = get_priority_class_safe(handle.as_raw());
            }
        }

        out.push(process);
    }

    Ok(out)
}

pub fn collect_cpuinfo() -> Vec<CpuInfoSnapshot> {
    let ncpu = cpu_count_from_nt();
    let (reg_vendor, reg_model, reg_mhz) = cpu_metadata_from_registry();
    let cache_size_bytes = cpu_cache_size_bytes();
    let mut sysinfo = SYSTEM_INFO::default();
    unsafe { GetSystemInfo(&mut sysinfo) };
    let arch = unsafe { sysinfo.Anonymous.Anonymous.wProcessorArchitecture };
    let vendor_id = match (reg_vendor, arch.0) {
        (Some(v), _) if !v.is_empty() => Some(v),
        (_, v) if v == PROCESSOR_ARCHITECTURE_INTEL.0 => Some("GenuineIntel".to_string()),
        (_, v) if v == PROCESSOR_ARCHITECTURE_AMD64.0 => None,
        (_, v) if v == PROCESSOR_ARCHITECTURE_ARM64.0 => Some("ARM".to_string()),
        _ => None,
    };
    (0..ncpu)
        .map(|cpu| CpuInfoSnapshot {
            cpu,
            vendor_id: vendor_id.clone(),
            model_name: reg_model.clone(),
            mhz: reg_mhz,
            cache_size_bytes,
        })
        .collect()
}

pub fn collect_mounts() -> Vec<MountSnapshot> {
    let Ok(drives) = drive_strings() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for drive in drives {
        let drive_w = wide_z(&drive);
        let drive_type = unsafe { GetDriveTypeW(PCWSTR(drive_w.as_ptr())) };
        let mut fs_buf = vec![0u16; 64];
        let mut flags = 0u32;
        let ok = unsafe {
            GetVolumeInformationW(
                PCWSTR(drive_w.as_ptr()),
                None,
                None,
                None,
                Some(&mut flags),
                Some(&mut fs_buf),
            )
            .is_ok()
        };
        let fs_type = if ok {
            let len = fs_buf.iter().position(|c| *c == 0).unwrap_or(fs_buf.len());
            String::from_utf16_lossy(&fs_buf[..len])
        } else {
            String::new()
        };
        let read_only = ok && (flags & 0x00080000 != 0);
        let drive_type_str = match drive_type {
            t if t == DRIVE_FIXED => "fixed",
            t if t == DRIVE_REMOVABLE => "removable",
            t if t == DRIVE_CDROM => "cdrom",
            t if t == DRIVE_REMOTE => "remote",
            t if t == DRIVE_RAMDISK => "ramdisk",
            _ => "unknown",
        };
        out.push(MountSnapshot {
            device: drive.trim_end_matches('\\').to_string(),
            mountpoint: drive.trim_end_matches('\\').to_string(),
            fs_type: if fs_type.is_empty() {
                drive_type_str.to_string()
            } else {
                fs_type
            },
            read_only,
        });
    }
    out
}

fn utf16z_to_string(buf: &[u16]) -> String {
    let len = buf.iter().position(|c| *c == 0).unwrap_or(buf.len());
    String::from_utf16_lossy(&buf[..len])
}

fn query_volume_guid_for_mount(mountpoint: &str) -> Option<String> {
    let mount_root = format!("{}\\", mountpoint.trim_end_matches('\\'));
    let mount_w = wide_z(&mount_root);
    let mut buf = vec![0u16; 512];
    unsafe {
        GetVolumeNameForVolumeMountPointW(PCWSTR(mount_w.as_ptr()), &mut buf).ok()?;
    }
    let value = utf16z_to_string(&buf);
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn query_nt_device_for_mount(mountpoint: &str) -> Option<String> {
    let drive = mountpoint.trim_end_matches('\\');
    let drive_w = wide_z(drive);
    let mut buf = vec![0u16; 1024];
    unsafe {
        let len = QueryDosDeviceW(PCWSTR(drive_w.as_ptr()), Some(&mut buf));
        if len == 0 {
            return None;
        }
    }
    let value = utf16z_to_string(&buf);
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn query_physical_drive_for_mount(mountpoint: &str) -> Option<String> {
    let drive = mountpoint.trim_end_matches('\\');
    let path = format!(r"\\.\{drive}");
    let path_w = wide_z(&path);
    unsafe {
        let handle = CreateFileW(
            PCWSTR(path_w.as_ptr()),
            0,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            None,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            None,
        )
        .ok()?;
        let mut dev = STORAGE_DEVICE_NUMBER::default();
        let mut returned = 0u32;
        let ok = DeviceIoControl(
            handle,
            IOCTL_STORAGE_GET_DEVICE_NUMBER,
            None,
            0,
            Some(&mut dev as *mut _ as *mut c_void),
            size_of::<STORAGE_DEVICE_NUMBER>() as u32,
            Some(&mut returned),
            None,
        )
        .is_ok();
        let _ = CloseHandle(handle);
        if ok && returned >= size_of::<STORAGE_DEVICE_NUMBER>() as u32 {
            Some(format!("PhysicalDrive{}", dev.DeviceNumber))
        } else {
            None
        }
    }
}

fn collect_filesystem_stats(mounts: &[MountSnapshot]) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();

    for mount in mounts {
        let root = if mount.mountpoint.ends_with('\\') {
            mount.mountpoint.clone()
        } else {
            format!("{}\\", mount.mountpoint)
        };
        let root_w = wide_z(&root);

        let mut avail_to_user = 0u64;
        let mut total = 0u64;
        let mut free = 0u64;

        let ok = unsafe {
            GetDiskFreeSpaceExW(
                PCWSTR(root_w.as_ptr()),
                Some(&mut avail_to_user),
                Some(&mut total),
                Some(&mut free),
            )
        }
        .is_ok();

        if !ok {
            continue;
        }

        let used = total.saturating_sub(free);
        out.insert(format!("{}|total_bytes|value", mount.mountpoint), total);
        out.insert(format!("{}|used_bytes|value", mount.mountpoint), used);
        out.insert(format!("{}|free_bytes|value", mount.mountpoint), free);
        out.insert(
            format!("{}|avail_bytes|value", mount.mountpoint),
            avail_to_user,
        );
    }

    out
}

