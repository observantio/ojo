use crate::model::{
    CpuTimes, DiskSnapshot, LoadSnapshot, MemorySnapshot, NetDevSnapshot, ProcessSnapshot,
    Snapshot, SystemSnapshot,
};
use anyhow::{anyhow, Context, Result};
use std::collections::BTreeMap;
use std::ffi::c_void;
use std::mem::size_of;
use std::ptr::null_mut;
use std::slice;
use windows::core::{PCWSTR, PWSTR};
use windows::Win32::Foundation::{CloseHandle, GetLastError, HANDLE, NTSTATUS};
use windows::Win32::NetworkManagement::IpHelper::{FreeMibTable, GetIfTable2, MIB_IF_TABLE2};
use windows::Win32::Storage::FileSystem::{
    CreateFileW, GetDiskFreeSpaceExW, GetDriveTypeW, GetLogicalDriveStringsW, DRIVE_FIXED,
    FILE_ATTRIBUTE_NORMAL, FILE_GENERIC_READ, FILE_SHARE_READ, FILE_SHARE_WRITE,
    IOCTL_STORAGE_QUERY_PROPERTY, OPEN_EXISTING, STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR,
    STORAGE_DESCRIPTOR_HEADER, STORAGE_PROPERTY_ID, STORAGE_PROPERTY_QUERY, STORAGE_QUERY_TYPE,
};
use windows::Win32::System::Kernel::FILETIME;
use windows::Win32::System::ProcessStatus::{
    GetPerformanceInfo, GetProcessMemoryInfo, PERFORMANCE_INFORMATION, PROCESS_MEMORY_COUNTERS_EX,
};
use windows::Win32::System::SystemInformation::{
    GetSystemInfo, GetSystemTimes, GetTickCount64, GlobalMemoryStatusEx, MEMORYSTATUSEX,
    SYSTEM_INFO,
};
use windows::Win32::System::Threading::{
    GetPriorityClass, GetProcessHandleCount, GetProcessIoCounters, GetProcessTimes, OpenProcess,
    QueryFullProcessImageNameW, PROCESS_ACCESS_RIGHTS, PROCESS_IO_COUNTERS,
    PROCESS_QUERY_INFORMATION, PROCESS_VM_READ,
};
use windows::Win32::System::IO::DeviceIoControl;

#[link(name = "ntdll")]
unsafe extern "system" {
    fn NtQuerySystemInformation(
        system_information_class: u32,
        system_information: *mut c_void,
        system_information_length: u32,
        return_length: *mut u32,
    ) -> NTSTATUS;
}

const STATUS_INFO_LENGTH_MISMATCH: i32 = -1073741820i32;
const SYSTEM_PROCESS_INFORMATION_CLASS: u32 = 5;
const SYSTEM_PROCESSOR_PERFORMANCE_INFORMATION_CLASS: u32 = 8;

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct UnicodeString {
    length: u16,
    maximum_length: u16,
    buffer: PWSTR,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct LargeInteger {
    quad_part: i64,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct SystemProcessInformation {
    next_entry_offset: u32,
    number_of_threads: u32,
    working_set_private_size: LargeInteger,
    hard_fault_count: u32,
    number_of_threads_high_watermark: u32,
    cycle_time: u64,
    create_time: LargeInteger,
    user_time: LargeInteger,
    kernel_time: LargeInteger,
    image_name: UnicodeString,
    base_priority: i32,
    unique_process_id: HANDLE,
    inherited_from_unique_process_id: HANDLE,
    handle_count: u32,
    session_id: u32,
    unique_process_key: usize,
    peak_virtual_size: usize,
    virtual_size: usize,
    page_fault_count: u32,
    peak_working_set_size: usize,
    working_set_size: usize,
    quota_peak_paged_pool_usage: usize,
    quota_paged_pool_usage: usize,
    quota_peak_non_paged_pool_usage: usize,
    quota_non_paged_pool_usage: usize,
    pagefile_usage: usize,
    peak_pagefile_usage: usize,
    private_page_count: usize,
    read_operation_count: LargeInteger,
    write_operation_count: LargeInteger,
    other_operation_count: LargeInteger,
    read_transfer_count: LargeInteger,
    write_transfer_count: LargeInteger,
    other_transfer_count: LargeInteger,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct SystemProcessorPerformanceInformation {
    idle_time: LargeInteger,
    kernel_time: LargeInteger,
    user_time: LargeInteger,
    dpc_time: LargeInteger,
    interrupt_time: LargeInteger,
    interrupt_count: u32,
}

fn wide_z(s: &str) -> Vec<u16> {
    s.encode_utf16().chain(std::iter::once(0)).collect()
}

fn filetime_to_u64(ft: FILETIME) -> u64 {
    ((ft.dwHighDateTime as u64) << 32) | ft.dwLowDateTime as u64
}

fn nt_time_100ns(v: i64) -> u64 {
    if v < 0 {
        0
    } else {
        v as u64
    }
}

fn page_size() -> u64 {
    unsafe {
        let mut info = SYSTEM_INFO::default();
        GetSystemInfo(&mut info);
        info.dwPageSize as u64
    }
}

fn cpu_count() -> usize {
    unsafe {
        let mut info = SYSTEM_INFO::default();
        GetSystemInfo(&mut info);
        info.dwNumberOfProcessors as usize
    }
}

fn current_uptime_secs() -> f64 {
    unsafe { GetTickCount64() as f64 / 1000.0 }
}

fn nt_success(status: NTSTATUS) -> bool {
    status.0 >= 0
}

fn query_system_information(class: u32) -> Result<Vec<u8>> {
    let mut size: u32 = 64 * 1024;
    loop {
        let mut buf = vec![0u8; size as usize];
        let mut ret_len = 0u32;
        let status = unsafe {
            NtQuerySystemInformation(class, buf.as_mut_ptr() as *mut c_void, size, &mut ret_len)
        };
        if nt_success(status) {
            if ret_len > 0 && (ret_len as usize) <= buf.len() {
                buf.truncate(ret_len as usize);
            }
            return Ok(buf);
        }

        if status.0 == STATUS_INFO_LENGTH_MISMATCH {
            size = if ret_len > size {
                ret_len.saturating_add(4096)
            } else {
                size.saturating_mul(2)
            };
            continue;
        }

        return Err(anyhow!(
            "NtQuerySystemInformation({class}) failed: 0x{:08x}",
            status.0 as u32
        ));
    }
}

fn utf16_from_unicode_string(s: &UnicodeString) -> String {
    if s.length == 0 || s.buffer.is_null() {
        return String::new();
    }
    let len = (s.length / 2) as usize;
    let utf16 = unsafe { slice::from_raw_parts(s.buffer.0, len) };
    String::from_utf16_lossy(utf16)
}

unsafe fn open_process_limited(pid: u32) -> Option<HANDLE> {
    OpenProcess(
        PROCESS_ACCESS_RIGHTS(PROCESS_QUERY_INFORMATION.0 | PROCESS_VM_READ.0),
        false,
        pid,
    )
    .ok()
}

fn process_image_name(handle: HANDLE) -> Option<String> {
    unsafe {
        let mut buf = vec![0u16; 32768];
        let mut size = buf.len() as u32;
        if QueryFullProcessImageNameW(handle, 0, PWSTR(buf.as_mut_ptr()), &mut size).as_bool() {
            Some(String::from_utf16_lossy(&buf[..size as usize]))
        } else {
            None
        }
    }
}

fn get_process_times_ticks_100ns(handle: HANDLE) -> Option<(u64, u64, u64)> {
    unsafe {
        let mut creation = FILETIME::default();
        let mut exit = FILETIME::default();
        let mut kernel = FILETIME::default();
        let mut user = FILETIME::default();

        if GetProcessTimes(handle, &mut creation, &mut exit, &mut kernel, &mut user).as_bool() {
            Some((
                filetime_to_u64(user),
                filetime_to_u64(kernel),
                filetime_to_u64(creation),
            ))
        } else {
            None
        }
    }
}

fn get_process_mem(handle: HANDLE) -> Option<PROCESS_MEMORY_COUNTERS_EX> {
    unsafe {
        let mut pmc = PROCESS_MEMORY_COUNTERS_EX::default();
        if GetProcessMemoryInfo(
            handle,
            &mut pmc as *mut _ as *mut _,
            size_of::<PROCESS_MEMORY_COUNTERS_EX>() as u32,
        )
        .as_bool()
        {
            Some(pmc)
        } else {
            None
        }
    }
}

fn get_process_io(handle: HANDLE) -> Option<PROCESS_IO_COUNTERS> {
    unsafe {
        let mut io = PROCESS_IO_COUNTERS::default();
        if GetProcessIoCounters(handle, &mut io).as_bool() {
            Some(io)
        } else {
            None
        }
    }
}

fn get_process_handle_count_safe(handle: HANDLE) -> Option<u64> {
    unsafe {
        let mut count = 0u32;
        if GetProcessHandleCount(handle, &mut count).as_bool() {
            Some(count as u64)
        } else {
            None
        }
    }
}

fn get_priority_class_safe(handle: HANDLE) -> Option<u64> {
    unsafe {
        let cls = GetPriorityClass(handle);
        if cls == 0 {
            None
        } else {
            Some(cls as u64)
        }
    }
}

fn collect_per_cpu_times() -> Result<(Vec<CpuTimes>, u64)> {
    let buf = query_system_information(SYSTEM_PROCESSOR_PERFORMANCE_INFORMATION_CLASS)?;
    let count = buf.len() / size_of::<SystemProcessorPerformanceInformation>();
    let infos = unsafe {
        slice::from_raw_parts(
            buf.as_ptr() as *const SystemProcessorPerformanceInformation,
            count,
        )
    };

    let mut per_cpu = Vec::with_capacity(count);
    let mut total_interrupt_count = 0u64;

    for cpu in infos {
        let idle = nt_time_100ns(cpu.idle_time.quad_part);
        let kernel = nt_time_100ns(cpu.kernel_time.quad_part);
        let user = nt_time_100ns(cpu.user_time.quad_part);
        let dpc = nt_time_100ns(cpu.dpc_time.quad_part);
        let interrupt = nt_time_100ns(cpu.interrupt_time.quad_part);

        total_interrupt_count = total_interrupt_count.saturating_add(cpu.interrupt_count as u64);

        per_cpu.push(CpuTimes {
            user,
            nice: 0,
            system: kernel.saturating_sub(idle),
            idle,
            iowait: 0,
            irq: interrupt,
            softirq: dpc,
            steal: 0,
            guest: 0,
            guest_nice: 0,
        });
    }

    Ok((per_cpu, total_interrupt_count))
}

fn collect_system_total_cpu() -> Result<CpuTimes> {
    unsafe {
        let mut idle = FILETIME::default();
        let mut kernel = FILETIME::default();
        let mut user = FILETIME::default();

        GetSystemTimes(&mut idle, &mut kernel, &mut user)
            .ok()
            .context("GetSystemTimes failed")?;

        let idle_u = filetime_to_u64(idle);
        let kernel_u = filetime_to_u64(kernel);
        let user_u = filetime_to_u64(user);

        Ok(CpuTimes {
            user: user_u,
            nice: 0,
            system: kernel_u.saturating_sub(idle_u),
            idle: idle_u,
            iowait: 0,
            irq: 0,
            softirq: 0,
            steal: 0,
            guest: 0,
            guest_nice: 0,
        })
    }
}

pub fn collect_system() -> Result<SystemSnapshot> {
    let (per_cpu, total_interrupt_count) = collect_per_cpu_times()?;
    let cpu_total = collect_system_total_cpu()?;
    let process_count = collect_process_summaries(false)?.len() as u64;

    Ok(SystemSnapshot {
        ticks_per_second: 10_000_000,
        boot_time_epoch_secs: 0,
        uptime_secs: current_uptime_secs(),
        context_switches: 0,
        forks_since_boot: 0,
        interrupts_total: total_interrupt_count,
        softirqs_total: 0,
        process_count,
        pid_max: 0,
        entropy_available_bits: 0,
        entropy_pool_size_bits: 0,
        procs_running: 0,
        procs_blocked: 0,
        cpu_total,
        per_cpu,
    })
}

pub fn collect_memory() -> Result<MemorySnapshot> {
    unsafe {
        let mut mem = MEMORYSTATUSEX::default();
        mem.dwLength = size_of::<MEMORYSTATUSEX>() as u32;
        GlobalMemoryStatusEx(&mut mem)
            .ok()
            .context("GlobalMemoryStatusEx failed")?;

        let mut perf = PERFORMANCE_INFORMATION::default();
        perf.cb = size_of::<PERFORMANCE_INFORMATION>() as u32;
        GetPerformanceInfo(&mut perf, size_of::<PERFORMANCE_INFORMATION>() as u32)
            .ok()
            .context("GetPerformanceInfo failed")?;

        let page = perf.PageSize as u64;
        let total_phys = mem.ullTotalPhys;
        let avail_phys = mem.ullAvailPhys;
        let total_pagefile = mem.ullTotalPageFile;
        let avail_pagefile = mem.ullAvailPageFile;

        Ok(MemorySnapshot {
            mem_total_bytes: total_phys,
            mem_free_bytes: avail_phys,
            mem_available_bytes: avail_phys,
            buffers_bytes: 0,
            cached_bytes: (perf.SystemCache as u64).saturating_mul(page),
            active_bytes: 0,
            inactive_bytes: 0,
            anon_pages_bytes: 0,
            mapped_bytes: 0,
            shmem_bytes: 0,
            swap_total_bytes: total_pagefile.saturating_sub(total_phys),
            swap_free_bytes: avail_pagefile.saturating_sub(avail_phys),
            swap_cached_bytes: 0,
            dirty_bytes: 0,
            writeback_bytes: 0,
            slab_bytes: 0,
            sreclaimable_bytes: 0,
            sunreclaim_bytes: 0,
            page_tables_bytes: 0,
            committed_as_bytes: (perf.CommitTotal as u64).saturating_mul(page),
            commit_limit_bytes: (perf.CommitLimit as u64).saturating_mul(page),
            kernel_stack_bytes: 0,
            hugepages_total: 0,
            hugepages_free: 0,
            hugepage_size_bytes: 0,
            anon_hugepages_bytes: 0,
        })
    }
}

pub fn collect_load() -> Result<LoadSnapshot> {
    Ok(LoadSnapshot {
        one: 0.0,
        five: 0.0,
        fifteen: 0.0,
        runnable: 0,
        entities: cpu_count() as u32,
        latest_pid: 0,
    })
}

fn drive_strings() -> Result<Vec<String>> {
    unsafe {
        let mut buf = vec![0u16; 512];
        let len = GetLogicalDriveStringsW(Some(&mut buf)) as usize;
        if len == 0 {
            return Err(anyhow!(
                "GetLogicalDriveStringsW failed: {}",
                GetLastError().0
            ));
        }

        let mut out = Vec::new();
        let mut start = 0usize;
        for i in 0..len {
            if buf[i] == 0 {
                if i > start {
                    out.push(String::from_utf16_lossy(&buf[start..i]));
                }
                start = i + 1;
            }
        }
        Ok(out)
    }
}

fn query_storage_alignment(drive_root: &str) -> (Option<u64>, Option<u64>, Option<bool>) {
    let drive_letter = drive_root.chars().next().unwrap_or('C');
    let path = format!(r"\\.\{}:", drive_letter);
    let path_w = wide_z(&path);

    unsafe {
        let handle = match CreateFileW(
            PCWSTR(path_w.as_ptr()),
            FILE_GENERIC_READ.0,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            None,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            None,
        ) {
            Ok(handle) => handle,
            Err(_) => return (None, None, None),
        };

        let mut query = STORAGE_PROPERTY_QUERY {
            PropertyId: STORAGE_PROPERTY_ID(6),
            QueryType: STORAGE_QUERY_TYPE(0),
            AdditionalParameters: [0],
        };
        let mut out = vec![0u8; 1024];
        let mut returned = 0u32;

        let ok = DeviceIoControl(
            handle,
            IOCTL_STORAGE_QUERY_PROPERTY,
            Some(&mut query as *mut _ as *mut c_void),
            size_of::<STORAGE_PROPERTY_QUERY>() as u32,
            Some(out.as_mut_ptr() as *mut c_void),
            out.len() as u32,
            Some(&mut returned),
            None,
        )
        .as_bool();

        let _ = CloseHandle(handle);

        if !ok || returned < size_of::<STORAGE_DESCRIPTOR_HEADER>() as u32 {
            return (None, None, None);
        }
        if returned < size_of::<STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR>() as u32 {
            return (None, None, None);
        }

        let desc = &*(out.as_ptr() as *const STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR);
        (
            Some(desc.BytesPerLogicalSector as u64),
            Some(desc.BytesPerPhysicalSector as u64),
            None,
        )
    }
}

pub fn collect_disks() -> Result<Vec<DiskSnapshot>> {
    let drives = drive_strings()?;
    let mut out = Vec::new();

    for drive in drives {
        let drive_w = wide_z(&drive);
        unsafe {
            if GetDriveTypeW(PCWSTR(drive_w.as_ptr())) != DRIVE_FIXED {
                continue;
            }

            let mut free_available = 0u64;
            let mut total_bytes = 0u64;
            let mut total_free = 0u64;
            let _ = GetDiskFreeSpaceExW(
                PCWSTR(drive_w.as_ptr()),
                Some(&mut free_available),
                Some(&mut total_bytes),
                Some(&mut total_free),
            );

            let (logical, physical, rotational) = query_storage_alignment(&drive);

            out.push(DiskSnapshot {
                name: drive.trim_end_matches('\\').to_string(),
                reads: 0,
                writes: 0,
                sectors_read: 0,
                sectors_written: 0,
                time_reading_ms: 0,
                time_writing_ms: 0,
                in_progress: 0,
                time_in_progress_ms: 0,
                weighted_time_in_progress_ms: 0,
                logical_block_size: logical,
                physical_block_size: physical,
                rotational,
            });
        }
    }

    Ok(out)
}

fn wchar_array_to_string<const N: usize>(arr: &[u16; N]) -> String {
    let len = arr.iter().position(|c| *c == 0).unwrap_or(N);
    String::from_utf16_lossy(&arr[..len])
}

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
            let name = wchar_array_to_string(&row.Alias);
            out.push(NetDevSnapshot {
                name,
                mtu: Some(row.Mtu as u64),
                speed_mbps: Some(row.ReceiveLinkSpeed / 1_000_000),
                tx_queue_len: None,
                carrier_up: Some(row.OperStatus.0 == 1),
                rx_bytes: row.InOctets,
                rx_packets: row.InUcastPkts + row.InNUcastPkts,
                rx_errs: row.InErrors,
                rx_drop: row.InDiscards,
                rx_fifo: 0,
                rx_frame: 0,
                rx_compressed: 0,
                rx_multicast: row.InMulticastPkts,
                tx_bytes: row.OutOctets,
                tx_packets: row.OutUcastPkts + row.OutNUcastPkts,
                tx_errs: row.OutErrors,
                tx_drop: row.OutDiscards,
                tx_fifo: 0,
                tx_colls: 0,
                tx_carrier: 0,
                tx_compressed: 0,
            });
        }

        FreeMibTable(table as _);
        Ok(out)
    }
}

fn collect_process_summaries(open_handles: bool) -> Result<Vec<ProcessSnapshot>> {
    let buf = query_system_information(SYSTEM_PROCESS_INFORMATION_CLASS)?;
    let mut out = Vec::new();
    let mut offset = 0usize;
    let page = page_size();

    loop {
        if offset + size_of::<SystemProcessInformation>() > buf.len() {
            break;
        }

        let spi = unsafe { &*(buf.as_ptr().add(offset) as *const SystemProcessInformation) };
        let pid = spi.unique_process_id.0 as usize as i32;
        let ppid = spi.inherited_from_unique_process_id.0 as usize as i32;

        let mut process = ProcessSnapshot {
            pid,
            ppid,
            comm: {
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
            },
            state: "unknown".to_string(),
            num_threads: spi.number_of_threads as i64,
            priority: spi.base_priority as i64,
            nice: 0,
            minflt: spi.page_fault_count as u64,
            majflt: spi.hard_fault_count as u64,
            vsize_bytes: spi.virtual_size as u64,
            rss_pages: (spi.working_set_size as u64 / page) as i64,
            utime_ticks: nt_time_100ns(spi.user_time.quad_part),
            stime_ticks: nt_time_100ns(spi.kernel_time.quad_part),
            start_time_ticks: nt_time_100ns(spi.create_time.quad_part),
            processor: None,
            rt_priority: None,
            policy: None,
            oom_score: None,
            fd_count: None,
            read_chars: None,
            write_chars: None,
            syscr: Some(spi.read_operation_count.quad_part.max(0) as u64),
            syscw: Some(spi.write_operation_count.quad_part.max(0) as u64),
            read_bytes: Some(spi.read_transfer_count.quad_part.max(0) as u64),
            write_bytes: Some(spi.write_transfer_count.quad_part.max(0) as u64),
            cancelled_write_bytes: Some(spi.other_transfer_count.quad_part),
            vm_size_kib: Some((spi.virtual_size as u64) / 1024),
            vm_rss_kib: Some((spi.working_set_size as u64) / 1024),
            vm_data_kib: Some((spi.private_page_count as u64) / 1024),
            vm_stack_kib: None,
            vm_exe_kib: None,
            vm_lib_kib: None,
            vm_swap_kib: Some((spi.pagefile_usage as u64) / 1024),
            vm_pte_kib: None,
            vm_hwm_kib: Some((spi.peak_working_set_size as u64) / 1024),
            voluntary_ctxt_switches: None,
            nonvoluntary_ctxt_switches: None,
        };

        if open_handles && pid > 0 {
            unsafe {
                if let Some(handle) = open_process_limited(pid as u32) {
                    if let Some(full_name) = process_image_name(handle) {
                        process.comm = full_name;
                    }

                    if let Some((utime, stime, ctime)) = get_process_times_ticks_100ns(handle) {
                        process.utime_ticks = utime;
                        process.stime_ticks = stime;
                        process.start_time_ticks = ctime;
                    }

                    if let Some(mem) = get_process_mem(handle) {
                        process.vm_size_kib = Some((mem.PrivateUsage as u64) / 1024);
                        process.vm_rss_kib = Some((mem.WorkingSetSize as u64) / 1024);
                        process.vm_hwm_kib = Some((mem.PeakWorkingSetSize as u64) / 1024);
                        process.rss_pages = (mem.WorkingSetSize as u64 / page) as i64;
                    }

                    if let Some(io) = get_process_io(handle) {
                        process.syscr = Some(io.ReadOperationCount);
                        process.syscw = Some(io.WriteOperationCount);
                        process.read_bytes = Some(io.ReadTransferCount);
                        process.write_bytes = Some(io.WriteTransferCount);
                        process.cancelled_write_bytes = Some(io.OtherTransferCount as i64);
                    }

                    process.fd_count = get_process_handle_count_safe(handle);
                    process.policy = get_priority_class_safe(handle);

                    let _ = CloseHandle(handle);
                }
            }
        }

        out.push(process);

        if spi.next_entry_offset == 0 {
            break;
        }
        offset += spi.next_entry_offset as usize;
    }

    Ok(out)
}

pub fn collect_processes() -> Result<Vec<ProcessSnapshot>> {
    collect_process_summaries(true)
}

pub fn collect_snapshot(include_process_metrics: bool) -> Result<Snapshot> {
    Ok(Snapshot {
        system: collect_system()?,
        memory: collect_memory()?,
        load: collect_load()?,
        pressure: BTreeMap::new(),
        pressure_totals_us: BTreeMap::new(),
        vmstat: BTreeMap::new(),
        interrupts: BTreeMap::new(),
        softirqs: BTreeMap::new(),
        net_snmp: BTreeMap::new(),
        softnet: Vec::new(),
        swaps: Vec::new(),
        mounts: Vec::new(),
        cpuinfo: Vec::new(),
        zoneinfo: BTreeMap::new(),
        buddyinfo: BTreeMap::new(),
        disks: collect_disks()?,
        net: collect_net()?,
        processes: if include_process_metrics {
            collect_processes()?
        } else {
            Vec::new()
        },
    })
}
