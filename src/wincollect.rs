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
use std::sync::{Mutex, OnceLock};
use std::time::Instant;
use tracing::debug;

use windows::core::{PCWSTR, PWSTR};

use windows::Win32::Foundation::{CloseHandle, GetLastError, FILETIME, HANDLE, NTSTATUS};

use windows::Win32::NetworkManagement::IpHelper::{FreeMibTable, GetIfTable2, MIB_IF_TABLE2};

use windows::Win32::Storage::FileSystem::{
    CreateFileW, GetDiskFreeSpaceExW, GetDriveTypeW, GetLogicalDriveStringsW,
    FILE_ATTRIBUTE_NORMAL, FILE_GENERIC_READ, FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
};

use windows::Win32::System::Ioctl::{
    IOCTL_STORAGE_GET_DEVICE_NUMBER, IOCTL_STORAGE_QUERY_PROPERTY,
    STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR, STORAGE_DESCRIPTOR_HEADER, STORAGE_DEVICE_NUMBER,
    STORAGE_PROPERTY_ID, STORAGE_PROPERTY_QUERY, STORAGE_QUERY_TYPE,
};

use windows::Win32::System::IO::DeviceIoControl;

use windows::Win32::System::ProcessStatus::{
    GetPerformanceInfo, GetProcessMemoryInfo, PERFORMANCE_INFORMATION, PROCESS_MEMORY_COUNTERS_EX,
};

use windows::Win32::System::SystemInformation::{
    GetSystemInfo, GetSystemTimeAsFileTime, GetTickCount64, GlobalMemoryStatusEx, MEMORYSTATUSEX,
    SYSTEM_INFO,
};

use windows::Win32::System::Threading::{
    GetPriorityClass, GetProcessHandleCount, GetProcessIoCounters, GetProcessTimes, GetSystemTimes,
    IO_COUNTERS, OpenProcess, QueryFullProcessImageNameW, PROCESS_ACCESS_RIGHTS,
    PROCESS_NAME_FORMAT, PROCESS_QUERY_INFORMATION, PROCESS_VM_READ,
};

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
const SYSTEM_PERFORMANCE_INFORMATION_CLASS: u32 = 2;
const SYSTEM_PROCESS_INFORMATION_CLASS: u32 = 5;
const SYSTEM_PROCESSOR_PERFORMANCE_INFORMATION_CLASS: u32 = 8;
const DRIVE_FIXED_VALUE: u32 = 3;
const IOCTL_DISK_PERFORMANCE: u32 = 0x0007_0020;
const WINDOWS_TO_UNIX_EPOCH_100NS: u64 = 116_444_736_000_000_000;
const SECTOR_SIZE: u64 = 512;

#[repr(C)]
#[derive(Clone, Copy)]
struct UnicodeString {
    length: u16,
    maximum_length: u16,
    buffer: PWSTR,
}

impl Default for UnicodeString {
    fn default() -> Self {
        Self {
            length: 0,
            maximum_length: 0,
            buffer: PWSTR::null(),
        }
    }
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
struct SystemPerformanceInformation {
    idle_process_time: LargeInteger,
    io_read_transfer_count: LargeInteger,
    io_write_transfer_count: LargeInteger,
    io_other_transfer_count: LargeInteger,
    io_read_operation_count: u32,
    io_write_operation_count: u32,
    io_other_operation_count: u32,
    available_pages: u32,
    committed_pages: u32,
    commit_limit: u32,
    peak_commitment: u32,
    page_fault_count: u32,
    copy_on_write_count: u32,
    transition_count: u32,
    cache_transition_count: u32,
    demand_zero_count: u32,
    page_read_count: u32,
    page_read_io_count: u32,
    cache_read_count: u32,
    cache_io_count: u32,
    dirty_pages_write_count: u32,
    dirty_write_io_count: u32,
    mapped_pages_write_count: u32,
    mapped_write_io_count: u32,
    paged_pool_pages: u32,
    non_paged_pool_pages: u32,
    paged_pool_allocs: u32,
    paged_pool_frees: u32,
    non_paged_pool_allocs: u32,
    non_paged_pool_frees: u32,
    free_system_ptes: u32,
    resident_system_code_page: u32,
    total_system_driver_pages: u32,
    total_system_code_pages: u32,
    non_paged_pool_lookaside_hits: u32,
    paged_pool_lookaside_hits: u32,
    spare3_count: u32,
    resident_system_cache_page: u32,
    resident_paged_pool_page: u32,
    resident_system_driver_page: u32,
    ccp_fast_read_no_wait: u32,
    ccp_fast_read_wait: u32,
    ccp_fast_read_resource_miss: u32,
    ccp_fast_read_not_possible: u32,
    ccp_fast_mdl_read_no_wait: u32,
    ccp_fast_mdl_read_wait: u32,
    ccp_fast_mdl_read_resource_miss: u32,
    ccp_fast_mdl_read_not_possible: u32,
    ccp_map_data_no_wait: u32,
    ccp_map_data_wait: u32,
    ccp_map_data_no_wait_miss: u32,
    ccp_map_data_wait_miss: u32,
    ccp_pin_mapped_data_count: u32,
    ccp_pin_read_no_wait: u32,
    ccp_pin_read_wait: u32,
    ccp_pin_read_no_wait_miss: u32,
    ccp_pin_read_wait_miss: u32,
    ccp_copy_read_no_wait: u32,
    ccp_copy_read_wait: u32,
    ccp_copy_read_no_wait_miss: u32,
    ccp_copy_read_wait_miss: u32,
    ccp_mdl_read_no_wait: u32,
    ccp_mdl_read_wait: u32,
    ccp_mdl_read_no_wait_miss: u32,
    ccp_mdl_read_wait_miss: u32,
    ccp_read_ahead_ios: u32,
    ccp_lazy_write_ios: u32,
    ccp_lazy_write_pages: u32,
    ccp_data_flushes: u32,
    ccp_data_pages: u32,
    context_switches: u32,
    first_level_tb_fills: u32,
    second_level_tb_fills: u32,
    system_calls: u32,
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

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct DiskPerformance {
    bytes_read: LargeInteger,
    bytes_written: LargeInteger,
    read_time: LargeInteger,
    write_time: LargeInteger,
    idle_time: LargeInteger,
    read_count: u32,
    write_count: u32,
    queue_depth: u32,
    split_count: u32,
    query_time: LargeInteger,
    storage_device_number: u32,
    storage_manager_name: [u16; 8],
}

struct DiskPerfData {
    reads: u64,
    writes: u64,
    sectors_read: u64,
    sectors_written: u64,
    time_reading_ms: u64,
    time_writing_ms: u64,
    queue_depth: u64,
    time_in_progress_ms: u64,
    weighted_time_in_progress_ms: u64,
}

fn wide_z(s: &str) -> Vec<u16> {
    s.encode_utf16().chain(std::iter::once(0)).collect()
}

fn filetime_to_u64(ft: FILETIME) -> u64 {
    ((ft.dwHighDateTime as u64) << 32) | ft.dwLowDateTime as u64
}

fn filetime_now_100ns() -> u64 {
    unsafe { filetime_to_u64(GetSystemTimeAsFileTime()) }
}

fn filetime_100ns_to_unix_secs(v: u64) -> u64 {
    v.saturating_sub(WINDOWS_TO_UNIX_EPOCH_100NS) / 10_000_000
}

fn boot_time_filetime_100ns() -> u64 {
    let now = filetime_now_100ns();
    let uptime_100ns = (current_uptime_secs() * 10_000_000.0) as u64;
    now.saturating_sub(uptime_100ns)
}

fn nt_time_100ns(v: i64) -> u64 {
    if v < 0 { 0 } else { v as u64 }
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

fn process_basename(path: &str) -> &str {
    path.rsplit(['\\', '/']).next().unwrap_or(path)
}

#[derive(Clone, Copy, Debug)]
struct LoadAvgState {
    one: f64,
    five: f64,
    fifteen: f64,
    prev_busy: u64,
    prev_total: u64,
    last: Instant,
}

static LOAD_AVG_STATE: OnceLock<Mutex<Option<LoadAvgState>>> = OnceLock::new();

fn current_uptime_secs() -> f64 {
    unsafe { GetTickCount64() as f64 / 1000.0 }
}

fn nt_success(status: NTSTATUS) -> bool {
    status.0 >= 0
}

fn system_performance_info() -> Option<SystemPerformanceInformation> {
    let buf = query_system_information(SYSTEM_PERFORMANCE_INFORMATION_CLASS).ok()?;
    if buf.len() < size_of::<SystemPerformanceInformation>() {
        return None;
    }
    Some(unsafe { *(buf.as_ptr() as *const SystemPerformanceInformation) })
}

fn collect_vmstat() -> BTreeMap<String, i64> {
    let mut vmstat = BTreeMap::new();
    let Some(perf) = system_performance_info() else {
        return vmstat;
    };

    let page_reads = perf.page_read_count as u64;
    let page_writes = perf.dirty_pages_write_count as u64 + perf.mapped_pages_write_count as u64;

    vmstat.insert("pgfault".to_string(), perf.page_fault_count as i64);
    vmstat.insert("pgmajfault".to_string(), perf.page_read_count as i64);
    vmstat.insert("pgpgin".to_string(), page_reads.min(i64::MAX as u64) as i64);
    vmstat.insert("pgpgout".to_string(), page_writes.min(i64::MAX as u64) as i64);
    vmstat.insert("pswpin".to_string(), page_reads.min(i64::MAX as u64) as i64);
    vmstat.insert("pswpout".to_string(), page_writes.min(i64::MAX as u64) as i64);

    vmstat
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
        if QueryFullProcessImageNameW(
            handle,
            PROCESS_NAME_FORMAT(0),
            PWSTR(buf.as_mut_ptr()),
            &mut size,
        )
        .is_ok()
        {
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
        if GetProcessTimes(handle, &mut creation, &mut exit, &mut kernel, &mut user).is_ok() {
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
        .is_ok()
        {
            Some(pmc)
        } else {
            None
        }
    }
}

fn get_process_io(handle: HANDLE) -> Option<IO_COUNTERS> {
    unsafe {
        let mut io = IO_COUNTERS::default();
        if GetProcessIoCounters(handle, &mut io).is_ok() {
            Some(io)
        } else {
            None
        }
    }
}

fn get_process_handle_count_safe(handle: HANDLE) -> Option<u64> {
    unsafe {
        let mut count = 0u32;
        if GetProcessHandleCount(handle, &mut count).is_ok() {
            Some(count as u64)
        } else {
            None
        }
    }
}

fn get_priority_class_safe(handle: HANDLE) -> Option<u64> {
    unsafe {
        let cls = GetPriorityClass(handle);
        if cls == 0 { None } else { Some(cls as u64) }
    }
}

fn per_cpu_times_from_nt() -> Option<Vec<CpuTimes>> {
    let entry_size = size_of::<SystemProcessorPerformanceInformation>();
    let cpu_count = cpu_count().max(1);
    let buf_size = (cpu_count * entry_size) as u32;
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
    for i in 0..count {
        let entry = unsafe {
            &*(buf.as_ptr().add(i * entry_size) as *const SystemProcessorPerformanceInformation)
        };
        let idle = nt_time_100ns(entry.idle_time.quad_part);
        let kernel = nt_time_100ns(entry.kernel_time.quad_part);
        let user = nt_time_100ns(entry.user_time.quad_part);
        let dpc = nt_time_100ns(entry.dpc_time.quad_part);
        let irq = nt_time_100ns(entry.interrupt_time.quad_part);
        let system = kernel.saturating_sub(idle).saturating_sub(dpc).saturating_sub(irq);
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
    Some(out)
}

fn cpu_times_from_system() -> Result<(CpuTimes, Vec<CpuTimes>)> {
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
    let kernel_100ns = filetime_to_u64(kernel);
    let user_100ns = filetime_to_u64(user);
    let system_100ns = kernel_100ns.saturating_sub(idle_100ns);
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
    let per_cpu = match per_cpu_times_from_nt() {
        Some(v) if !v.is_empty() => v,
        _ => {
            let cores = cpu_count().max(1) as u64;
            let per_core_user = user_100ns / cores;
            let per_core_system = system_100ns / cores;
            let per_core_idle = idle_100ns / cores;
            (0..cores)
                .map(|_| CpuTimes {
                    user: per_core_user,
                    nice: 0,
                    system: per_core_system,
                    idle: per_core_idle,
                    iowait: 0,
                    irq: 0,
                    softirq: 0,
                    steal: 0,
                    guest: 0,
                    guest_nice: 0,
                })
                .collect()
        }
    };
    Ok((total, per_cpu))
}

pub fn collect_system() -> Result<SystemSnapshot> {
    debug!("wincollect: collect_system start");
    let uptime_secs = current_uptime_secs();
    let (cpu_total, per_cpu) = cpu_times_from_system()?;
    debug!("wincollect: collect_system per_cpu done");
    let perf = system_performance_info();
    let boot_filetime = boot_time_filetime_100ns();
    let (process_count, procs_running) = unsafe {
        let mut info = PERFORMANCE_INFORMATION::default();
        info.cb = size_of::<PERFORMANCE_INFORMATION>() as u32;
        if GetPerformanceInfo(&mut info, info.cb).is_ok() {
            (info.ProcessCount as u64, info.ProcessCount as u32)
        } else {
            (0u64, 0u32)
        }
    };
    debug!("wincollect: collect_system process_count done");
    Ok(SystemSnapshot {
        is_windows: true,
        ticks_per_second: 10_000_000,
        boot_time_epoch_secs: filetime_100ns_to_unix_secs(boot_filetime),
        uptime_secs,
        context_switches: perf.map(|p| p.context_switches as u64).unwrap_or(0),
        forks_since_boot: 0,
        interrupts_total: 0,
        softirqs_total: 0,
        process_count,
        pid_max: 0,
        entropy_available_bits: 0,
        entropy_pool_size_bits: 0,
        procs_running,
        procs_blocked: 0,
        cpu_total,
        per_cpu,
        cpu_cycle_utilization: None,
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

pub fn collect_load(cpu_total: &CpuTimes) -> Result<LoadSnapshot> {
    let entities = cpu_count().max(1) as u32;
    let current_busy = cpu_total.busy();
    let current_total = cpu_total.total();
    let now = Instant::now();
    let state = LOAD_AVG_STATE.get_or_init(|| Mutex::new(None));
    let mut guard = state.lock().expect("load avg mutex poisoned");
    let (one, five, fifteen) = match *guard {
        None => {
            let instant_util = if current_total > 0 {
                current_busy as f64 / current_total as f64
            } else {
                0.0
            };
            let instant_load = (instant_util * entities as f64).clamp(0.0, entities as f64);
            *guard = Some(LoadAvgState {
                one: instant_load,
                five: instant_load,
                fifteen: instant_load,
                prev_busy: current_busy,
                prev_total: current_total,
                last: now,
            });
            (instant_load, instant_load, instant_load)
        }
        Some(prev) => {
            let delta_total = current_total.saturating_sub(prev.prev_total);
            let delta_busy = current_busy.saturating_sub(prev.prev_busy);
            let instant_util = if delta_total > 0 {
                delta_busy as f64 / delta_total as f64
            } else {
                0.0
            };
            let instant_load = (instant_util * entities as f64).clamp(0.0, entities as f64);
            let dt = now.duration_since(prev.last).as_secs_f64().max(0.001);
            let alpha1 = (-dt / 60.0).exp();
            let alpha5 = (-dt / 300.0).exp();
            let alpha15 = (-dt / 900.0).exp();
            let one = prev.one * alpha1 + instant_load * (1.0 - alpha1);
            let five = prev.five * alpha5 + instant_load * (1.0 - alpha5);
            let fifteen = prev.fifteen * alpha15 + instant_load * (1.0 - alpha15);
            *guard = Some(LoadAvgState {
                one,
                five,
                fifteen,
                prev_busy: current_busy,
                prev_total: current_total,
                last: now,
            });
            (one, five, fifteen)
        }
    };
    Ok(LoadSnapshot {
        one,
        five,
        fifteen,
        runnable: one.round() as u32,
        entities,
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
            Ok(h) => h,
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
        .is_ok();
        let _ = CloseHandle(handle);
        if !ok
            || returned < size_of::<STORAGE_DESCRIPTOR_HEADER>() as u32
            || returned < size_of::<STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR>() as u32
        {
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

fn query_disk_performance(drive_root: &str) -> Option<DiskPerfData> {
    let drive_letter = drive_root.chars().next().unwrap_or('C');
    let path = format!(r"\\.\{}:", drive_letter);
    let path_w = wide_z(&path);

    unsafe {
        let handle = CreateFileW(
            PCWSTR(path_w.as_ptr()),
            FILE_GENERIC_READ.0,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            None,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            None,
        )
        .ok()?;

        let query_ioctl = |h: HANDLE| -> Option<DiskPerformance> {
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
                None
            }
        };

        let raw = if let Some(p) = query_ioctl(handle) {
            Some(p)
        } else {
            let mut dev_num = STORAGE_DEVICE_NUMBER::default();
            let mut returned = 0u32;
            let ok = DeviceIoControl(
                handle,
                IOCTL_STORAGE_GET_DEVICE_NUMBER,
                None,
                0,
                Some(&mut dev_num as *mut _ as *mut c_void),
                size_of::<STORAGE_DEVICE_NUMBER>() as u32,
                Some(&mut returned),
                None,
            )
            .is_ok();
            if ok && returned >= size_of::<STORAGE_DEVICE_NUMBER>() as u32 {
                let phys_path = format!(r"\\.\PhysicalDrive{}", dev_num.DeviceNumber);
                let phys_w = wide_z(&phys_path);
                if let Ok(phys_handle) = CreateFileW(
                    PCWSTR(phys_w.as_ptr()),
                    FILE_GENERIC_READ.0,
                    FILE_SHARE_READ | FILE_SHARE_WRITE,
                    None,
                    OPEN_EXISTING,
                    FILE_ATTRIBUTE_NORMAL,
                    None,
                ) {
                    let p = query_ioctl(phys_handle);
                    let _ = CloseHandle(phys_handle);
                    p
                } else {
                    None
                }
            } else {
                None
            }
        };

        let _ = CloseHandle(handle);
        let raw = raw?;

        let bytes_read = raw.bytes_read.quad_part.max(0) as u64;
        let bytes_written = raw.bytes_written.quad_part.max(0) as u64;
        let read_time_ms = nt_time_100ns(raw.read_time.quad_part) / 10_000;
        let write_time_ms = nt_time_100ns(raw.write_time.quad_part) / 10_000;
        let query_time_100ns = nt_time_100ns(raw.query_time.quad_part);
        let boot_time_100ns = boot_time_filetime_100ns();
        let idle_time_100ns = nt_time_100ns(raw.idle_time.quad_part);

        Some(DiskPerfData {
            reads: raw.read_count as u64,
            writes: raw.write_count as u64,
            sectors_read: bytes_read / SECTOR_SIZE,
            sectors_written: bytes_written / SECTOR_SIZE,
            time_reading_ms: read_time_ms,
            time_writing_ms: write_time_ms,
            queue_depth: raw.queue_depth as u64,
            time_in_progress_ms: query_time_100ns
                .saturating_sub(boot_time_100ns)
                .saturating_sub(idle_time_100ns)
                / 10_000,
            weighted_time_in_progress_ms: read_time_ms.saturating_add(write_time_ms),
        })
    }
}

pub fn collect_disks() -> Result<Vec<DiskSnapshot>> {
    let drives = drive_strings()?;
    let mut out = Vec::new();
    for drive in drives {
        let drive_w = wide_z(&drive);
        unsafe {
            if GetDriveTypeW(PCWSTR(drive_w.as_ptr())) != DRIVE_FIXED_VALUE {
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
            let perf = query_disk_performance(&drive);
            out.push(DiskSnapshot {
                name: drive.trim_end_matches('\\').to_string(),
                has_counters: perf.is_some(),
                reads: perf.as_ref().map(|v| v.reads).unwrap_or(0),
                writes: perf.as_ref().map(|v| v.writes).unwrap_or(0),
                sectors_read: perf.as_ref().map(|v| v.sectors_read).unwrap_or(0),
                sectors_written: perf.as_ref().map(|v| v.sectors_written).unwrap_or(0),
                time_reading_ms: perf.as_ref().map(|v| v.time_reading_ms).unwrap_or(0),
                time_writing_ms: perf.as_ref().map(|v| v.time_writing_ms).unwrap_or(0),
                in_progress: perf.as_ref().map(|v| v.queue_depth).unwrap_or(0),
                time_in_progress_ms: perf.as_ref().map(|v| v.time_in_progress_ms).unwrap_or(0),
                weighted_time_in_progress_ms: perf
                    .as_ref()
                    .map(|v| v.weighted_time_in_progress_ms)
                    .unwrap_or(0),
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
            let speed_mbps = row.ReceiveLinkSpeed.max(row.TransmitLinkSpeed) / 1_000_000;
            out.push(NetDevSnapshot {
                name,
                mtu: Some(row.Mtu as u64),
                speed_mbps: Some(speed_mbps),
                tx_queue_len: None,
                carrier_up: Some(row.OperStatus.0 == 1),
                rx_bytes: row.InOctets,
                rx_packets: row.InUcastPkts + row.InNUcastPkts,
                rx_errs: row.InErrors,
                rx_drop: row.InDiscards,
                rx_fifo: 0,
                rx_frame: 0,
                rx_compressed: 0,
                rx_multicast: row.InNUcastPkts,
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
    let boot_filetime = boot_time_filetime_100ns();

    loop {
        if offset + size_of::<SystemProcessInformation>() > buf.len() {
            break;
        }
        let spi = unsafe { &*(buf.as_ptr().add(offset) as *const SystemProcessInformation) };
        let pid = spi.unique_process_id.0 as usize as i32;
        let ppid = spi.inherited_from_unique_process_id.0 as usize as i32;
        let raw_create = nt_time_100ns(spi.create_time.quad_part);
        let start_time_ticks = if raw_create > boot_filetime {
            raw_create - boot_filetime
        } else {
            0
        };

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
            start_time_ticks,
            processor: None,
            rt_priority: None,
            policy: None,
            oom_score: None,
            fd_count: None,
            read_chars: Some(spi.read_transfer_count.quad_part.max(0) as u64),
            write_chars: Some(spi.write_transfer_count.quad_part.max(0) as u64),
            syscr: Some(spi.read_operation_count.quad_part.max(0) as u64),
            syscw: Some(spi.write_operation_count.quad_part.max(0) as u64),
            read_bytes: Some(spi.read_transfer_count.quad_part.max(0) as u64),
            write_bytes: Some(spi.write_transfer_count.quad_part.max(0) as u64),
            cancelled_write_bytes: None,
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
                    if let Some(full_path) = process_image_name(handle) {
                        process.comm = process_basename(&full_path).to_string();
                    }
                    if let Some((utime, stime, ctime)) = get_process_times_ticks_100ns(handle) {
                        process.utime_ticks = utime;
                        process.stime_ticks = stime;
                        process.start_time_ticks = if ctime > boot_filetime {
                            ctime - boot_filetime
                        } else {
                            0
                        };
                    }
                    if let Some(mem) = get_process_mem(handle) {
                        process.vm_rss_kib = Some((mem.WorkingSetSize as u64) / 1024);
                        process.vm_data_kib = Some((mem.PrivateUsage as u64) / 1024);
                        process.vm_swap_kib = Some((mem.PagefileUsage as u64) / 1024);
                        process.vm_hwm_kib = Some((mem.PeakWorkingSetSize as u64) / 1024);
                        process.rss_pages = (mem.WorkingSetSize as u64 / page) as i64;
                    }
                    if let Some(io) = get_process_io(handle) {
                        process.read_chars = Some(io.ReadTransferCount);
                        process.write_chars = Some(io.WriteTransferCount);
                        process.syscr = Some(io.ReadOperationCount);
                        process.syscw = Some(io.WriteOperationCount);
                        process.read_bytes = Some(io.ReadTransferCount);
                        process.write_bytes = Some(io.WriteTransferCount);
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
    debug!("wincollect: collect_snapshot start");
    let system = collect_system()?;
    debug!("wincollect: collect_system done");
    let memory = collect_memory()?;
    debug!("wincollect: collect_memory done");
    let load = collect_load(&system.cpu_total)?;
    debug!("wincollect: collect_load done");
    let disks = collect_disks()?;
    debug!(disk_count = disks.len(), "wincollect: collect_disks done");
    let net = collect_net()?;
    debug!(iface_count = net.len(), "wincollect: collect_net done");
    let vmstat = collect_vmstat();
    debug!(vmstat_keys = vmstat.len(), "wincollect: collect_vmstat done");
    let processes = if include_process_metrics {
        let p = collect_processes()?;
        debug!(process_count = p.len(), "wincollect: collect_processes done");
        p
    } else {
        Vec::new()
    };
    Ok(Snapshot {
        system,
        memory,
        load,
        pressure: BTreeMap::new(),
        pressure_totals_us: BTreeMap::new(),
        vmstat,
        interrupts: BTreeMap::new(),
        softirqs: BTreeMap::new(),
        net_snmp: BTreeMap::new(),
        sockets: BTreeMap::new(),
        softnet: Vec::new(),
        swaps: Vec::new(),
        mounts: Vec::new(),
        cpuinfo: Vec::new(),
        zoneinfo: BTreeMap::new(),
        buddyinfo: BTreeMap::new(),
        disks,
        net,
        processes,
    })
}
