use crate::model::{
    CpuInfoSnapshot, CpuTimes, DiskSnapshot, LoadSnapshot, MemorySnapshot, MountSnapshot,
    NetDevSnapshot, ProcessSnapshot, Snapshot, SwapDeviceSnapshot, SystemSnapshot,
};
use anyhow::{anyhow, Context, Result};
use std::collections::BTreeMap;
use std::ffi::c_void;
use std::mem::size_of;
use std::ptr::null_mut;
use std::slice;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;
use tracing::{debug, warn};

use windows::core::{PCWSTR, PWSTR};
use windows::Win32::Foundation::{CloseHandle, GetLastError, FILETIME, HANDLE, NTSTATUS};
use windows::Win32::NetworkManagement::IpHelper::{
    FreeMibTable, GetIfTable2, GetIpStatistics, GetTcpStatistics, GetTcpTable2, GetUdpStatistics,
    GetUdpTable, MIB_IF_TABLE2, MIB_IPSTATS_LH, MIB_TCP_STATE, MIB_TCPROW2, MIB_TCPSTATS_LH,
    MIB_IF_TYPE_LOOPBACK, MIB_TCPTABLE2,
    MIB_TCP_STATE_CLOSE_WAIT, MIB_TCP_STATE_CLOSING, MIB_TCP_STATE_ESTAB,
    MIB_TCP_STATE_FIN_WAIT1, MIB_TCP_STATE_FIN_WAIT2, MIB_TCP_STATE_LAST_ACK,
    MIB_TCP_STATE_LISTEN, MIB_TCP_STATE_SYN_RCVD, MIB_TCP_STATE_SYN_SENT,
    MIB_TCP_STATE_TIME_WAIT, MIB_UDPSTATS, MIB_UDPTABLE,
};
use windows::Win32::Storage::FileSystem::{
    CreateFileW, GetDriveTypeW, GetLogicalDriveStringsW, GetVolumeInformationW,
    FILE_ATTRIBUTE_NORMAL, FILE_FLAG_NO_BUFFERING, FILE_GENERIC_READ, FILE_SHARE_READ,
    FILE_SHARE_WRITE, OPEN_EXISTING,
};
use windows::Win32::System::Ioctl::{
    IOCTL_STORAGE_QUERY_PROPERTY, STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR, STORAGE_PROPERTY_ID,
    STORAGE_PROPERTY_QUERY, STORAGE_QUERY_TYPE,
};
use windows::Win32::System::IO::DeviceIoControl;
use windows::Win32::System::ProcessStatus::{
    GetPerformanceInfo, GetProcessMemoryInfo, PERFORMANCE_INFORMATION, PROCESS_MEMORY_COUNTERS_EX,
};
use windows::Win32::System::Registry::{
    RegCloseKey, RegOpenKeyExW, RegQueryValueExW, HKEY, HKEY_LOCAL_MACHINE, KEY_READ, REG_DWORD,
    REG_SZ,
};
use windows::Win32::System::SystemInformation::{
    GetLogicalProcessorInformationEx, GetSystemInfo, GetSystemTimeAsFileTime, GetTickCount64,
    GlobalMemoryStatusEx, LOGICAL_PROCESSOR_RELATIONSHIP, MEMORYSTATUSEX,
    PROCESSOR_ARCHITECTURE_AMD64, PROCESSOR_ARCHITECTURE_ARM64, PROCESSOR_ARCHITECTURE_INTEL,
    RelationCache, SYSTEM_INFO, SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX,
};
use windows::Win32::System::WindowsProgramming::{
    DRIVE_CDROM, DRIVE_FIXED, DRIVE_RAMDISK, DRIVE_REMOTE, DRIVE_REMOVABLE,
};
use windows::Win32::System::Threading::{
    GetPriorityClass, GetProcessHandleCount, GetProcessIoCounters, GetProcessTimes, GetSystemTimes,
    IO_COUNTERS, OpenProcess, QueryFullProcessImageNameW, PROCESS_NAME_WIN32,
    PROCESS_QUERY_INFORMATION, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_VM_READ,
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
const SYSTEM_BASIC_INFORMATION_CLASS: u32 = 0;
const SYSTEM_PERFORMANCE_INFORMATION_CLASS: u32 = 2;
const SYSTEM_TIME_OF_DAY_INFORMATION_CLASS: u32 = 3;
const SYSTEM_PROCESS_INFORMATION_CLASS: u32 = 5;
const SYSTEM_PROCESSOR_PERFORMANCE_INFORMATION_CLASS: u32 = 8;
const SYSTEM_INTERRUPT_INFORMATION_CLASS: u32 = 23;
const IOCTL_DISK_PERFORMANCE: u32 = 0x0007_0020;
const WINDOWS_TO_UNIX_EPOCH_100NS: u64 = 116_444_736_000_000_000;
const SECTOR_SIZE: u64 = 512;
const STORAGE_PROPERTY_ID_SEEK_PENALTY: u32 = 7;

const THREAD_STATE_RUNNING: u32 = 2;
const THREAD_STATE_READY: u32 = 1;
const THREAD_STATE_WAIT: u32 = 5;

#[repr(C)]
#[derive(Clone, Copy)]
struct UnicodeString {
    length: u16,
    maximum_length: u16,
    buffer: PWSTR,
}

impl Default for UnicodeString {
    fn default() -> Self {
        Self { length: 0, maximum_length: 0, buffer: PWSTR::null() }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct LargeInteger {
    quad_part: i64,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct SystemBasicInformation {
    reserved: u32,
    timer_resolution: u32,
    page_size: u32,
    number_of_physical_pages: u32,
    lowest_physical_page_number: u32,
    highest_physical_page_number: u32,
    allocation_granularity: u32,
    minimum_user_mode_address: usize,
    maximum_user_mode_address: usize,
    active_processors_affinity_mask: usize,
    number_of_processors: u8,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct SystemTimeOfDayInformation {
    boot_time: LargeInteger,
    current_time: LargeInteger,
    time_zone_bias: LargeInteger,
    time_zone_id: u32,
    _reserved: u32,
    boot_timebias: LargeInteger,
    sleep_bias: LargeInteger,
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
struct ClientId {
    unique_process: HANDLE,
    unique_thread: HANDLE,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct SystemThreadInformation {
    kernel_time: LargeInteger,
    user_time: LargeInteger,
    create_time: LargeInteger,
    wait_time: u32,
    start_address: usize,
    client_id: ClientId,
    priority: i32,
    base_priority: i32,
    context_switches: u32,
    thread_state: u32,
    wait_reason: u32,
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

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct DeviceSeekPenaltyDescriptor {
    version: u32,
    size: u32,
    incurs_seek_penalty: u8,
}

struct DiskPerfData {
    reads: u64,
    writes: u64,
    bytes_read: u64,
    bytes_written: u64,
    time_reading_ms: u64,
    time_writing_ms: u64,
    queue_depth: u64,
    time_in_progress_ms: u64,
    weighted_time_in_progress_ms: u64,
}

struct ProcessThreadSummary {
    state: String,
    last_cpu: Option<i64>,
}

static DISK_COUNTER_WARNING_EMITTED: OnceLock<Mutex<bool>> = OnceLock::new();
static LOAD_SYNTH_WARNING_EMITTED: OnceLock<Mutex<bool>> = OnceLock::new();

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

fn nt_time_100ns(v: i64) -> u64 {
    if v <= 0 { 0 } else { v as u64 }
}

fn nt_success(status: NTSTATUS) -> bool {
    status.0 >= 0
}

fn page_size_from_nt() -> u64 {
    let mut buf = [0u8; size_of::<SystemBasicInformation>()];
    let mut ret_len = 0u32;
    let status = unsafe {
        NtQuerySystemInformation(
            SYSTEM_BASIC_INFORMATION_CLASS,
            buf.as_mut_ptr() as *mut c_void,
            buf.len() as u32,
            &mut ret_len,
        )
    };
    if nt_success(status) && ret_len >= size_of::<SystemBasicInformation>() as u32 {
        let info = unsafe { std::ptr::read_unaligned(buf.as_ptr() as *const SystemBasicInformation) };
        if info.page_size > 0 {
            return info.page_size as u64;
        }
    }
    unsafe {
        let mut sysinfo = SYSTEM_INFO::default();
        GetSystemInfo(&mut sysinfo);
        sysinfo.dwPageSize as u64
    }
}

fn cpu_count_from_nt() -> usize {
    let mut buf = [0u8; size_of::<SystemBasicInformation>()];
    let mut ret_len = 0u32;
    let status = unsafe {
        NtQuerySystemInformation(
            SYSTEM_BASIC_INFORMATION_CLASS,
            buf.as_mut_ptr() as *mut c_void,
            buf.len() as u32,
            &mut ret_len,
        )
    };
    if nt_success(status) && ret_len >= size_of::<SystemBasicInformation>() as u32 {
        let info = unsafe { std::ptr::read_unaligned(buf.as_ptr() as *const SystemBasicInformation) };
        if info.number_of_processors > 0 {
            return info.number_of_processors as usize;
        }
    }
    unsafe {
        let mut sysinfo = SYSTEM_INFO::default();
        GetSystemInfo(&mut sysinfo);
        sysinfo.dwNumberOfProcessors as usize
    }
}

fn boot_time_from_nt() -> Option<u64> {
    let mut buf = [0u8; size_of::<SystemTimeOfDayInformation>()];
    let mut ret_len = 0u32;
    let status = unsafe {
        NtQuerySystemInformation(
            SYSTEM_TIME_OF_DAY_INFORMATION_CLASS,
            buf.as_mut_ptr() as *mut c_void,
            buf.len() as u32,
            &mut ret_len,
        )
    };
    if nt_success(status) && ret_len >= 16 {
        let info =
            unsafe { std::ptr::read_unaligned(buf.as_ptr() as *const SystemTimeOfDayInformation) };
        let boot_100ns = nt_time_100ns(info.boot_time.quad_part);
        if boot_100ns > WINDOWS_TO_UNIX_EPOCH_100NS {
            return Some(filetime_100ns_to_unix_secs(boot_100ns));
        }
    }
    None
}

fn current_uptime_secs() -> f64 {
    unsafe { GetTickCount64() as f64 / 1000.0 }
}

fn boot_time_epoch_secs() -> u64 {
    if let Some(t) = boot_time_from_nt() {
        return t;
    }
    let now_100ns = filetime_now_100ns();
    let uptime_100ns = (current_uptime_secs() * 10_000_000.0) as u64;
    filetime_100ns_to_unix_secs(now_100ns.saturating_sub(uptime_100ns))
}

fn boot_time_filetime_100ns() -> u64 {
    if let Some(t) = boot_time_from_nt() {
        return t * 10_000_000 + WINDOWS_TO_UNIX_EPOCH_100NS;
    }
    let now = filetime_now_100ns();
    let uptime_100ns = (current_uptime_secs() * 10_000_000.0) as u64;
    now.saturating_sub(uptime_100ns)
}

fn process_basename(path: &str) -> &str {
    path.rsplit(['\\', '/']).next().unwrap_or(path)
}

fn reg_query_string(hkey: HKEY, value_name: &str) -> Option<String> {
    let value_w = wide_z(value_name);
    unsafe {
        let mut value_type = REG_SZ;
        let mut size = 0u32;
        if RegQueryValueExW(
            hkey,
            PCWSTR(value_w.as_ptr()),
            None,
            Some(&mut value_type),
            None,
            Some(&mut size),
        )
        .0
            != 0
            || size < 2
            || value_type != REG_SZ
        {
            return None;
        }
        let mut data = vec![0u8; size as usize];
        if RegQueryValueExW(
            hkey,
            PCWSTR(value_w.as_ptr()),
            None,
            Some(&mut value_type),
            Some(data.as_mut_ptr()),
            Some(&mut size),
        )
        .0
            != 0
            || value_type != REG_SZ
            || size < 2
        {
            return None;
        }
        let wlen = (size as usize / 2).saturating_sub(1);
        let slice = slice::from_raw_parts(data.as_ptr() as *const u16, wlen);
        Some(String::from_utf16_lossy(slice).trim().to_string())
    }
}

fn reg_query_dword(hkey: HKEY, value_name: &str) -> Option<u32> {
    let value_w = wide_z(value_name);
    unsafe {
        let mut value_type = REG_DWORD;
        let mut data = 0u32;
        let mut size = size_of::<u32>() as u32;
        if RegQueryValueExW(
            hkey,
            PCWSTR(value_w.as_ptr()),
            None,
            Some(&mut value_type),
            Some((&mut data as *mut u32).cast::<u8>()),
            Some(&mut size),
        )
        .0
            == 0
            && value_type == REG_DWORD
            && size == size_of::<u32>() as u32
        {
            Some(data)
        } else {
            None
        }
    }
}

fn cpu_metadata_from_registry() -> (Option<String>, Option<String>, Option<f64>) {
    let key_path = wide_z(r"HARDWARE\DESCRIPTION\System\CentralProcessor\0");
    let mut key = HKEY::default();
    let opened = unsafe {
        RegOpenKeyExW(
            HKEY_LOCAL_MACHINE,
            PCWSTR(key_path.as_ptr()),
            Some(0),
            KEY_READ,
            &mut key,
        )
        .0
            == 0
    };
    if !opened {
        return (None, None, None);
    }

    let vendor = reg_query_string(key, "VendorIdentifier");
    let model = reg_query_string(key, "ProcessorNameString");
    let mhz = reg_query_dword(key, "~MHz").map(|v| v as f64);

    let _ = unsafe { RegCloseKey(key) };
    (vendor, model, mhz)
}

fn cpu_cache_size_bytes() -> Option<u64> {
    let mut required = 0u32;
    let _ = unsafe {
        GetLogicalProcessorInformationEx(
            LOGICAL_PROCESSOR_RELATIONSHIP(RelationCache.0),
            None,
            &mut required,
        )
    };
    if required == 0 {
        return None;
    }

    let mut buf = vec![0u8; required as usize];
    unsafe {
        GetLogicalProcessorInformationEx(
            LOGICAL_PROCESSOR_RELATIONSHIP(RelationCache.0),
            Some(buf.as_mut_ptr() as *mut SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX),
            &mut required,
        )
        .ok()?;
    }

    let mut max_cache = 0u64;
    let mut offset = 0usize;
    while offset + size_of::<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX>() <= required as usize {
        let info = unsafe {
            &*(buf.as_ptr().add(offset) as *const SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX)
        };
        if info.Relationship == LOGICAL_PROCESSOR_RELATIONSHIP(RelationCache.0) {
            let cache = unsafe { info.Anonymous.Cache };
            max_cache = max_cache.max(cache.CacheSize as u64);
        }
        if info.Size == 0 {
            break;
        }
        offset = offset.saturating_add(info.Size as usize);
    }

    if max_cache > 0 { Some(max_cache) } else { None }
}

fn open_process_limited(pid: u32) -> Option<HANDLE> {
    unsafe {
        OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_VM_READ, false, pid)
            .ok()
            .or_else(|| OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, false, pid).ok())
    }
}

fn process_image_name(handle: HANDLE) -> Option<String> {
    let mut buf = vec![0u16; 32_768];
    let mut len = buf.len() as u32;
    unsafe {
        QueryFullProcessImageNameW(
            handle,
            PROCESS_NAME_WIN32,
            PWSTR(buf.as_mut_ptr()),
            &mut len,
        )
        .ok()?;
    }
    Some(String::from_utf16_lossy(&buf[..len as usize]))
}

fn get_process_times_100ns(handle: HANDLE) -> Option<(u64, u64, u64)> {
    unsafe {
        let mut ctime = FILETIME::default();
        let mut etime = FILETIME::default();
        let mut ktime = FILETIME::default();
        let mut utime = FILETIME::default();
        if GetProcessTimes(handle, &mut ctime, &mut etime, &mut ktime, &mut utime).is_err() {
            return None;
        }
        Some((
            filetime_to_u64(utime),
            filetime_to_u64(ktime),
            filetime_to_u64(ctime),
        ))
    }
}

fn get_process_mem(handle: HANDLE) -> Option<PROCESS_MEMORY_COUNTERS_EX> {
    let mut mem = PROCESS_MEMORY_COUNTERS_EX::default();
    mem.cb = size_of::<PROCESS_MEMORY_COUNTERS_EX>() as u32;
    unsafe {
        if GetProcessMemoryInfo(
            handle,
            &mut mem as *mut _ as *mut _,
            size_of::<PROCESS_MEMORY_COUNTERS_EX>() as u32,
        )
        .is_err()
        {
            return None;
        }
    }
    Some(mem)
}

fn get_process_io(handle: HANDLE) -> Option<IO_COUNTERS> {
    let mut io = IO_COUNTERS::default();
    unsafe {
        if GetProcessIoCounters(handle, &mut io).is_err() {
            return None;
        }
    }
    Some(io)
}

fn get_process_handle_count_safe(handle: HANDLE) -> Option<u64> {
    let mut count = 0u32;
    unsafe {
        if GetProcessHandleCount(handle, &mut count).is_err() {
            return None;
        }
    }
    Some(count as u64)
}

fn get_priority_class_safe(handle: HANDLE) -> Option<u64> {
    let pc = unsafe { GetPriorityClass(handle) };
    if pc == 0 { None } else { Some(pc as u64) }
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
            let actual = if ret_len > 0 && (ret_len as usize) <= buf.len() {
                ret_len as usize
            } else {
                buf.len()
            };
            buf.truncate(actual);
            return Ok(buf);
        }
        if status.0 == STATUS_INFO_LENGTH_MISMATCH {
            size = if ret_len > size {
                ret_len.saturating_add(4096)
            } else {
                size.saturating_mul(2)
            };
            if size > 256 * 1024 * 1024 {
                return Err(anyhow!("NtQuerySystemInformation({class}) buffer grew too large"));
            }
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

fn system_performance_info() -> Option<SystemPerformanceInformation> {
    let buf = query_system_information(SYSTEM_PERFORMANCE_INFORMATION_CLASS).ok()?;
    if buf.len() < size_of::<SystemPerformanceInformation>() {
        return None;
    }
    Some(unsafe { *(buf.as_ptr() as *const SystemPerformanceInformation) })
}

fn collect_vmstat(perf: Option<&SystemPerformanceInformation>) -> BTreeMap<String, i64> {
    let mut out = BTreeMap::new();
    let Some(p) = perf else { return out };
    out.insert("pgfault".to_string(), p.page_fault_count as i64);
    out.insert("pgmajfault".to_string(), p.page_read_io_count as i64);
    out.insert("pgpgin".to_string(), p.page_read_count as i64);
    out.insert(
        "pgpgout".to_string(),
        (p.dirty_pages_write_count as i64).saturating_add(p.mapped_pages_write_count as i64),
    );
    out.insert("paged_pool_pages".to_string(), p.paged_pool_pages as i64);
    out.insert("non_paged_pool_pages".to_string(), p.non_paged_pool_pages as i64);
    out.insert("system_calls".to_string(), p.system_calls as i64);
    out.insert("context_switches".to_string(), p.context_switches as i64);
    out
}

fn collect_net_snmp() -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    unsafe {
        let mut ip = MIB_IPSTATS_LH::default();
        if GetIpStatistics(&mut ip) == 0 {
            out.insert("Ip.InReceives".to_string(), ip.dwInReceives as u64);
            out.insert("Ip.InHdrErrors".to_string(), ip.dwInHdrErrors as u64);
            out.insert("Ip.InAddrErrors".to_string(), ip.dwInAddrErrors as u64);
            out.insert("Ip.ForwDatagrams".to_string(), ip.dwForwDatagrams as u64);
            out.insert("Ip.InUnknownProtos".to_string(), ip.dwInUnknownProtos as u64);
            out.insert("Ip.InDiscards".to_string(), ip.dwInDiscards as u64);
            out.insert("Ip.InDelivers".to_string(), ip.dwInDelivers as u64);
            out.insert("Ip.OutRequests".to_string(), ip.dwOutRequests as u64);
            out.insert("Ip.RoutingDiscards".to_string(), ip.dwRoutingDiscards as u64);
            out.insert("Ip.OutDiscards".to_string(), ip.dwOutDiscards as u64);
            out.insert("Ip.OutNoRoutes".to_string(), ip.dwOutNoRoutes as u64);
            out.insert("Ip.ReasmReqds".to_string(), ip.dwReasmReqds as u64);
            out.insert("Ip.ReasmOKs".to_string(), ip.dwReasmOks as u64);
            out.insert("Ip.ReasmFails".to_string(), ip.dwReasmFails as u64);
            out.insert("Ip.FragOKs".to_string(), ip.dwFragOks as u64);
            out.insert("Ip.FragFails".to_string(), ip.dwFragFails as u64);
            out.insert("Ip.FragCreates".to_string(), ip.dwFragCreates as u64);
        }

        let mut tcp = MIB_TCPSTATS_LH::default();
        if GetTcpStatistics(&mut tcp) == 0 {
            out.insert("Tcp.ActiveOpens".to_string(), tcp.dwActiveOpens as u64);
            out.insert("Tcp.PassiveOpens".to_string(), tcp.dwPassiveOpens as u64);
            out.insert("Tcp.AttemptFails".to_string(), tcp.dwAttemptFails as u64);
            out.insert("Tcp.EstabResets".to_string(), tcp.dwEstabResets as u64);
            out.insert("Tcp.CurrEstab".to_string(), tcp.dwCurrEstab as u64);
            out.insert("Tcp.InSegs".to_string(), tcp.dwInSegs as u64);
            out.insert("Tcp.OutSegs".to_string(), tcp.dwOutSegs as u64);
            out.insert("Tcp.RetransSegs".to_string(), tcp.dwRetransSegs as u64);
            out.insert("Tcp.InErrs".to_string(), tcp.dwInErrs as u64);
            out.insert("Tcp.OutRsts".to_string(), tcp.dwOutRsts as u64);
        }

        let mut udp = MIB_UDPSTATS::default();
        if GetUdpStatistics(&mut udp) == 0 {
            out.insert("Udp.InDatagrams".to_string(), udp.dwInDatagrams as u64);
            out.insert("Udp.NoPorts".to_string(), udp.dwNoPorts as u64);
            out.insert("Udp.InErrors".to_string(), udp.dwInErrors as u64);
            out.insert("Udp.OutDatagrams".to_string(), udp.dwOutDatagrams as u64);
            out.insert("Udp.NumAddrs".to_string(), udp.dwNumAddrs as u64);
        }
    }
    out
}

fn collect_socket_counts() -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    unsafe {
        let mut size = 0u32;
        GetTcpTable2(None, &mut size, false);
        if size > 0 {
            let mut buf = vec![0u8; size as usize];
            if GetTcpTable2(Some(buf.as_mut_ptr() as *mut MIB_TCPTABLE2), &mut size, false) == 0
            {
                let table_ptr = buf.as_ptr() as *const MIB_TCPTABLE2;
                let count = std::ptr::addr_of!((*table_ptr).dwNumEntries).read_unaligned() as usize;
                let rows_ptr = std::ptr::addr_of!((*table_ptr).table) as *const MIB_TCPROW2;
                let mut established = 0u64;
                let mut listen = 0u64;
                let mut time_wait = 0u64;
                let mut close_wait = 0u64;
                let mut syn_sent = 0u64;
                let mut syn_recv = 0u64;
                let mut fin_wait1 = 0u64;
                let mut fin_wait2 = 0u64;
                let mut closing = 0u64;
                let mut last_ack = 0u64;
                for i in 0..count {
                    let row = rows_ptr.add(i).read_unaligned();
                    match MIB_TCP_STATE(row.dwState as i32) {
                        s if s == MIB_TCP_STATE_ESTAB => established += 1,
                        s if s == MIB_TCP_STATE_LISTEN => listen += 1,
                        s if s == MIB_TCP_STATE_TIME_WAIT => time_wait += 1,
                        s if s == MIB_TCP_STATE_CLOSE_WAIT => close_wait += 1,
                        s if s == MIB_TCP_STATE_SYN_SENT => syn_sent += 1,
                        s if s == MIB_TCP_STATE_SYN_RCVD => syn_recv += 1,
                        s if s == MIB_TCP_STATE_FIN_WAIT1 => fin_wait1 += 1,
                        s if s == MIB_TCP_STATE_FIN_WAIT2 => fin_wait2 += 1,
                        s if s == MIB_TCP_STATE_CLOSING => closing += 1,
                        s if s == MIB_TCP_STATE_LAST_ACK => last_ack += 1,
                        _ => {}
                    }
                }
                out.insert("v4.tcp.inuse".to_string(), established + listen);
                out.insert("v4.tcp.established".to_string(), established);
                out.insert("v4.tcp.listen".to_string(), listen);
                out.insert("v4.tcp.time_wait".to_string(), time_wait);
                out.insert("v4.tcp.close_wait".to_string(), close_wait);
                out.insert("v4.tcp.syn_sent".to_string(), syn_sent);
                out.insert("v4.tcp.syn_recv".to_string(), syn_recv);
                out.insert("v4.tcp.fin_wait1".to_string(), fin_wait1);
                out.insert("v4.tcp.fin_wait2".to_string(), fin_wait2);
                out.insert("v4.tcp.closing".to_string(), closing);
                out.insert("v4.tcp.last_ack".to_string(), last_ack);
                out.insert("v4.tcp.alloc".to_string(), count as u64);
            }
        }

        let mut size = 0u32;
        GetUdpTable(None, &mut size, false);
        if size > 0 {
            let mut buf = vec![0u8; size as usize];
            if GetUdpTable(Some(buf.as_mut_ptr() as *mut MIB_UDPTABLE), &mut size, false) == 0 {
                let table_ptr = buf.as_ptr() as *const MIB_UDPTABLE;
                let count = std::ptr::addr_of!((*table_ptr).dwNumEntries).read_unaligned() as u64;
                out.insert("v4.udp.inuse".to_string(), count);
            }
        }
    }
    out
}

fn collect_interrupts_detail(per_cpu: &[CpuTimes]) -> BTreeMap<String, u64> {
    let _ = SYSTEM_INTERRUPT_INFORMATION_CLASS;
    let mut out = BTreeMap::new();
    for (cpu, times) in per_cpu.iter().enumerate() {
        out.insert(format!("total|{cpu}"), times.irq);
    }
    out
}

fn collect_softirqs_detail(per_cpu: &[CpuTimes]) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    for (cpu, times) in per_cpu.iter().enumerate() {
        out.insert(format!("total|{cpu}"), times.softirq);
    }
    out
}

fn extract_process_thread_summaries(buf: &[u8]) -> (u64, u32, BTreeMap<i32, ProcessThreadSummary>) {
    let mut count = 0u64;
    let mut procs_blocked = 0u32;
    let mut summaries: BTreeMap<i32, ProcessThreadSummary> = BTreeMap::new();
    let mut offset = 0usize;
    let spi_size = size_of::<SystemProcessInformation>();
    let sti_size = size_of::<SystemThreadInformation>();

    loop {
        if offset + spi_size > buf.len() {
            break;
        }
        let spi = unsafe { &*(buf.as_ptr().add(offset) as *const SystemProcessInformation) };
        let pid = spi.unique_process_id.0 as usize as i32;
        count += 1;

        let thread_count = spi.number_of_threads as usize;
        let threads_base = offset + spi_size;
        let mut any_running = false;
        let mut any_ready = false;
        let mut all_waiting = thread_count > 0;
        let mut any_blocked = false;

        for t in 0..thread_count {
            let t_off = threads_base + t * sti_size;
            if t_off + sti_size > buf.len() {
                break;
            }
            let ti = unsafe { &*(buf.as_ptr().add(t_off) as *const SystemThreadInformation) };
            match ti.thread_state {
                THREAD_STATE_RUNNING => {
                    any_running = true;
                    all_waiting = false;
                }
                THREAD_STATE_READY => {
                    any_ready = true;
                    all_waiting = false;
                }
                THREAD_STATE_WAIT => {
                    if ti.wait_reason == 0 || ti.wait_reason == 14 {
                        any_blocked = true;
                    }
                }
                _ => {
                    all_waiting = false;
                }
            }
        }

        if any_blocked && !any_running && !any_ready {
            procs_blocked += 1;
        }

        let state = if any_running || any_ready {
            "R".to_string()
        } else if all_waiting {
            "S".to_string()
        } else {
            "unknown".to_string()
        };

        summaries.insert(pid, ProcessThreadSummary { state, last_cpu: None });

        if spi.next_entry_offset == 0 {
            break;
        }
        offset += spi.next_entry_offset as usize;
    }

    (count.saturating_sub(1), procs_blocked, summaries)
}

fn query_seek_penalty(device_path: &str) -> Option<bool> {
    let path_w = wide_z(device_path);
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
        let mut query = STORAGE_PROPERTY_QUERY {
            PropertyId: STORAGE_PROPERTY_ID(STORAGE_PROPERTY_ID_SEEK_PENALTY as i32),
            QueryType: STORAGE_QUERY_TYPE(0),
            AdditionalParameters: [0],
        };
        let mut desc = DeviceSeekPenaltyDescriptor::default();
        let mut returned = 0u32;
        let ok = DeviceIoControl(
            handle,
            IOCTL_STORAGE_QUERY_PROPERTY,
            Some(&mut query as *mut _ as *mut c_void),
            size_of::<STORAGE_PROPERTY_QUERY>() as u32,
            Some(&mut desc as *mut _ as *mut c_void),
            size_of::<DeviceSeekPenaltyDescriptor>() as u32,
            Some(&mut returned),
            None,
        )
        .is_ok();
        let _ = CloseHandle(handle);
        if ok && returned >= size_of::<DeviceSeekPenaltyDescriptor>() as u32 {
            Some(desc.incurs_seek_penalty != 0)
        } else {
            None
        }
    }
}

fn query_storage_alignment(device_path: &str) -> (Option<u64>, Option<u64>, Option<bool>) {
    let rotational = query_seek_penalty(device_path);
    let path_w = wide_z(device_path);
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
            Err(_) => return (Some(512), Some(512), rotational),
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
        if ok && returned >= size_of::<STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR>() as u32 {
            let desc = &*(out.as_ptr() as *const STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR);
            let l = if desc.BytesPerLogicalSector > 0 { desc.BytesPerLogicalSector as u64 } else { 512 };
            let p = if desc.BytesPerPhysicalSector > 0 { desc.BytesPerPhysicalSector as u64 } else { l };
            (Some(l), Some(p), rotational)
        } else {
            (Some(512), Some(512), rotational)
        }
    }
}

fn open_storage_query_handle(path: &str) -> Option<HANDLE> {
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
                return Some(handle);
            }
        }
    }
    None
}

fn query_disk_performance_for_path(path: &str) -> Option<DiskPerfData> {
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

        let raw = try_ioctl_perf(handle)?;
        let _ = CloseHandle(handle);

        let boot_100ns = boot_time_filetime_100ns();
        let query_100ns = nt_time_100ns(raw.query_time.quad_part);
        let idle_100ns = nt_time_100ns(raw.idle_time.quad_part);
        let time_in_progress_ms =
            query_100ns.saturating_sub(boot_100ns).saturating_sub(idle_100ns) / 10_000;
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
        let entry = unsafe {
            &*(buf.as_ptr().add(i * entry_size) as *const SystemProcessorPerformanceInformation)
        };
        let idle = nt_time_100ns(entry.idle_time.quad_part);
        let kernel_total = nt_time_100ns(entry.kernel_time.quad_part);
        let user = nt_time_100ns(entry.user_time.quad_part);
        let dpc = nt_time_100ns(entry.dpc_time.quad_part);
        let irq = nt_time_100ns(entry.interrupt_time.quad_part);
        let system =
            kernel_total.saturating_sub(idle).saturating_sub(dpc).saturating_sub(irq);
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

fn compute_load_averages(cpu_total: &CpuTimes, entities: u32) -> (f64, f64, f64) {
    let current_busy = cpu_total.busy();
    let current_total = cpu_total.total();
    let now = Instant::now();
    let state = LOAD_AVG_STATE.get_or_init(|| Mutex::new(None));
    let mut guard = state.lock().expect("load avg mutex poisoned");

    let instant_load = |busy: u64, total: u64| -> f64 {
        if total == 0 { return 0.0; }
        (busy as f64 / total as f64 * entities as f64).clamp(0.0, entities as f64)
    };

    match *guard {
        None => {
            let load = instant_load(current_busy, current_total);
            *guard = Some(LoadAvgState {
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
        owned_buf = query_system_information(SYSTEM_PROCESS_INFORMATION_CLASS).unwrap_or_default();
        owned_buf.as_slice()
    };
    let (process_count, procs_blocked, summaries) = extract_process_thread_summaries(&proc_buf);
    let procs_running = summaries.values().filter(|s| s.state == "R").count() as u32;
    let context_switches = perf.as_ref().map(|p| p.context_switches as u64).unwrap_or(0);
    let softirqs_total = per_cpu.iter().map(|c| c.softirq).fold(0u64, u64::saturating_add);
    debug!("wincollect: collect_system done");
    Ok(SystemSnapshot {
        is_windows: true,
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
        per_cpu,
        cpu_cycle_utilization: None,
    })
}

pub fn collect_memory() -> Result<MemorySnapshot> {
    unsafe {
        let mut mem = MEMORYSTATUSEX::default();
        mem.dwLength = size_of::<MEMORYSTATUSEX>() as u32;
        GlobalMemoryStatusEx(&mut mem).ok().context("GlobalMemoryStatusEx failed")?;

        let page = page_size_from_nt();
        let total_phys = mem.ullTotalPhys;
        let avail_phys = mem.ullAvailPhys;
        let total_pagefile = mem.ullTotalPageFile;
        let avail_pagefile = mem.ullAvailPageFile;

        let (cached_bytes, commit_total, commit_limit, non_paged_pool, paged_pool, perf_avail_bytes) =
            match system_performance_info() {
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

        // On Windows, commit accounting is the best approximation for pagefile-backed swap.
        let mut swap_total = commit_limit.saturating_sub(total_phys);
        if swap_total == 0 {
            // Fallback for environments where commit limit isn't available.
            swap_total = total_pagefile.saturating_sub(total_phys);
        }

        let mut swap_used = commit_total.saturating_sub(total_phys);
        if swap_used == 0 && swap_total > 0 {
            // Heuristic fallback when commit counters are unavailable.
            let committed = total_pagefile.saturating_sub(avail_pagefile);
            let resident = total_phys.saturating_sub(avail_phys);
            swap_used = committed.saturating_sub(resident).min(swap_total);
        }
        let swap_avail = swap_total.saturating_sub(swap_used);

        Ok(MemorySnapshot {
            mem_total_bytes: total_phys,
            mem_free_bytes: avail_phys,
            mem_available_bytes: perf_avail_bytes.min(total_phys),
            buffers_bytes: 0,
            cached_bytes,
            active_bytes: used_phys.saturating_sub(cached_bytes),
            inactive_bytes: 0,
            anon_pages_bytes: 0,
            mapped_bytes: 0,
            shmem_bytes: 0,
            swap_total_bytes: swap_total,
            swap_free_bytes: swap_avail,
            swap_cached_bytes: 0,
            dirty_bytes: 0,
            writeback_bytes: 0,
            slab_bytes: non_paged_pool.saturating_add(paged_pool),
            sreclaimable_bytes: paged_pool,
            sunreclaim_bytes: non_paged_pool,
            page_tables_bytes: 0,
            committed_as_bytes: commit_total,
            commit_limit_bytes: commit_limit,
            kernel_stack_bytes: 0,
            hugepages_total: 0,
            hugepages_free: 0,
            hugepage_size_bytes: 0,
            anon_hugepages_bytes: 0,
        })
    }
}

pub fn collect_load(cpu_total: &CpuTimes) -> Result<LoadSnapshot> {
    let warning_once = LOAD_SYNTH_WARNING_EMITTED.get_or_init(|| Mutex::new(false));
    let mut guard = warning_once.lock().expect("load warning mutex poisoned");
    if !*guard {
        warn!(
            "wincollect: load.{one,five,fifteen} on Windows is synthesized from CPU busy-time EMA and is not Linux loadavg-equivalent."
        );
        *guard = true;
    }
    drop(guard);
    let entities = cpu_count_from_nt().max(1) as u32;
    let (one, five, fifteen) = compute_load_averages(cpu_total, entities);
    Ok(LoadSnapshot {
        one,
        five,
        fifteen,
        runnable: 0,
        entities,
        latest_pid: 0,
    })
}

fn drive_strings() -> Result<Vec<String>> {
    unsafe {
        let mut buf = vec![0u16; 1024];
        let len = GetLogicalDriveStringsW(Some(&mut buf)) as usize;
        if len == 0 {
            return Err(anyhow!("GetLogicalDriveStringsW failed: {}", GetLastError().0));
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

pub fn collect_disks() -> Result<Vec<DiskSnapshot>> {
    let mut out = Vec::new();
    for idx in 0u32..64 {
        let device_path = format!(r"\\.\PhysicalDrive{idx}");
        let handle = match open_storage_query_handle(&device_path) {
            Some(h) => h,
            None => continue,
        };
        let _ = unsafe { CloseHandle(handle) };

        let (logical, physical, rotational) = query_storage_alignment(&device_path);
        let perf = query_disk_performance_for_path(&device_path);
        if perf.is_none() {
            let warning_once = DISK_COUNTER_WARNING_EMITTED.get_or_init(|| Mutex::new(false));
            let mut guard = warning_once.lock().expect("disk warning mutex poisoned");
            if !*guard {
                warn!(
                    "wincollect: disk throughput/IOPS counters unavailable (IOCTL_DISK_PERFORMANCE failed). \
common causes are missing privileges or disabled Windows disk performance counters; try running elevated and `diskperf -y`."
                );
                *guard = true;
            }
        }
        let name = format!("PhysicalDrive{idx}");
        out.push(DiskSnapshot {
            name,
            has_counters: perf.is_some(),
            reads: perf.as_ref().map(|v| v.reads).unwrap_or(0),
            writes: perf.as_ref().map(|v| v.writes).unwrap_or(0),
            sectors_read: perf.as_ref().map(|v| v.bytes_read / SECTOR_SIZE.max(1)).unwrap_or(0),
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
            logical_block_size: logical,
            physical_block_size: physical,
            rotational,
        });
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
            let alias = wchar_array_to_string(&row.Alias);
            let desc = wchar_array_to_string(&row.Description);
            let name_lc = format!("{} {}", alias, desc).to_ascii_lowercase();
            // Skip filter/binding adapters that duplicate real NIC stats.
            if name_lc.contains("lightweight filter")
                || name_lc.contains("wfp ")
                || name_lc.contains("qos packet scheduler")
            {
                continue;
            }
            let is_up = row.OperStatus.0 == 1;
            let has_traffic = row.InOctets > 0
                || row.OutOctets > 0
                || row.InUcastPkts > 0
                || row.OutUcastPkts > 0;
            let is_loopback = row.Type == MIB_IF_TYPE_LOOPBACK;
            if !is_up && !has_traffic && !is_loopback {
                continue;
            }
            let name = if !alias.is_empty() { alias } else { desc };
            let speed_mbps = row.ReceiveLinkSpeed.max(row.TransmitLinkSpeed) / 1_000_000;
            out.push(NetDevSnapshot {
                name,
                mtu: Some(row.Mtu as u64),
                speed_mbps: if speed_mbps > 0 { Some(speed_mbps) } else { None },
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
        FreeMibTable(table as _);
        Ok(out)
    }
}

fn collect_processes_from_nt(
    open_handles: bool,
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
    let mut offset = 0usize;
    let page = page_size_from_nt().max(1);
    let boot_filetime = boot_time_filetime_100ns();
    let spi_size = size_of::<SystemProcessInformation>();

    loop {
        if offset + spi_size > buf.len() {
            break;
        }
        let spi = unsafe { &*(buf.as_ptr().add(offset) as *const SystemProcessInformation) };
        let pid = spi.unique_process_id.0 as usize as i32;
        let ppid = spi.inherited_from_unique_process_id.0 as usize as i32;
        let raw_create = nt_time_100ns(spi.create_time.quad_part);
        let start_time_ticks = raw_create.saturating_sub(boot_filetime);

        let comm_from_spi = {
            let n = utf16_from_unicode_string(&spi.image_name);
            if n.is_empty() {
                if pid == 0 { "System Idle Process".to_string() } else { "System".to_string() }
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
            utime_ticks: nt_time_100ns(spi.user_time.quad_part),
            stime_ticks: nt_time_100ns(spi.kernel_time.quad_part),
            start_time_ticks,
            processor: last_cpu,
            rt_priority: None,
            policy: None,
            oom_score: None,
            fd_count: Some(spi.handle_count as u64),
            read_chars: Some(nt_time_100ns(spi.read_transfer_count.quad_part)),
            write_chars: Some(nt_time_100ns(spi.write_transfer_count.quad_part)),
            syscr: Some(nt_time_100ns(spi.read_operation_count.quad_part)),
            syscw: Some(nt_time_100ns(spi.write_operation_count.quad_part)),
            read_bytes: Some(nt_time_100ns(spi.read_transfer_count.quad_part)),
            write_bytes: Some(nt_time_100ns(spi.write_transfer_count.quad_part)),
            cancelled_write_bytes: None,
            vm_size_kib: Some(spi.virtual_size as u64 / 1024),
            vm_rss_kib: Some(spi.working_set_size as u64 / 1024),
            vm_data_kib: Some(spi.private_page_count as u64 / 1024),
            vm_stack_kib: None,
            vm_exe_kib: None,
            vm_lib_kib: None,
            vm_swap_kib: Some(spi.pagefile_usage as u64 / 1024),
            vm_pte_kib: None,
            vm_hwm_kib: Some(spi.peak_working_set_size as u64 / 1024),
            voluntary_ctxt_switches: None,
            nonvoluntary_ctxt_switches: None,
        };

        if open_handles && pid > 4 {
            unsafe {
                if let Some(handle) = open_process_limited(pid as u32) {
                    if let Some(full_path) = process_image_name(handle) {
                        let base = process_basename(&full_path).to_string();
                        if !base.is_empty() {
                            process.comm = base;
                        }
                    }
                    if let Some((utime, stime, ctime)) = get_process_times_100ns(handle) {
                        process.utime_ticks = utime;
                        process.stime_ticks = stime;
                        process.start_time_ticks = ctime.saturating_sub(boot_filetime);
                    }
                    if let Some(mem) = get_process_mem(handle) {
                        process.rss_pages = (mem.WorkingSetSize as u64 / page) as i64;
                        process.vm_rss_kib = Some(mem.WorkingSetSize as u64 / 1024);
                        process.vm_data_kib = Some(mem.PrivateUsage as u64 / 1024);
                        process.vm_swap_kib = Some(mem.PagefileUsage as u64 / 1024);
                        process.vm_hwm_kib = Some(mem.PeakWorkingSetSize as u64 / 1024);
                    }
                    if let Some(io) = get_process_io(handle) {
                        process.read_chars = Some(io.ReadTransferCount);
                        process.write_chars = Some(io.WriteTransferCount);
                        process.syscr = Some(io.ReadOperationCount);
                        process.syscw = Some(io.WriteOperationCount);
                        process.read_bytes = Some(io.ReadTransferCount);
                        process.write_bytes = Some(io.WriteTransferCount);
                    }
                    if let Some(h) = get_process_handle_count_safe(handle) {
                        process.fd_count = Some(h);
                    }
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
    let Ok(drives) = drive_strings() else { return Vec::new() };
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

pub fn collect_swaps(memory: &MemorySnapshot) -> Vec<SwapDeviceSnapshot> {
    if memory.swap_total_bytes == 0 {
        return Vec::new();
    }
    vec![SwapDeviceSnapshot {
        device: "pagefile".to_string(),
        swap_type: "partition".to_string(),
        size_bytes: memory.swap_total_bytes,
        used_bytes: memory.swap_total_bytes.saturating_sub(memory.swap_free_bytes),
        priority: -1,
    }]
}

pub fn collect_snapshot(include_process_metrics: bool) -> Result<Snapshot> {
    debug!("wincollect: collect_snapshot start");
    let process_info_buf = if include_process_metrics {
        Some(query_system_information(SYSTEM_PROCESS_INFORMATION_CLASS)?)
    } else {
        None
    };

    let system = collect_system(process_info_buf.as_deref())?;
    debug!("wincollect: collect_system done");
    let memory = collect_memory()?;
    debug!("wincollect: collect_memory done");
    let mut load = collect_load(&system.cpu_total)?;
    load.runnable = system.procs_running;
    load.entities = if system.process_count > u32::MAX as u64 {
        u32::MAX
    } else {
        system.process_count as u32
    };
    debug!("wincollect: collect_load done");
    let disks = collect_disks()?;
    debug!(disk_count = disks.len(), "wincollect: collect_disks done");
    let net = collect_net()?;
    debug!(iface_count = net.len(), "wincollect: collect_net done");
    let perf = system_performance_info();
    let vmstat = collect_vmstat(perf.as_ref());
    debug!(vmstat_keys = vmstat.len(), "wincollect: collect_vmstat done");
    let net_snmp = collect_net_snmp();
    debug!(snmp_keys = net_snmp.len(), "wincollect: collect_net_snmp done");
    let sockets = collect_socket_counts();
    debug!(socket_keys = sockets.len(), "wincollect: collect_sockets done");
    let interrupts = collect_interrupts_detail(&system.per_cpu);
    let softirqs = collect_softirqs_detail(&system.per_cpu);
    let cpuinfo = collect_cpuinfo();
    let mounts = collect_mounts();
    let swaps = collect_swaps(&memory);
    let processes = if include_process_metrics {
        let p = collect_processes_from_nt(true, process_info_buf.as_deref())?;
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
        interrupts,
        softirqs,
        net_snmp,
        sockets,
        softnet: Vec::new(),
        swaps,
        mounts,
        cpuinfo,
        zoneinfo: BTreeMap::new(),
        buddyinfo: BTreeMap::new(),
        disks,
        net,
        processes,
    })
}
