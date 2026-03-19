use crate::model::{
    CpuInfoSnapshot, CpuTimes, CpuTimesSeconds, DiskSnapshot, DiskVolumeCorrelation, LoadSnapshot,
    MemorySnapshot, MountSnapshot, NetDevSnapshot, ProcessSnapshot, Snapshot, SwapDeviceSnapshot,
    SystemSnapshot, WindowsCommitSnapshot, WindowsLoadSnapshot, WindowsMemoryPoolsSnapshot,
    WindowsMemoryPressureSnapshot, WindowsMemorySnapshot, WindowsPagefileSnapshot, WindowsSnapshot,
    WindowsSyntheticLoadSnapshot,
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
    FreeMibTable, GetIfTable2, GetIpStatistics, GetTcp6Table2, GetTcpStatistics, GetTcpTable2,
    GetUdp6Table, GetUdpStatistics, GetUdpTable, MIB_IF_TABLE2, MIB_IF_TYPE_LOOPBACK,
    MIB_IPSTATS_LH, MIB_TCP6ROW2, MIB_TCP6TABLE2, MIB_TCPROW2, MIB_TCPSTATS_LH, MIB_TCPTABLE2,
    MIB_TCP_STATE, MIB_TCP_STATE_CLOSE_WAIT, MIB_TCP_STATE_CLOSING, MIB_TCP_STATE_ESTAB,
    MIB_TCP_STATE_FIN_WAIT1, MIB_TCP_STATE_FIN_WAIT2, MIB_TCP_STATE_LAST_ACK, MIB_TCP_STATE_LISTEN,
    MIB_TCP_STATE_SYN_RCVD, MIB_TCP_STATE_SYN_SENT, MIB_TCP_STATE_TIME_WAIT, MIB_UDP6TABLE,
    MIB_UDPSTATS, MIB_UDPTABLE,
};
use windows::Win32::Storage::FileSystem::{
    CreateFileW, GetDiskFreeSpaceExW, GetDriveTypeW, GetLogicalDriveStringsW,
    GetVolumeInformationW, GetVolumeNameForVolumeMountPointW, QueryDosDeviceW,
    FILE_ATTRIBUTE_NORMAL, FILE_FLAG_NO_BUFFERING, FILE_GENERIC_READ, FILE_SHARE_READ,
    FILE_SHARE_WRITE, OPEN_EXISTING,
};
use windows::Win32::System::Ioctl::{
    IOCTL_STORAGE_GET_DEVICE_NUMBER, IOCTL_STORAGE_QUERY_PROPERTY,
    STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR, STORAGE_DEVICE_NUMBER, STORAGE_PROPERTY_ID,
    STORAGE_PROPERTY_QUERY, STORAGE_QUERY_TYPE,
};
use windows::Win32::System::ProcessStatus::{
    GetPerformanceInfo, GetProcessMemoryInfo, PERFORMANCE_INFORMATION, PROCESS_MEMORY_COUNTERS_EX,
};
use windows::Win32::System::Registry::{
    RegCloseKey, RegOpenKeyExW, RegQueryValueExW, HKEY, HKEY_LOCAL_MACHINE, KEY_READ, REG_DWORD,
    REG_SZ,
};
use windows::Win32::System::SystemInformation::{
    GetLogicalProcessorInformationEx, GetSystemInfo, GetSystemTimeAsFileTime, GetTickCount64,
    GlobalMemoryStatusEx, RelationCache, LOGICAL_PROCESSOR_RELATIONSHIP, MEMORYSTATUSEX,
    PROCESSOR_ARCHITECTURE_AMD64, PROCESSOR_ARCHITECTURE_ARM64, PROCESSOR_ARCHITECTURE_INTEL,
    SYSTEM_INFO, SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX,
};
use windows::Win32::System::Threading::{
    GetNumaHighestNodeNumber, GetPriorityClass, GetProcessHandleCount, GetProcessIoCounters,
    GetProcessTimes, GetSystemTimes, OpenProcess, QueryFullProcessImageNameW, IO_COUNTERS,
    PROCESS_NAME_WIN32, PROCESS_QUERY_INFORMATION, PROCESS_QUERY_LIMITED_INFORMATION,
    PROCESS_VM_READ,
};
use windows::Win32::System::WindowsProgramming::{
    DRIVE_CDROM, DRIVE_FIXED, DRIVE_RAMDISK, DRIVE_REMOTE, DRIVE_REMOVABLE,
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

#[derive(Clone, Copy, Default)]
struct DiskStaticMeta {
    logical_block_size: Option<u64>,
    physical_block_size: Option<u64>,
    rotational: Option<bool>,
}

struct OwnedHandle(HANDLE);

impl OwnedHandle {
    fn as_raw(&self) -> HANDLE {
        self.0
    }
}

impl Drop for OwnedHandle {
    fn drop(&mut self) {
        unsafe {
            let _ = CloseHandle(self.0);
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ProcessMode {
    Fast,
    Detailed,
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

#[derive(Clone, Copy, Debug)]
struct WindowsPagingRateState {
    hard_fault_total: u64,
    page_reads_total: u64,
    page_writes_total: u64,
    last: Instant,
}

#[derive(Default)]
struct WinCollectState {
    load_avg: Option<LoadAvgState>,
    paging_rate: Option<WindowsPagingRateState>,
    disk_perf_unavailable: bool,
    warned_synth_load: bool,
    disk_static: BTreeMap<String, DiskStaticMeta>,
}

static WIN_COLLECT_STATE: OnceLock<Mutex<WinCollectState>> = OnceLock::new();

fn with_wincollect_state<T>(f: impl FnOnce(&mut WinCollectState) -> Result<T>) -> Result<T> {
    let lock = WIN_COLLECT_STATE.get_or_init(|| Mutex::new(WinCollectState::default()));
    let mut guard = lock
        .lock()
        .map_err(|_| anyhow!("wincollect global state lock poisoned"))?;
    f(&mut guard)
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

fn nt_time_100ns(v: i64) -> u64 {
    if v <= 0 {
        0
    } else {
        v as u64
    }
}

fn nt_success(status: NTSTATUS) -> bool {
    status.0 >= 0
}

fn page_size_from_nt() -> u64 {
    if let Ok(info) = query_nt_struct::<SystemBasicInformation>(SYSTEM_BASIC_INFORMATION_CLASS) {
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
    if let Ok(info) = query_nt_struct::<SystemBasicInformation>(SYSTEM_BASIC_INFORMATION_CLASS) {
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
    if let Ok(info) =
        query_nt_struct::<SystemTimeOfDayInformation>(SYSTEM_TIME_OF_DAY_INFORMATION_CLASS)
    {
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
        .0 != 0
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
        .0 != 0
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
        .0 == 0
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
        .0 == 0
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
        let info =
            match read_unaligned_struct::<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX>(&buf, offset) {
                Some(i) => i,
                None => break,
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

    if max_cache > 0 {
        Some(max_cache)
    } else {
        None
    }
}

fn open_process_limited(pid: u32) -> Option<OwnedHandle> {
    unsafe {
        OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_VM_READ,
            false,
            pid,
        )
        .ok()
        .map(OwnedHandle)
        .or_else(|| {
            OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, false, pid)
                .ok()
                .map(OwnedHandle)
        })
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
    if pc == 0 {
        None
    } else {
        Some(pc as u64)
    }
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
                return Err(anyhow!(
                    "NtQuerySystemInformation({class}) buffer grew too large"
                ));
            }
            continue;
        }
        return Err(anyhow!(
            "NtQuerySystemInformation({class}) failed: 0x{:08x}",
            status.0 as u32
        ));
    }
}

fn query_nt_struct<T: Copy>(class: u32) -> Result<T> {
    let buf = query_system_information(class)?;
    if buf.len() < size_of::<T>() {
        return Err(anyhow!(
            "NtQuerySystemInformation({class}) returned {} bytes, expected at least {}",
            buf.len(),
            size_of::<T>()
        ));
    }
    let ptr = buf.as_ptr() as *const T;
    Ok(unsafe { std::ptr::read_unaligned(ptr) })
}

fn read_unaligned_struct<T: Copy>(buf: &[u8], offset: usize) -> Option<T> {
    let size = size_of::<T>();
    if offset + size > buf.len() {
        return None;
    }
    let ptr = unsafe { buf.as_ptr().add(offset) as *const T };
    Some(unsafe { std::ptr::read_unaligned(ptr) })
}

pub trait NtListEntry {
    fn next_entry_offset(&self) -> u32;
}

impl NtListEntry for SystemProcessInformation {
    fn next_entry_offset(&self) -> u32 {
        self.next_entry_offset
    }
}

impl NtListEntry for SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX {
    fn next_entry_offset(&self) -> u32 {
        self.Size
    }
}

struct NtListIter<'a, T: NtListEntry + Copy> {
    buf: &'a [u8],
    offset: usize,
    _marker: std::marker::PhantomData<T>,
}

impl<'a, T: NtListEntry + Copy> Iterator for NtListIter<'a, T> {
    type Item = (usize, T);

    fn next(&mut self) -> Option<Self::Item> {
        let size = size_of::<T>();
        if self.offset + size > self.buf.len() {
            return None;
        }

        let value =
            unsafe { std::ptr::read_unaligned(self.buf.as_ptr().add(self.offset) as *const T) };
        let next_offset = value.next_entry_offset() as usize;

        let current_offset = self.offset;
        self.offset = if next_offset == 0 {
            self.buf.len()
        } else {
            self.offset.saturating_add(next_offset)
        };
        Some((current_offset, value))
    }
}

fn walk_nt_list<'a, T: NtListEntry + Copy + 'a>(
    buf: &'a [u8],
) -> impl Iterator<Item = (usize, T)> + 'a {
    NtListIter {
        buf,
        offset: 0,
        _marker: std::marker::PhantomData,
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
    query_nt_struct::<SystemPerformanceInformation>(SYSTEM_PERFORMANCE_INFORMATION_CLASS).ok()
}

