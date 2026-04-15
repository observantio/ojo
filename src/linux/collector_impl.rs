use crate::model::{
    CpuInfoSnapshot, CpuTimes, CpuTimesSeconds, DiskSnapshot, LoadSnapshot, MemorySnapshot,
    MountSnapshot, NetDevSnapshot, ProcessSnapshot, Snapshot, SoftnetCpuSnapshot,
    SwapDeviceSnapshot, SystemSnapshot,
};
use anyhow::Result;
use procfs::{process::all_processes, Current, CurrentSI};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::CString;
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

#[cfg(not(coverage))]
include!("core.rs");
#[cfg(not(coverage))]
include!("snapshot_collector.rs");
#[cfg(not(coverage))]
include!("scheduler_collector.rs");
#[cfg(not(coverage))]
include!("slab_filesystem_collector.rs");
#[cfg(not(coverage))]
include!("cgroup_collector.rs");
#[cfg(not(coverage))]
include!("process_status_parser.rs");
#[cfg(not(coverage))]
include!("pressure_collector.rs");
#[cfg(not(coverage))]
include!("kernel_net_collector.rs");
#[cfg(not(coverage))]
include!("mounts_cpuinfo_collector.rs");
#[cfg(not(coverage))]
include!("system_memory_collector.rs");
#[cfg(not(coverage))]
include!("disk_net_collector.rs");
#[cfg(not(coverage))]
include!("process_collector.rs");

#[cfg(coverage)]
include!("collector_impl_coverage.rs");
