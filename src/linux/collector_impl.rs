use crate::model::{LoadSnapshot, MemorySnapshot, ProcessSnapshot, Snapshot, SystemSnapshot};
use anyhow::Result;
use std::collections::BTreeMap;

#[cfg(not(coverage))]
use crate::model::{
    CpuInfoSnapshot, CpuTimes, CpuTimesSeconds, DiskSnapshot, MountSnapshot, NetDevSnapshot,
    SoftnetCpuSnapshot, SwapDeviceSnapshot,
};
#[cfg(not(coverage))]
use procfs::{process::all_processes, Current, CurrentSI};
#[cfg(not(coverage))]
use std::collections::{HashMap, HashSet};
#[cfg(not(coverage))]
use std::ffi::CString;
#[cfg(not(coverage))]
use std::fs;
#[cfg(not(coverage))]
use std::os::unix::ffi::OsStrExt;
#[cfg(not(coverage))]
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
