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

include!("core.rs");
include!("snapshot_collector.rs");
include!("scheduler_collector.rs");
include!("slab_filesystem_collector.rs");
include!("cgroup_collector.rs");
include!("process_status_parser.rs");
include!("pressure_collector.rs");
include!("kernel_net_collector.rs");
include!("mounts_cpuinfo_collector.rs");
include!("system_memory_collector.rs");
include!("disk_net_collector.rs");
include!("process_collector.rs");
