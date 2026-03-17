use crate::model::Snapshot;
use anyhow::Result;

#[cfg(target_os = "windows")]
pub fn collect_snapshot(include_process_metrics: bool) -> Result<Snapshot> {
    crate::wincollect::collect_snapshot(include_process_metrics)
}

#[cfg(target_os = "solaris")]
pub fn collect_snapshot(include_process_metrics: bool) -> Result<Snapshot> {
    crate::solarcollect::collect_snapshot(include_process_metrics)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
pub fn collect_snapshot(include_process_metrics: bool) -> Result<Snapshot> {
    crate::linuxcollect::collect_snapshot(include_process_metrics)
}

#[cfg(not(any(
    target_os = "windows",
    target_os = "solaris",
    target_os = "linux",
    target_os = "android"
)))]
pub fn collect_snapshot(_include_process_metrics: bool) -> Result<Snapshot> {
    anyhow::bail!("unsupported target OS")
}
