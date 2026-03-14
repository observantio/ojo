use crate::model::Snapshot;
use anyhow::Result;

#[cfg(target_os = "windows")]
pub fn collect_snapshot(include_process_metrics: bool) -> Result<Snapshot> {
    crate::wincollect::collect_snapshot(include_process_metrics)
}

#[cfg(not(target_os = "windows"))]
pub fn collect_snapshot(include_process_metrics: bool) -> Result<Snapshot> {
    crate::linuxcollect::collect_snapshot(include_process_metrics)
}
