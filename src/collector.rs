use crate::model::Snapshot;
use anyhow::Result;

#[cfg(target_os = "windows")]
pub fn collect_snapshot(include_process_metrics: bool) -> Result<Snapshot> {
    crate::windows::collect_snapshot(include_process_metrics)
}

#[cfg(target_os = "solaris")]
pub fn collect_snapshot(include_process_metrics: bool) -> Result<Snapshot> {
    crate::solaris::collect_snapshot(include_process_metrics)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
pub fn collect_snapshot(include_process_metrics: bool) -> Result<Snapshot> {
    crate::linux::collect_snapshot(include_process_metrics)
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

#[cfg(test)]
mod tests {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[test]
    fn collect_snapshot_smoke_linux_like() {
        let snap = super::collect_snapshot(false).expect("collect snapshot");
        assert!(!snap.system.os_type.trim().is_empty());
        assert!(snap.system.ticks_per_second > 0);
    }
}
