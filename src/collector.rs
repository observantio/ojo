use crate::config::HostType;
use crate::model::Snapshot;
use anyhow::Result;

#[cfg(target_os = "windows")]
pub fn collect_snapshot(include_process_metrics: bool, host_type: HostType) -> Result<Snapshot> {
    match host_type {
        HostType::Auto | HostType::Windows => crate::windows::collect_snapshot(include_process_metrics),
        HostType::Linux => anyhow::bail!(
            "configured host_type=linux is not supported on this build target (windows); use host_type=auto/windows"
        ),
    }
}

#[cfg(target_os = "solaris")]
pub fn collect_snapshot(include_process_metrics: bool, host_type: HostType) -> Result<Snapshot> {
    match host_type {
        HostType::Auto => crate::solaris::collect_snapshot(include_process_metrics),
        HostType::Linux | HostType::Windows => anyhow::bail!(
            "configured host_type={} is not supported on this build target (solaris)",
            host_type.as_str()
        ),
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
pub fn collect_snapshot(include_process_metrics: bool, host_type: HostType) -> Result<Snapshot> {
    match host_type {
        HostType::Auto | HostType::Linux => crate::linux::collect_snapshot(include_process_metrics),
        HostType::Windows => anyhow::bail!(
            "configured host_type=windows is not supported on this build target (linux/android); use host_type=auto/linux"
        ),
    }
}

#[cfg(not(any(
    target_os = "windows",
    target_os = "solaris",
    target_os = "linux",
    target_os = "android"
)))]
pub fn collect_snapshot(_include_process_metrics: bool, host_type: HostType) -> Result<Snapshot> {
    let _ = host_type;
    anyhow::bail!("unsupported target OS")
}

#[cfg(test)]
#[path = "tests/collector_tests.rs"]
mod tests;
