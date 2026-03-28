#[cfg(not(coverage))]
mod common;

#[cfg(coverage)]
mod coverage;

#[cfg(all(not(coverage), target_os = "linux"))]
mod linux;
#[cfg(all(not(coverage), target_os = "windows"))]
mod windows;

#[cfg(coverage)]
pub(crate) use coverage::collect_snapshot;
#[cfg(all(not(coverage), target_os = "linux"))]
pub(crate) use linux::collect_snapshot;
#[cfg(all(not(coverage), target_os = "windows"))]
pub(crate) use windows::collect_snapshot;

#[cfg(all(not(coverage), not(any(target_os = "linux", target_os = "windows"))))]
pub(crate) fn collect_snapshot(_cfg: &crate::RedisConfig) -> crate::RedisSnapshot {
    crate::RedisSnapshot::default()
}
