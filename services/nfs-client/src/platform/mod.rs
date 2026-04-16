#[cfg(coverage)]
mod coverage;

#[cfg(target_os = "windows")]
mod windows;

#[cfg(not(target_os = "windows"))]
mod linux;

#[cfg(coverage)]
pub(crate) use coverage::collect_snapshot;
#[cfg(all(not(coverage), target_os = "windows"))]
pub(crate) use windows::collect_snapshot;

#[cfg(all(not(coverage), not(target_os = "windows")))]
pub(crate) use linux::collect_snapshot;
