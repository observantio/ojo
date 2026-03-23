#[cfg(not(target_os = "windows"))]
mod linux;
#[cfg(target_os = "windows")]
mod windows;

#[cfg(not(target_os = "windows"))]
pub(crate) use linux::collect_snapshot;
#[cfg(target_os = "windows")]
pub(crate) use windows::collect_snapshot;
