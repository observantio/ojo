#[cfg(target_os = "windows")]
mod windows;

#[cfg(not(target_os = "windows"))]
mod unix;

#[cfg(target_os = "windows")]
pub(crate) use windows::collect_snapshot;

#[cfg(not(target_os = "windows"))]
pub(crate) use unix::collect_snapshot;
