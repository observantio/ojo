#[cfg(target_os = "windows")]
pub(crate) fn collect_snapshot(cfg: &crate::NfsClientConfig) -> crate::NfsClientSnapshot {
    super::windows::collect_snapshot(cfg)
}

#[cfg(not(target_os = "windows"))]
pub(crate) fn collect_snapshot(cfg: &crate::NfsClientConfig) -> crate::NfsClientSnapshot {
    super::linux::collect_snapshot(cfg)
}
