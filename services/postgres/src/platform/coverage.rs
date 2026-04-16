#[cfg(target_os = "linux")]
pub(crate) fn collect_snapshot(cfg: &crate::PostgresConfig) -> crate::PostgresSnapshot {
    super::common::collect_snapshot_impl(cfg, "psql")
}

#[cfg(target_os = "windows")]
pub(crate) fn collect_snapshot(cfg: &crate::PostgresConfig) -> crate::PostgresSnapshot {
    super::common::collect_snapshot_impl(cfg, "psql.exe")
}
