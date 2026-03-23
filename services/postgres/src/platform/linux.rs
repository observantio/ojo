use crate::{PostgresConfig, PostgresSnapshot};

pub(crate) fn collect_snapshot(cfg: &PostgresConfig) -> PostgresSnapshot {
    super::common::collect_snapshot_impl(cfg, "psql")
}
