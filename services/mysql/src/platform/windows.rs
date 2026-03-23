use crate::{MysqlConfig, MysqlSnapshot};

pub(crate) fn collect_snapshot(cfg: &MysqlConfig) -> MysqlSnapshot {
    super::common::collect_snapshot_impl(cfg, "mysql.exe")
}
