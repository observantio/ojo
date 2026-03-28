use crate::{RedisConfig, RedisSnapshot};

pub(crate) fn collect_snapshot(cfg: &RedisConfig) -> RedisSnapshot {
    super::common::collect_snapshot_impl(cfg, "redis-cli.exe")
}
