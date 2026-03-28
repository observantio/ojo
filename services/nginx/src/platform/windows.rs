use crate::{NginxConfig, NginxSnapshot};

pub(crate) fn collect_snapshot(cfg: &NginxConfig) -> NginxSnapshot {
    super::common::collect_snapshot_impl(cfg, "curl.exe")
}
