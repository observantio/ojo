use crate::LogRecord;

use super::{PlatformCollection, PlatformConfig, PlatformSnapshot};

pub(crate) fn collect(_cfg: &PlatformConfig) -> PlatformCollection {
    PlatformCollection {
        snapshot: PlatformSnapshot {
            available: true,
            journald_available: true,
            etw_available: true,
            dmesg_available: true,
            process_logs_available: true,
            application_logs_available: true,
            file_watch_targets_active: 2,
            collection_errors: 0,
        },
        records: vec![LogRecord {
            observed_time_unix_nano: 1,
            severity_text: "INFO".to_string(),
            body: "coverage-log".to_string(),
            source: "coverage".to_string(),
            stream: "test".to_string(),
            watch_target: "coverage".to_string(),
        }],
    }
}
