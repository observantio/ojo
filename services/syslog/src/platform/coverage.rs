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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_returns_expected_stub_snapshot_and_record() {
        let cfg = PlatformConfig {
            max_lines_per_source: 10,
            max_message_bytes: 256,
            watch_files: vec![],
        };
        let collected = collect(&cfg);
        assert!(collected.snapshot.available);
        assert!(collected.snapshot.journald_available);
        assert!(collected.snapshot.etw_available);
        assert!(collected.snapshot.dmesg_available);
        assert!(collected.snapshot.process_logs_available);
        assert!(collected.snapshot.application_logs_available);
        assert_eq!(collected.snapshot.file_watch_targets_active, 2);
        assert_eq!(collected.snapshot.collection_errors, 0);
        assert_eq!(collected.records.len(), 1);
        assert_eq!(collected.records[0].body, "coverage-log");
        assert_eq!(collected.records[0].source, "coverage");
    }
}
