use crate::{WatchSource, WatchedFileConfig};

use crate::platform::{
    file_offsets, sanitize_watch_target_name, PlatformCollection, PlatformConfig,
};
use std::fs;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

fn offsets_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn reset_offsets() {
    file_offsets().lock().expect("offset lock").clear();
}

fn collect_for_test(cfg: &PlatformConfig) -> PlatformCollection {
    #[cfg(coverage)]
    {
        let watched = crate::platform::collect_watched_file_records(cfg);
        return PlatformCollection {
            snapshot: crate::platform::PlatformSnapshot {
                available: watched.active_targets > 0,
                process_logs_available: watched.process_logs_available,
                application_logs_available: watched.application_logs_available,
                file_watch_targets_active: watched.active_targets,
                collection_errors: watched.collection_errors,
                ..crate::platform::PlatformSnapshot::default()
            },
            records: watched.records,
        };
    }

    #[cfg(not(coverage))]
    {
        crate::platform::collect(cfg)
    }
}

fn unique_temp_path(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "ojo-syslog-platform-{name}-{}-{nanos}",
        std::process::id()
    ))
}

#[test]
fn sanitize_watch_target_name_normalizes_symbols() {
    assert_eq!(sanitize_watch_target_name("App/API Log"), "app_api_log");
    assert_eq!(sanitize_watch_target_name(""), "watch");

    let long = "A".repeat(100);
    let normalized = sanitize_watch_target_name(&long);
    assert_eq!(normalized.len(), 64);
}

#[test]
fn collect_groups_incremental_lines_from_watched_files() {
    let _guard = offsets_test_lock().lock().expect("offset test lock");
    reset_offsets();
    let watched = unique_temp_path("watch.log");
    fs::write(&watched, "line1\nline2\n").expect("seed file");

    let cfg = PlatformConfig {
        max_lines_per_source: 10,
        max_message_bytes: 256,
        watch_files: vec![WatchedFileConfig {
            name: "app-log".to_string(),
            path: watched.to_string_lossy().to_string(),
            source: WatchSource::Application,
        }],
    };

    let first = collect_for_test(&cfg);
    let first_watch_records = first
        .records
        .iter()
        .filter(|record| record.watch_target == "app-log")
        .collect::<Vec<_>>();
    assert_eq!(first_watch_records.len(), 1);
    assert_eq!(first_watch_records[0].body, "line1\nline2");
    assert_eq!(first.snapshot.file_watch_targets_active, 1);

    fs::write(&watched, "line1\nline2\nline3\n").expect("append file");
    let second = collect_for_test(&cfg);
    let second_watch_records = second
        .records
        .iter()
        .filter(|record| record.watch_target == "app-log")
        .collect::<Vec<_>>();
    assert_eq!(second_watch_records.len(), 1);
    assert_eq!(second_watch_records[0].body, "line3");

    fs::remove_file(&watched).expect("cleanup");
}

#[test]
fn collect_handles_missing_watch_files() {
    let _guard = offsets_test_lock().lock().expect("offset test lock");
    reset_offsets();
    let cfg = PlatformConfig {
        max_lines_per_source: 10,
        max_message_bytes: 256,
        watch_files: vec![WatchedFileConfig {
            name: "missing".to_string(),
            path: "/tmp/definitely-missing-syslog-watch.log".to_string(),
            source: WatchSource::Process,
        }],
    };

    let result = collect_for_test(&cfg);
    assert_eq!(result.snapshot.file_watch_targets_active, 0);
}

#[test]
fn collect_counts_error_when_watch_target_is_unreadable_directory() {
    let _guard = offsets_test_lock().lock().expect("offset test lock");
    reset_offsets();

    let dir = unique_temp_path("watch-dir");
    fs::create_dir_all(&dir).expect("mkdir");

    let cfg = PlatformConfig {
        max_lines_per_source: 10,
        max_message_bytes: 256,
        watch_files: vec![WatchedFileConfig {
            name: "dir-watch".to_string(),
            path: dir.to_string_lossy().to_string(),
            source: WatchSource::Application,
        }],
    };

    let result = collect_for_test(&cfg);
    assert!(result.snapshot.collection_errors >= 1);

    fs::remove_dir_all(&dir).expect("cleanup");
}

#[test]
fn collect_skips_watch_lines_that_sanitize_to_empty() {
    let _guard = offsets_test_lock().lock().expect("offset test lock");
    reset_offsets();

    let watched = unique_temp_path("watch-empty.log");
    fs::write(&watched, "\u{0007}\u{0008}\n").expect("seed file");

    let cfg = PlatformConfig {
        max_lines_per_source: 10,
        max_message_bytes: 256,
        watch_files: vec![WatchedFileConfig {
            name: "empty-line-watch".to_string(),
            path: watched.to_string_lossy().to_string(),
            source: WatchSource::Application,
        }],
    };

    let result = collect_for_test(&cfg);
    assert!(!result.records.iter().any(|record| {
        record.source == "application" && record.watch_target == "empty-line-watch"
    }));
    assert_eq!(result.snapshot.file_watch_targets_active, 0);

    fs::remove_file(&watched).expect("cleanup");
}

#[test]
fn collect_respects_process_source_and_line_limit() {
    let _guard = offsets_test_lock().lock().expect("offset test lock");
    reset_offsets();
    let watched = unique_temp_path("watch-process.log");
    fs::write(&watched, "alpha\nbeta\n").expect("seed file");

    let cfg = PlatformConfig {
        max_lines_per_source: 1,
        max_message_bytes: 256,
        watch_files: vec![WatchedFileConfig {
            name: "proc-log".to_string(),
            path: watched.to_string_lossy().to_string(),
            source: WatchSource::Process,
        }],
    };

    let first = collect_for_test(&cfg);
    assert!(first.records.iter().any(|r| r.source == "process"));
    assert_eq!(first.snapshot.file_watch_targets_active, 1);

    let second = collect_for_test(&cfg);
    assert!(!second.records.iter().any(|r| r.source == "process"));
    assert_eq!(second.snapshot.file_watch_targets_active, 0);

    fs::remove_file(&watched).expect("cleanup");
}
