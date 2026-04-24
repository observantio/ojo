use crate::{WatchSource, WatchedFileConfig};

use crate::platform::{file_offsets, sanitize_watch_target_name, PlatformConfig};
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

fn offsets_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn reset_offsets() {
    file_offsets().lock().expect("offset lock").clear();
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
    let initial = (1..=20)
        .map(|index| format!("line-{index:02}-abcdefghij"))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    fs::write(&watched, &initial).expect("seed file");

    let cfg = PlatformConfig {
        max_lines_per_source: 10,
        max_message_bytes: 64,
        watch_files: vec![WatchedFileConfig {
            name: "app-log".to_string(),
            path: watched.to_string_lossy().to_string(),
            source: WatchSource::Application,
        }],
    };

    let first = crate::platform::collect_watched_file_records(&cfg);
    let first_watch_records = first
        .records
        .iter()
        .filter(|record| record.source == "application" && record.stream == "file")
        .collect::<Vec<_>>();
    assert_eq!(first_watch_records.len(), 1, "records: {:?}", first.records);
    assert!(first_watch_records[0].body.contains("line-01-abcdefghij"));
    assert_eq!(first.active_targets, 1);

    let mut append = OpenOptions::new()
        .append(true)
        .open(&watched)
        .expect("open append file");
    writeln!(append, "line-21-abcdefghij").expect("append line 21");
    writeln!(append, "line-22-abcdefghij").expect("append line 22");
    let second = crate::platform::collect_watched_file_records(&cfg);
    let second_watch_records = second
        .records
        .iter()
        .filter(|record| record.source == "application" && record.stream == "file")
        .collect::<Vec<_>>();
    assert_eq!(
        second_watch_records.len(),
        1,
        "records: {:?}",
        second.records
    );
    assert!(second_watch_records[0].body.contains("line-21-abcdefghij"));

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

    let result = crate::platform::collect_watched_file_records(&cfg);
    assert_eq!(result.active_targets, 0);
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

    let result = crate::platform::collect_watched_file_records(&cfg);
    assert!(result.collection_errors >= 1);

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

    let result = crate::platform::collect_watched_file_records(&cfg);
    assert!(!result
        .records
        .iter()
        .any(|record| { record.source == "application" && record.stream == "file" }));
    assert_eq!(result.active_targets, 0);

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

    let first = crate::platform::collect_watched_file_records(&cfg);
    assert!(first.records.iter().any(|r| r.source == "process"));
    assert_eq!(first.active_targets, 1);

    let second = crate::platform::collect_watched_file_records(&cfg);
    assert!(!second.records.iter().any(|r| r.source == "process"));
    assert_eq!(second.active_targets, 0);

    fs::remove_file(&watched).expect("cleanup");
}
