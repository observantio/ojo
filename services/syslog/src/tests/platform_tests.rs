use crate::{WatchSource, WatchedFileConfig};

use crate::platform::{collect, sanitize_watch_target_name, PlatformConfig};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

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
}

#[test]
fn collect_reads_incremental_lines_from_watched_files() {
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

    let first = collect(&cfg);
    assert!(first.records.iter().any(|r| r.body.contains("line1")));
    assert_eq!(first.snapshot.file_watch_targets_active, 1);

    fs::write(&watched, "line1\nline2\nline3\n").expect("append file");
    let second = collect(&cfg);
    assert!(second.records.iter().any(|r| r.body.contains("line3")));

    fs::remove_file(&watched).expect("cleanup");
}

#[test]
fn collect_handles_missing_watch_files() {
    let cfg = PlatformConfig {
        max_lines_per_source: 10,
        max_message_bytes: 256,
        watch_files: vec![WatchedFileConfig {
            name: "missing".to_string(),
            path: "/tmp/definitely-missing-syslog-watch.log".to_string(),
            source: WatchSource::Process,
        }],
    };

    let result = collect(&cfg);
    assert_eq!(result.snapshot.file_watch_targets_active, 0);
}
