use crate::{normalize_severity, now_unix_nanos, sanitize_ascii_line, LogRecord};

use super::WindowsSourceSnapshot;

pub(super) fn collect_windows(
    max_lines_per_source: usize,
    max_message_bytes: usize,
) -> (WindowsSourceSnapshot, Vec<LogRecord>) {
    let mut snapshot = WindowsSourceSnapshot::default();
    let mut records = Vec::new();

    let max_events_formatted = format!("/c:{}", max_lines_per_source);

    if let Some(output) = super::common::run_command_with_timeout(
        "wevtutil",
        &["qe", "System", "/f:text", "/rd:true", &max_events_formatted],
    ) {
        if output.status.success() {
            snapshot.etw_available = true;
            snapshot.application_logs_available = true;
            let text = String::from_utf8_lossy(&output.stdout);
            for line in text.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() || trimmed.starts_with("Event[") {
                    continue;
                }
                let body = sanitize_ascii_line(trimmed, max_message_bytes);
                if body.is_empty() {
                    continue;
                }
                let severity = if body.contains("Level: Error") {
                    "ERROR"
                } else if body.contains("Level: Warning") {
                    "WARN"
                } else {
                    "INFO"
                };
                records.push(LogRecord {
                    observed_time_unix_nano: now_unix_nanos(),
                    severity_text: normalize_severity(severity),
                    body,
                    source: "windows_eventlog".to_string(),
                    stream: "system".to_string(),
                    watch_target: "windows-system".to_string(),
                });
            }
        }
    }

    if let Some(output) = super::common::run_command_with_timeout(
        "wevtutil",
        &[
            "qe",
            "Application",
            "/f:text",
            "/rd:true",
            &max_events_formatted,
        ],
    ) {
        if output.status.success() {
            snapshot.etw_available = true;
            snapshot.application_logs_available = true;
            let text = String::from_utf8_lossy(&output.stdout);
            for line in text.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() || trimmed.starts_with("Event[") {
                    continue;
                }
                let body = sanitize_ascii_line(trimmed, max_message_bytes);
                if body.is_empty() {
                    continue;
                }
                let severity = if body.contains("Level: Error") {
                    "ERROR"
                } else if body.contains("Level: Warning") {
                    "WARN"
                } else {
                    "INFO"
                };
                records.push(LogRecord {
                    observed_time_unix_nano: now_unix_nanos(),
                    severity_text: normalize_severity(severity),
                    body,
                    source: "windows_eventlog".to_string(),
                    stream: "application".to_string(),
                    watch_target: "windows-application".to_string(),
                });
            }
        }
    }

    snapshot.available = snapshot.etw_available;
    (snapshot, records)
}
