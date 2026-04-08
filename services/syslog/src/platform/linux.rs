use crate::{normalize_severity, now_unix_nanos, sanitize_ascii_line, LogRecord};

use super::LinuxSourceSnapshot;

pub(super) fn collect_linux(
    max_lines_per_source: usize,
    max_message_bytes: usize,
) -> (LinuxSourceSnapshot, Vec<LogRecord>) {
    let mut snapshot = LinuxSourceSnapshot::default();
    let mut records = Vec::new();

    if let Some(output) = super::common::run_command_with_timeout(
        "journalctl",
        &[
            "-n",
            &max_lines_per_source.to_string(),
            "--no-pager",
            "--output=short-iso",
        ],
    ) {
        if output.status.success() {
            snapshot.journald_available = true;
            let text = String::from_utf8_lossy(&output.stdout);
            for line in text.lines().filter(|line| !line.trim().is_empty()) {
                let body = sanitize_ascii_line(line, max_message_bytes);
                if body.is_empty() {
                    continue;
                }
                records.push(LogRecord {
                    observed_time_unix_nano: now_unix_nanos(),
                    severity_text: "INFO".to_string(),
                    body,
                    source: "journald".to_string(),
                    stream: "system".to_string(),
                    watch_target: "journald".to_string(),
                });
            }
        }
    }

    if let Some(output) =
        super::common::run_command_with_timeout("dmesg", &["--color=never", "--ctime"])
    {
        if output.status.success() {
            snapshot.dmesg_available = true;
            let text = String::from_utf8_lossy(&output.stdout);
            let mut lines = text
                .lines()
                .filter(|line| !line.trim().is_empty())
                .collect::<Vec<_>>();
            if lines.len() > max_lines_per_source {
                let start = lines.len() - max_lines_per_source;
                lines = lines.split_off(start);
            }
            for line in lines {
                let lower = line.to_ascii_lowercase();
                let severity = if lower.contains("error") || lower.contains("fail") {
                    "ERROR"
                } else if lower.contains("warn") {
                    "WARN"
                } else if lower.contains("debug") {
                    "DEBUG"
                } else {
                    "INFO"
                };
                let body = sanitize_ascii_line(line, max_message_bytes);
                if body.is_empty() {
                    continue;
                }
                records.push(LogRecord {
                    observed_time_unix_nano: now_unix_nanos(),
                    severity_text: normalize_severity(severity),
                    body,
                    source: "kernel".to_string(),
                    stream: "dmesg".to_string(),
                    watch_target: "dmesg".to_string(),
                });
            }
        }
    }

    snapshot.available = snapshot.journald_available || snapshot.dmesg_available;
    (snapshot, records)
}
