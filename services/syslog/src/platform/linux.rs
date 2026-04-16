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
            records.extend(collect_journal_records(&text, max_message_bytes));
        }
    }

    if let Some(output) =
        super::common::run_command_with_timeout("dmesg", &["--color=never", "--ctime"])
    {
        if output.status.success() {
            snapshot.dmesg_available = true;
            let text = String::from_utf8_lossy(&output.stdout);
            records.extend(collect_dmesg_records(
                &text,
                max_lines_per_source,
                max_message_bytes,
            ));
        }
    }

    snapshot.available = snapshot.journald_available || snapshot.dmesg_available;
    (snapshot, records)
}

fn collect_journal_records(text: &str, max_message_bytes: usize) -> Vec<LogRecord> {
    let mut records = Vec::new();
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
    records
}

fn collect_dmesg_records(
    text: &str,
    max_lines_per_source: usize,
    max_message_bytes: usize,
) -> Vec<LogRecord> {
    let mut records = Vec::new();
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
    records
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_journal_records_skips_empty_and_sanitizes() {
        let text = "line-1\n\nline-2\u{2603}\n";
        let records = collect_journal_records(text, 16);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].source, "journald");
        assert!(records[1].body.contains('?'));
    }

    #[test]
    fn collect_dmesg_records_applies_tail_and_severity_mapping() {
        let text = "debug boot\nwarn disk\nerror fail\ninfo done\n";
        let records = collect_dmesg_records(text, 3, 128);
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].severity_text, "WARN");
        assert_eq!(records[1].severity_text, "ERROR");
        assert_eq!(records[2].severity_text, "INFO");
        assert!(records.iter().all(|r| r.source == "kernel"));
        assert!(records.iter().all(|r| r.stream == "dmesg"));
    }
}
