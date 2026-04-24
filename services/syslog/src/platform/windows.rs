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
            records.extend(collect_eventlog_records(
                &text,
                max_message_bytes,
                "system",
                "windows-system",
            ));
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
            records.extend(collect_eventlog_records(
                &text,
                max_message_bytes,
                "application",
                "windows-application",
            ));
        }
    }

    snapshot.available = snapshot.etw_available;
    (snapshot, records)
}

fn collect_eventlog_records(
    text: &str,
    max_message_bytes: usize,
    stream: &str,
    watch_target: &str,
) -> Vec<LogRecord> {
    let mut records = Vec::new();
    let mut current_event_lines = Vec::new();
    let mut in_event = false;

    for raw_line in text.lines() {
        let trimmed = raw_line.trim();
        if trimmed.starts_with("Event[") {
            if in_event {
                push_event_record(
                    &current_event_lines,
                    max_message_bytes,
                    stream,
                    watch_target,
                    &mut records,
                );
                current_event_lines.clear();
            }
            in_event = true;
            current_event_lines.push(raw_line.to_string());
            continue;
        }

        if !in_event {
            continue;
        }

        current_event_lines.push(raw_line.to_string());
    }

    if in_event {
        push_event_record(
            &current_event_lines,
            max_message_bytes,
            stream,
            watch_target,
            &mut records,
        );
    }

    records
}

fn push_event_record(
    event_lines: &[String],
    max_message_bytes: usize,
    stream: &str,
    watch_target: &str,
    records: &mut Vec<LogRecord>,
) {
    if event_lines.is_empty() {
        return;
    }

    let body = sanitize_ascii_line(&event_lines.join("\n"), max_message_bytes);
    if body.is_empty() {
        return;
    }

    let severity = if event_lines.iter().any(|line| line.contains("Level: Error")) {
        "ERROR"
    } else if event_lines
        .iter()
        .any(|line| line.contains("Level: Warning"))
    {
        "WARN"
    } else {
        "INFO"
    };

    records.push(LogRecord {
        observed_time_unix_nano: now_unix_nanos(),
        severity_text: normalize_severity(severity),
        body,
        source: "windows_eventlog".to_string(),
        stream: stream.to_string(),
        watch_target: watch_target.to_string(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_eventlog_records_groups_event_blocks() {
        let text = concat!(
            "Event[0]:\n",
            "  Log Name: System\n",
            "  Level: Warning\n",
            "  Message: First line\n",
            "    Second line\n",
            "\n",
            "Event[1]:\n",
            "  Log Name: Application\n",
            "  Level: Error\n",
            "  Message: Third line\n",
        );

        let records = collect_eventlog_records(text, 512, "system", "windows-system");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].source, "windows_eventlog");
        assert_eq!(records[0].stream, "system");
        assert_eq!(records[0].watch_target, "windows-system");
        assert!(records[0].body.contains("Event[0]:"));
        assert!(records[0].body.contains("First line"));
        assert!(records[0].body.lines().count() >= 4);
        assert_eq!(records[0].severity_text, "WARN");
        assert_eq!(records[1].severity_text, "ERROR");
        assert!(records[1].body.contains("Event[1]:"));
        assert!(records[1].body.contains("Third line"));
    }
}
