#[cfg(all(not(coverage), any(target_os = "linux", target_os = "windows")))]
mod common;

#[cfg(coverage)]
mod common;
#[cfg(coverage)]
mod coverage;

#[cfg(all(not(coverage), target_os = "linux"))]
mod linux;
#[cfg(all(not(coverage), target_os = "windows"))]
mod windows;

use crate::{now_unix_nanos, sanitize_ascii_line, LogRecord, WatchSource, WatchedFileConfig};
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

#[derive(Clone, Debug)]
pub(crate) struct PlatformConfig {
    pub(crate) max_lines_per_source: usize,
    pub(crate) max_message_bytes: usize,
    pub(crate) watch_files: Vec<WatchedFileConfig>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct PlatformSnapshot {
    pub(crate) available: bool,
    pub(crate) journald_available: bool,
    pub(crate) etw_available: bool,
    pub(crate) dmesg_available: bool,
    pub(crate) process_logs_available: bool,
    pub(crate) application_logs_available: bool,
    pub(crate) file_watch_targets_active: u64,
    pub(crate) collection_errors: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct PlatformCollection {
    pub(crate) snapshot: PlatformSnapshot,
    pub(crate) records: Vec<LogRecord>,
}

#[cfg(all(not(coverage), target_os = "linux"))]
#[derive(Clone, Copy, Debug, Default)]
struct LinuxSourceSnapshot {
    available: bool,
    journald_available: bool,
    dmesg_available: bool,
}

#[cfg(all(not(coverage), target_os = "windows"))]
#[derive(Clone, Copy, Debug, Default)]
struct WindowsSourceSnapshot {
    available: bool,
    etw_available: bool,
    application_logs_available: bool,
}

#[cfg(any(test, not(coverage)))]
fn file_offsets() -> &'static Mutex<BTreeMap<String, u64>> {
    static OFFSETS: OnceLock<Mutex<BTreeMap<String, u64>>> = OnceLock::new();
    OFFSETS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(coverage)]
pub(crate) fn collect(cfg: &PlatformConfig) -> PlatformCollection {
    coverage::collect(cfg)
}

#[cfg(all(not(coverage), target_os = "linux"))]
pub(crate) fn collect(cfg: &PlatformConfig) -> PlatformCollection {
    collect_linux(cfg)
}

#[cfg(all(not(coverage), target_os = "windows"))]
pub(crate) fn collect(cfg: &PlatformConfig) -> PlatformCollection {
    collect_windows(cfg)
}

#[cfg(all(not(coverage), not(any(target_os = "linux", target_os = "windows"))))]
pub(crate) fn collect(cfg: &PlatformConfig) -> PlatformCollection {
    collect_fallback(cfg)
}

#[cfg(all(not(coverage), target_os = "linux"))]
fn collect_linux(cfg: &PlatformConfig) -> PlatformCollection {
    let (linux_snap, mut records) =
        linux::collect_linux(cfg.max_lines_per_source, cfg.max_message_bytes);
    let watched = collect_watched_file_records(cfg);
    records.extend(watched.records);

    PlatformCollection {
        snapshot: PlatformSnapshot {
            available: linux_snap.available || watched.active_targets > 0,
            journald_available: linux_snap.journald_available,
            etw_available: false,
            dmesg_available: linux_snap.dmesg_available,
            process_logs_available: watched.process_logs_available,
            application_logs_available: watched.application_logs_available,
            file_watch_targets_active: watched.active_targets,
            collection_errors: watched.collection_errors,
        },
        records,
    }
}

#[cfg(all(not(coverage), target_os = "windows"))]
fn collect_windows(cfg: &PlatformConfig) -> PlatformCollection {
    let (win_snap, mut records) =
        windows::collect_windows(cfg.max_lines_per_source, cfg.max_message_bytes);
    let watched = collect_watched_file_records(cfg);
    records.extend(watched.records);

    PlatformCollection {
        snapshot: PlatformSnapshot {
            available: win_snap.available || watched.active_targets > 0,
            journald_available: false,
            etw_available: win_snap.etw_available,
            dmesg_available: false,
            process_logs_available: watched.process_logs_available,
            application_logs_available: win_snap.application_logs_available
                || watched.application_logs_available,
            file_watch_targets_active: watched.active_targets,
            collection_errors: watched.collection_errors,
        },
        records,
    }
}

#[cfg(all(not(coverage), not(any(target_os = "linux", target_os = "windows"))))]
fn collect_fallback(cfg: &PlatformConfig) -> PlatformCollection {
    let watched = collect_watched_file_records(cfg);
    PlatformCollection {
        snapshot: PlatformSnapshot {
            available: watched.active_targets > 0,
            journald_available: false,
            etw_available: false,
            dmesg_available: false,
            process_logs_available: watched.process_logs_available,
            application_logs_available: watched.application_logs_available,
            file_watch_targets_active: watched.active_targets,
            collection_errors: watched.collection_errors,
        },
        records: watched.records,
    }
}

#[cfg(any(test, not(coverage)))]
struct WatchedFileResult {
    records: Vec<LogRecord>,
    active_targets: u64,
    process_logs_available: bool,
    application_logs_available: bool,
    collection_errors: u64,
}

#[cfg(any(test, not(coverage)))]
fn collect_watched_file_records(cfg: &PlatformConfig) -> WatchedFileResult {
    let mut records = Vec::new();
    let mut active_targets = 0u64;
    let mut process_logs_available = false;
    let mut application_logs_available = false;
    let mut collection_errors = 0u64;

    let mut offsets = file_offsets()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    for watch in &cfg.watch_files {
        let path = Path::new(&watch.path);
        if !path.exists() {
            continue;
        }
        let content = match fs::read_to_string(path) {
            Ok(raw) => raw,
            Err(_) => {
                collection_errors = collection_errors.saturating_add(1);
                continue;
            }
        };

        let lines = content.lines().collect::<Vec<_>>();
        let consumed = offsets.get(&watch.path).copied().unwrap_or(0) as usize;
        let start = consumed.min(lines.len());
        let mut grouped_bodies = Vec::new();
        let mut current_body = String::new();

        for line in lines.iter().skip(start).take(cfg.max_lines_per_source) {
            let line_body = sanitize_ascii_line(line, cfg.max_message_bytes);
            if line_body.is_empty() {
                continue;
            }

            if current_body.is_empty() {
                current_body.push_str(&line_body);
                continue;
            }

            let next_len = current_body.len() + 1 + line_body.len();
            if next_len > cfg.max_message_bytes {
                grouped_bodies.push(std::mem::take(&mut current_body));
                current_body.push_str(&line_body);
            } else {
                current_body.push('\n');
                current_body.push_str(&line_body);
            }
        }

        if !current_body.is_empty() {
            grouped_bodies.push(current_body);
        }

        offsets.insert(watch.path.clone(), lines.len() as u64);
        if !grouped_bodies.is_empty() {
            let source = match watch.source {
                WatchSource::Application => "application",
                WatchSource::Process => "process",
            };
            let watch_target = sanitize_watch_target_name(&watch.name);
            for body in grouped_bodies {
                records.push(LogRecord {
                    observed_time_unix_nano: now_unix_nanos(),
                    severity_text: "INFO".to_string(),
                    body,
                    source: source.to_string(),
                    stream: "file".to_string(),
                    watch_target: watch_target.clone(),
                });
            }
            active_targets = active_targets.saturating_add(1);
            match watch.source {
                WatchSource::Application => application_logs_available = true,
                WatchSource::Process => process_logs_available = true,
            }
        }
    }

    WatchedFileResult {
        records,
        active_targets,
        process_logs_available,
        application_logs_available,
        collection_errors,
    }
}

#[cfg(any(test, not(coverage)))]
fn sanitize_watch_target_name(name: &str) -> String {
    let mut out = String::new();
    for ch in name.chars() {
        let mapped = match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '-' => Some(ch.to_ascii_lowercase()),
            _ => Some('_'),
        };
        if let Some(c) = mapped {
            out.push(c);
        }
        if out.len() >= 64 {
            break;
        }
    }
    if out.is_empty() {
        "watch".to_string()
    } else {
        out
    }
}

#[cfg(test)]
#[path = "../tests/platform_tests.rs"]
mod tests;
