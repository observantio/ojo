use super::{
    advance_export_state, compute_sleep_duration, handle_flush_event, has_flag, log_flush_result,
    make_stop_handler, ExportState, FlushEvent,
};
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn unique_temp_path(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("ojo-main-{name}-{}-{nanos}", std::process::id()))
}

#[test]
fn has_flag_detects_presence() {
    let args = vec![
        "ojo".to_string(),
        "--config".to_string(),
        "linux.yaml".to_string(),
        "--dump-snapshot".to_string(),
    ];
    assert!(has_flag(&args, "--dump-snapshot"));
    assert!(has_flag(&args, "--config"));
    assert!(!has_flag(&args, "--once"));
}

#[test]
fn export_state_transitions_cover_success_and_failure_paths() {
    assert_eq!(
        advance_export_state(ExportState::Pending, true),
        (ExportState::Connected, FlushEvent::Connected)
    );
    assert_eq!(
        advance_export_state(ExportState::Reconnecting, true),
        (ExportState::Connected, FlushEvent::Reconnected)
    );
    assert_eq!(
        advance_export_state(ExportState::Connected, true),
        (ExportState::Connected, FlushEvent::None)
    );

    assert_eq!(
        advance_export_state(ExportState::Connected, false),
        (ExportState::Reconnecting, FlushEvent::Reconnecting)
    );
    assert_eq!(
        advance_export_state(ExportState::Pending, false),
        (ExportState::Reconnecting, FlushEvent::StillUnavailable)
    );
    assert_eq!(
        advance_export_state(ExportState::Reconnecting, false),
        (ExportState::Reconnecting, FlushEvent::StillUnavailable)
    );
}

#[test]
fn compute_sleep_duration_only_sleeps_when_running_and_before_deadline() {
    assert_eq!(
        compute_sleep_duration(Duration::from_millis(100), Duration::from_secs(1), true),
        Some(Duration::from_millis(900))
    );
    assert_eq!(
        compute_sleep_duration(Duration::from_secs(1), Duration::from_secs(1), true),
        None
    );
    assert_eq!(
        compute_sleep_duration(Duration::from_secs(2), Duration::from_secs(1), true),
        None
    );
    assert_eq!(
        compute_sleep_duration(Duration::from_millis(100), Duration::from_secs(1), false),
        None
    );
}

#[test]
fn main_returns_error_when_config_missing() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::set_var("PROC_OTEL_CONFIG", "/definitely/missing/ojo.yaml");
    let result = super::main();
    assert!(result.is_err());
    std::env::remove_var("PROC_OTEL_CONFIG");
}

#[test]
fn main_runs_once_with_valid_config() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("valid-config.yaml");
    fs::write(
            &path,
            "service:\n  name: ojo-test\n  instance_id: ojo-test-01\ncollection:\n  poll_interval_secs: 1\n  include_process_metrics: false\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
        )
        .expect("write config");

    std::env::set_var("PROC_OTEL_CONFIG", &path);
    std::env::set_var("OJO_RUN_ONCE", "1");

    let result = super::main();
    assert!(result.is_ok(), "{result:?}");

    std::env::remove_var("PROC_OTEL_CONFIG");
    std::env::remove_var("OJO_RUN_ONCE");
    fs::remove_file(path).expect("cleanup config");
}

#[test]
fn main_runs_once_with_process_metrics_enabled() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("valid-config-procs.yaml");
    fs::write(
            &path,
            "service:\n  name: ojo-test-procs\n  instance_id: ojo-test-procs-01\ncollection:\n  poll_interval_secs: 1\n  include_process_metrics: true\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
        )
        .expect("write config");

    std::env::set_var("PROC_OTEL_CONFIG", &path);
    std::env::set_var("OJO_RUN_ONCE", "1");

    let result = super::main();
    assert!(result.is_ok(), "{result:?}");

    std::env::remove_var("PROC_OTEL_CONFIG");
    std::env::remove_var("OJO_RUN_ONCE");
    fs::remove_file(path).expect("cleanup config");
}

#[test]
fn flush_helpers_cover_all_event_paths() {
    let now = Instant::now();
    log_flush_result(now, true);
    log_flush_result(now, false);

    handle_flush_event(FlushEvent::Connected, None);
    handle_flush_event(FlushEvent::Reconnected, None);
    handle_flush_event(FlushEvent::None, None);
    handle_flush_event(FlushEvent::None, Some(&"err"));
    handle_flush_event(FlushEvent::Reconnecting, Some(&"err"));
    handle_flush_event(FlushEvent::StillUnavailable, Some(&"err"));
}

#[test]
fn stop_handler_sets_running_false() {
    let running = Arc::new(AtomicBool::new(true));
    let stop = make_stop_handler(Arc::clone(&running));
    stop();
    assert!(!running.load(Ordering::SeqCst));
}

#[test]
fn main_supports_test_iteration_cap_when_not_run_once() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("valid-config-sigint.yaml");
    fs::write(
            &path,
            "service:\n  name: ojo-test-sigint\n  instance_id: ojo-test-sigint-01\ncollection:\n  poll_interval_secs: 1\n  include_process_metrics: false\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
        )
        .expect("write config");

    std::env::set_var("PROC_OTEL_CONFIG", &path);
    std::env::remove_var("OJO_RUN_ONCE");
    std::env::set_var("OJO_TEST_MAX_ITERATIONS", "1");

    let result = super::main();
    assert!(result.is_ok(), "{result:?}");

    std::env::remove_var("PROC_OTEL_CONFIG");
    std::env::remove_var("OJO_TEST_MAX_ITERATIONS");
    fs::remove_file(path).expect("cleanup config");
}
