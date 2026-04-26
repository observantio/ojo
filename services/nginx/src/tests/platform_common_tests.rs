use super::{
    collect_snapshot_impl, parse_stub_status, run_with_timeout, run_with_timeout_using_waiter,
};
use crate::NginxConfig;
use std::process::Command;
use std::time::Duration;

#[test]
fn collect_snapshot_impl_returns_default_when_command_cannot_spawn() {
    let _guard = crate::test_support::env_guard();
    let previous = std::env::var("OJO_NGINX_STUB_STATUS").ok();
    std::env::remove_var("OJO_NGINX_STUB_STATUS");

    let cfg = NginxConfig {
        executable: "/definitely/missing/curl".to_string(),
        status_url: "http://127.0.0.1/nginx_status".to_string(),
    };
    let snap = collect_snapshot_impl(&cfg, "curl");
    assert!(!snap.available);
    assert_eq!(snap.requests_total, 0);

    if let Some(previous) = previous {
        std::env::set_var("OJO_NGINX_STUB_STATUS", previous);
    }
}

#[test]
fn collect_snapshot_impl_parses_stub_status_from_env_override() {
    let _guard = crate::test_support::env_guard();
    let previous = std::env::var("OJO_NGINX_STUB_STATUS").ok();
    std::env::set_var(
        "OJO_NGINX_STUB_STATUS",
        "Active connections: 291\nserver accepts handled requests\n 16630948 16630948 31070465\nReading: 6 Writing: 179 Waiting: 106\n",
    );

    let cfg = NginxConfig {
        executable: "/definitely/missing/curl".to_string(),
        status_url: "http://127.0.0.1/nginx_status".to_string(),
    };
    let snap = collect_snapshot_impl(&cfg, "curl");
    assert!(snap.available);
    assert!(snap.up);
    assert_eq!(snap.connections_active, 291);
    assert_eq!(snap.connections_reading, 6);
    assert_eq!(snap.connections_writing, 179);
    assert_eq!(snap.connections_waiting, 106);
    assert_eq!(snap.accepts_total, 16_630_948);
    assert_eq!(snap.handled_total, 16_630_948);
    assert_eq!(snap.requests_total, 31_070_465);

    if let Some(previous) = previous {
        std::env::set_var("OJO_NGINX_STUB_STATUS", previous);
    } else {
        std::env::remove_var("OJO_NGINX_STUB_STATUS");
    }
}

#[test]
fn collect_snapshot_impl_returns_default_for_invalid_status_text() {
    let snap = parse_stub_status("not nginx status");
    assert!(!snap.available);
}

#[test]
fn run_with_timeout_covers_success_timeout_and_wait_error() {
    let mut ok_cmd = Command::new("sh");
    ok_cmd.args(["-c", "printf 'ok'"]);
    let output = run_with_timeout(ok_cmd, Duration::from_secs(1)).expect("expected output");
    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), "ok");

    let mut slow_cmd = Command::new("sh");
    slow_cmd.args(["-c", "sleep 1"]);
    assert_eq!(run_with_timeout(slow_cmd, Duration::from_millis(10)), None);

    let mut err_cmd = Command::new("sh");
    err_cmd.args(["-c", "printf 'ok'"]);
    let errored = run_with_timeout_using_waiter(err_cmd, Duration::from_secs(1), |_child| {
        Err(std::io::Error::other("forced wait error"))
    });
    assert_eq!(errored, None);
}
