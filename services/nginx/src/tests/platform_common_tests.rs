use super::{collect_snapshot_impl, run_with_timeout, run_with_timeout_using_waiter};
use crate::NginxConfig;
use std::fs;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn unique_temp_dir(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "ojo-nginx-platform-{name}-{}-{nanos}",
        std::process::id()
    ))
}

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
fn collect_snapshot_impl_parses_stub_status_from_fake_script() {
    let _guard = crate::test_support::env_guard();
    let previous = std::env::var("OJO_NGINX_STUB_STATUS").ok();
    std::env::remove_var("OJO_NGINX_STUB_STATUS");

    let dir = unique_temp_dir("script");
    fs::create_dir_all(&dir).expect("mkdir");
    let script = dir.join("fake-curl.sh");
    fs::write(
        &script,
        "#!/bin/sh\ncat <<'OUT'\nActive connections: 291 \nserver accepts handled requests\n 16630948 16630948 31070465 \nReading: 6 Writing: 179 Waiting: 106 \nOUT\n",
    )
    .expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&script).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script, perms).expect("chmod");
    }

    let cfg = NginxConfig {
        executable: script.to_string_lossy().to_string(),
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
    }
    fs::remove_file(&script).expect("cleanup script");
    fs::remove_dir_all(&dir).expect("cleanup dir");
}

#[test]
fn collect_snapshot_impl_returns_default_for_invalid_status_text() {
    let _guard = crate::test_support::env_guard();
    let previous = std::env::var("OJO_NGINX_STUB_STATUS").ok();
    std::env::remove_var("OJO_NGINX_STUB_STATUS");

    let dir = unique_temp_dir("invalid");
    fs::create_dir_all(&dir).expect("mkdir");
    let script = dir.join("fake-curl-invalid.sh");
    fs::write(&script, "#!/bin/sh\nprintf 'not nginx status\n'\n").expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&script).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script, perms).expect("chmod");
    }

    let cfg = NginxConfig {
        executable: script.to_string_lossy().to_string(),
        status_url: "http://127.0.0.1/nginx_status".to_string(),
    };
    let snap = collect_snapshot_impl(&cfg, "curl");
    assert!(!snap.available);

    if let Some(previous) = previous {
        std::env::set_var("OJO_NGINX_STUB_STATUS", previous);
    }
    fs::remove_file(&script).expect("cleanup script");
    fs::remove_dir_all(&dir).expect("cleanup dir");
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
