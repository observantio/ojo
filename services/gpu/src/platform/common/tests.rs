use super::{collect_snapshot, run_with_timeout, run_with_timeout_using_waiter};
use std::fs;
use std::process::Command;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn unique_temp_dir(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("ojo-gpu-{name}-{}-{nanos}", std::process::id()))
}

#[test]
fn collect_snapshot_parses_fake_nvidia_smi_output() {
    let _guard = env_lock().lock().expect("env lock");
    let dir = unique_temp_dir("bin");
    fs::create_dir_all(&dir).expect("mkdir");
    let cmd = dir.join("nvidia-smi");
    fs::write(
        &cmd,
        "#!/bin/sh\nprintf 'RTX 4090, 50, 1024, 24576, 70, 250, 0x0\\n'\n",
    )
    .expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&cmd).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&cmd, perms).expect("chmod");
    }

    let old_path = std::env::var("PATH").unwrap_or_default();
    std::env::set_var("PATH", format!("{}:{}", dir.to_string_lossy(), old_path));

    let snap = collect_snapshot();
    assert!(snap.available);
    assert_eq!(snap.samples.len(), 1);
    assert_eq!(snap.samples[0].name, "RTX 4090");
    assert_eq!(snap.samples[0].util_ratio, 0.5);
    assert!(!snap.samples[0].throttled);

    std::env::set_var("PATH", old_path);
    fs::remove_file(&cmd).expect("cleanup script");
    fs::remove_dir_all(&dir).expect("cleanup dir");
}

#[test]
fn collect_snapshot_returns_default_when_nvidia_smi_missing() {
    let _guard = env_lock().lock().expect("env lock");
    let dir = unique_temp_dir("empty-bin");
    fs::create_dir_all(&dir).expect("mkdir");
    let old_path = std::env::var("PATH").unwrap_or_default();
    std::env::set_var("PATH", dir.to_string_lossy().to_string());

    let snap = collect_snapshot();
    assert!(!snap.available);
    assert!(snap.samples.is_empty());

    std::env::set_var("PATH", old_path);
    fs::remove_dir_all(&dir).expect("cleanup dir");
}

#[test]
fn collect_snapshot_handles_failed_and_malformed_output() {
    let _guard = env_lock().lock().expect("env lock");
    let dir = unique_temp_dir("bad");
    fs::create_dir_all(&dir).expect("mkdir");
    let cmd = dir.join("nvidia-smi");
    fs::write(
            &cmd,
            "#!/bin/sh\nprintf 'RTX 4090, short\n'\nprintf 'RTX 4080, 80, 2048, 16384, 65, 200, 0X1\n'\n",
        )
        .expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&cmd).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&cmd, perms).expect("chmod");
    }
    let old_path = std::env::var("PATH").unwrap_or_default();
    std::env::set_var("PATH", format!("{}:{}", dir.to_string_lossy(), old_path));

    let snap = collect_snapshot();
    assert!(snap.available);
    assert_eq!(snap.samples.len(), 1);
    assert!(snap.samples[0].throttled);

    std::env::set_var("PATH", old_path);
    fs::remove_file(&cmd).expect("cleanup script");
    fs::remove_dir_all(&dir).expect("cleanup dir");

    let dir = unique_temp_dir("fail");
    fs::create_dir_all(&dir).expect("mkdir");
    let cmd = dir.join("nvidia-smi");
    fs::write(&cmd, "#!/bin/sh\nprintf 'nope' 1>&2\nexit 2\n").expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&cmd).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&cmd, perms).expect("chmod");
    }
    let old_path = std::env::var("PATH").unwrap_or_default();
    std::env::set_var("PATH", format!("{}:{}", dir.to_string_lossy(), old_path));
    let snap = collect_snapshot();
    assert!(!snap.available);
    std::env::set_var("PATH", old_path);
    fs::remove_file(&cmd).expect("cleanup script");
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
