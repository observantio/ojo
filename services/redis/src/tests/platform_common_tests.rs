use super::{collect_snapshot_impl, parse_u64, run_with_timeout, run_with_timeout_using_waiter};
use crate::RedisConfig;
use std::fs;
use std::process::Command;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn unique_temp_dir(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "ojo-redis-platform-{name}-{}-{nanos}",
        std::process::id()
    ))
}

#[test]
fn parse_u64_handles_missing_and_invalid_values() {
    assert_eq!(parse_u64(None), 0);
    assert_eq!(parse_u64(Some(&"bad".to_string())), 0);
    assert_eq!(parse_u64(Some(&" 42 ".to_string())), 42);
}

#[test]
fn collect_snapshot_impl_returns_default_when_command_cannot_spawn() {
    let cfg = RedisConfig {
        executable: "/definitely/missing/redis-cli".to_string(),
        ..RedisConfig::default()
    };
    let snap = collect_snapshot_impl(&cfg, "redis-cli");
    assert!(!snap.available);
}

#[test]
fn collect_snapshot_impl_parses_info_from_fake_script() {
    let _guard = env_lock().lock().expect("env lock");
    let dir = unique_temp_dir("script");
    fs::create_dir_all(&dir).expect("mkdir");
    let script = dir.join("fake-redis.sh");
    fs::write(
        &script,
        "#!/bin/sh\ncat <<'OUT'\n# Server\nuptime_in_seconds:120\n# Clients\nconnected_clients:20\nblocked_clients:1\n# Memory\nused_memory:1024\nmaxmemory:4096\n# Stats\ntotal_connections_received:30\ntotal_commands_processed:300\nkeyspace_hits:70\nkeyspace_misses:30\nexpired_keys:5\nevicted_keys:2\nOUT\n",
    )
    .expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&script).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script, perms).expect("chmod");
    }

    let cfg = RedisConfig {
        executable: script.to_string_lossy().to_string(),
        host: Some("127.0.0.1".to_string()),
        port: Some(6379),
        username: Some("default".to_string()),
        password: Some("secret".to_string()),
    };
    let snap = collect_snapshot_impl(&cfg, "redis-cli");
    assert!(snap.available);
    assert!(snap.up);
    assert_eq!(snap.connected_clients, 20);
    assert_eq!(snap.blocked_clients, 1);
    assert_eq!(snap.memory_used_bytes, 1024);
    assert_eq!(snap.memory_max_bytes, 4096);
    assert_eq!(snap.uptime_seconds, 120);
    assert_eq!(snap.commands_processed_total, 300);
    assert_eq!(snap.connections_received_total, 30);
    assert_eq!(snap.keyspace_hits_total, 70);
    assert_eq!(snap.keyspace_misses_total, 30);
    assert_eq!(snap.expired_keys_total, 5);
    assert_eq!(snap.evicted_keys_total, 2);

    fs::remove_file(&script).expect("cleanup script");
    fs::remove_dir_all(&dir).expect("cleanup dir");
}

#[test]
fn collect_snapshot_impl_returns_default_for_invalid_info() {
    let _guard = env_lock().lock().expect("env lock");
    let dir = unique_temp_dir("invalid");
    fs::create_dir_all(&dir).expect("mkdir");
    let script = dir.join("fake-redis-invalid.sh");
    fs::write(&script, "#!/bin/sh\nprintf 'junk\n'\n").expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&script).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script, perms).expect("chmod");
    }

    let cfg = RedisConfig {
        executable: script.to_string_lossy().to_string(),
        ..RedisConfig::default()
    };
    let snap = collect_snapshot_impl(&cfg, "redis-cli");
    assert!(!snap.available);

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
