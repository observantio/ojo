use super::{
    collect_snapshot_impl, parse_mysql_status_output, parse_u64, run_with_timeout,
    run_with_timeout_using_waiter,
};
use crate::MysqlConfig;
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
        "ojo-mysql-common-{name}-{}-{nanos}",
        std::process::id()
    ))
}

#[test]
fn parse_u64_handles_missing_and_invalid_values() {
    assert_eq!(parse_u64(None), 0);
    assert_eq!(parse_u64(Some(&"not-a-number".to_string())), 0);
    assert_eq!(parse_u64(Some(&" 42 ".to_string())), 42);
}

#[test]
fn collect_snapshot_impl_returns_default_when_command_cannot_spawn() {
    let cfg = MysqlConfig {
        executable: "/definitely/missing/mysql".to_string(),
        ..MysqlConfig::default()
    };
    let snap = collect_snapshot_impl(&cfg, "mysql");
    assert!(!snap.available);
    assert_eq!(snap.queries_total, 0);
}

#[test]
fn collect_snapshot_impl_covers_default_executable_and_all_optional_args() {
    let _guard = env_lock().lock().expect("env lock");
    let dir = unique_temp_dir("script");
    fs::create_dir_all(&dir).expect("mkdir");
    let script = dir.join("fake-mysql.sh");
    let marker = dir.join("args.txt");
    let env_marker = dir.join("env.txt");
    fs::write(
        &script,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > {}\nprintf '%s' \"$MYSQL_PWD\" > {}\ncat <<'OUT'\nThreads_connected\t1\nThreads_running\t2\nQueries\t3\nSlow_queries\t4\nBytes_received\t5\nBytes_sent\t6\nOUT\n",
            marker.to_string_lossy(),
            env_marker.to_string_lossy()
        ),
    )
    .expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&script).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script, perms).expect("chmod");
    }

    let cfg = MysqlConfig {
        executable: script.to_string_lossy().to_string(),
        host: Some("db.example".to_string()),
        port: Some(3307),
        user: Some("root".to_string()),
        password: Some("secret".to_string()),
        database: Some("app".to_string()),
    };
    let snap = collect_snapshot_impl(&cfg, "mysql");
    assert!(snap.available);
    assert_eq!(snap.connections, 1);
    assert_eq!(snap.bytes_sent_total, 6);

    let args = fs::read_to_string(&marker).expect("read args");
    assert!(args.contains("--batch"));
    assert!(args.contains("-h\ndb.example"));
    assert!(args.contains("-P\n3307"));
    assert!(args.contains("-u\nroot"));
    assert!(!args.contains("-psecret"));
    assert!(args.contains("-D\napp"));
    assert!(args.contains("-e"));
    let env_value = fs::read_to_string(&env_marker).expect("read env");
    assert_eq!(env_value, "secret");

    fs::remove_file(&script).expect("cleanup script");
    fs::remove_file(&marker).expect("cleanup marker");
    fs::remove_file(&env_marker).expect("cleanup env marker");
    fs::remove_dir_all(&dir).expect("cleanup dir");
}

#[test]
fn collect_snapshot_impl_returns_default_on_failed_exit_status() {
    let _guard = env_lock().lock().expect("env lock");
    let dir = unique_temp_dir("fail-script");
    fs::create_dir_all(&dir).expect("mkdir");
    let script = dir.join("fake-mysql-fail.sh");
    fs::write(&script, "#!/bin/sh\nprintf 'nope' 1>&2\nexit 7\n").expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&script).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script, perms).expect("chmod");
    }

    let cfg = MysqlConfig {
        executable: script.to_string_lossy().to_string(),
        ..MysqlConfig::default()
    };
    let snap = collect_snapshot_impl(&cfg, "mysql");
    assert!(!snap.available);

    fs::remove_file(&script).expect("cleanup script");
    fs::remove_dir_all(&dir).expect("cleanup dir");
}

#[test]
fn parse_mysql_status_output_parses_expected_tabular_output() {
    let text = "Threads_connected\t12\nThreads_running\t3\nQueries\t200\nSlow_queries\t4\nBytes_received\t500\nBytes_sent\t700\n";
    let snap = parse_mysql_status_output(text);

    assert!(snap.available);
    assert!(snap.up);
    assert_eq!(snap.connections, 12);
    assert_eq!(snap.threads_running, 3);
    assert_eq!(snap.queries_total, 200);
    assert_eq!(snap.slow_queries_total, 4);
    assert_eq!(snap.bytes_received_total, 500);
    assert_eq!(snap.bytes_sent_total, 700);
}

#[test]
fn parse_mysql_status_output_returns_default_for_empty_text() {
    let snap = parse_mysql_status_output("\n\n");
    assert!(!snap.available);
    assert_eq!(snap.queries_total, 0);
}

#[test]
fn parse_mysql_status_output_ignores_empty_keys_and_parses_valid_rows() {
    let text = "\t999\nThreads_connected\t7\n";
    let snap = parse_mysql_status_output(text);
    assert!(snap.available);
    assert_eq!(snap.connections, 7);
}

#[test]
fn run_with_timeout_covers_success_and_timeout_paths() {
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
