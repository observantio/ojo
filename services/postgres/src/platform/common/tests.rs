use super::{
    collect_snapshot_impl, parse_postgres_tsv_output, parse_u64, run_with_timeout,
    run_with_timeout_using_waiter,
};
use crate::PostgresConfig;
use std::fs;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn unique_temp_dir(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "ojo-postgres-platform-{name}-{}-{nanos}",
        std::process::id()
    ))
}

#[test]
fn parse_u64_handles_invalid_and_trimmed_input() {
    assert_eq!(parse_u64("bad"), 0);
    assert_eq!(parse_u64(" 99 "), 99);
}

#[test]
fn collect_snapshot_impl_returns_default_when_command_cannot_spawn() {
    let cfg = PostgresConfig {
        executable: "/definitely/missing/psql".to_string(),
        ..PostgresConfig::default()
    };
    let snap = collect_snapshot_impl(&cfg, "psql");
    assert!(!snap.available);
    assert_eq!(snap.connections, 0);
}

#[test]
fn parse_postgres_tsv_output_parses_expected_tsv_line() {
    let snap = parse_postgres_tsv_output("7\t100\t4\t2\t20\t50\n");

    assert!(snap.available);
    assert!(snap.up);
    assert_eq!(snap.connections, 7);
    assert_eq!(snap.xact_commit_total, 100);
    assert_eq!(snap.xact_rollback_total, 4);
    assert_eq!(snap.deadlocks_total, 2);
    assert_eq!(snap.blks_read_total, 20);
    assert_eq!(snap.blks_hit_total, 50);
}

#[test]
fn parse_postgres_tsv_output_returns_default_for_short_lines() {
    let snap = parse_postgres_tsv_output("1\t2\n");
    assert!(!snap.available);
    assert_eq!(snap.connections, 0);
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

#[test]
fn collect_snapshot_impl_covers_default_executable_uri_and_failed_exit() {
    let dir = unique_temp_dir("psql-script");
    fs::create_dir_all(&dir).expect("mkdir");
    let script = dir.join("fake-psql.sh");
    fs::write(
            &script,
            "#!/bin/sh\nif [ \"$1\" = \"-At\" ]; then\n  printf '7\t100\t4\t2\t20\t50\\n'\n  exit 0\nfi\nexit 1\n",
        )
        .expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&script).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script, perms).expect("chmod");
    }

    let cfg = PostgresConfig {
        executable: "".to_string(),
        uri: Some("postgres://demo".to_string()),
    };
    let snap = collect_snapshot_impl(&cfg, script.to_string_lossy().as_ref());
    assert!(snap.available);
    assert_eq!(snap.connections, 7);

    let fail_cfg = PostgresConfig {
        executable: "sh".to_string(),
        uri: None,
    };
    let failed = collect_snapshot_impl(&fail_cfg, "psql");
    assert!(!failed.available);

    fs::remove_file(&script).expect("cleanup script");
    fs::remove_dir_all(&dir).expect("cleanup dir");
}
