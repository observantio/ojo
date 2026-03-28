use super::{
    collect_snapshot, enrich_sample, resolve_summary_entry, run_with_timeout,
    run_with_timeout_using_waiter,
};
use crate::DockerSample;
use std::collections::BTreeMap;
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
    std::env::temp_dir().join(format!("ojo-docker-{name}-{}-{nanos}", std::process::id()))
}

#[test]
fn resolve_summary_entry_matches_exact_and_prefix_ids() {
    let mut by_id = BTreeMap::new();
    by_id.insert(
        "abcdef123456".to_string(),
        (
            "/web".to_string(),
            "nginx".to_string(),
            "running".to_string(),
        ),
    );

    let exact = resolve_summary_entry(&by_id, "abcdef123456");
    assert_eq!(exact, Some(("/web", "nginx", "running")));

    let prefix = resolve_summary_entry(&by_id, "abcdef");
    assert_eq!(prefix, Some(("/web", "nginx", "running")));

    let missing = resolve_summary_entry(&by_id, "xyz");
    assert_eq!(missing, None);

    let missing_long = resolve_summary_entry(&by_id, "zzzzzz");
    assert_eq!(missing_long, None);

    let reverse_prefix = resolve_summary_entry(&by_id, "abcdef1234567890");
    assert_eq!(reverse_prefix, Some(("/web", "nginx", "running")));
}

#[test]
fn collect_snapshot_keeps_sample_identity_when_summary_match_missing() {
    let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let dir = unique_temp_dir("no-summary-match");
    fs::create_dir_all(&dir).expect("mkdir");
    let docker = dir.join("docker");
    fs::write(
        &docker,
        "#!/bin/sh\nif [ \"$1\" = \"ps\" ]; then\n  printf '{\"ID\":\"deadbeef\",\"Names\":\"/db\",\"Image\":\"postgres\",\"State\":\"running\"}\\n'\nelif [ \"$1\" = \"stats\" ]; then\n  printf '{\"ID\":\"abcdef\",\"Name\":\"/web\",\"CPUPerc\":\"2%%\",\"MemUsage\":\"1MiB / 2MiB\",\"NetIO\":\"1kB / 2kB\",\"BlockIO\":\"3kB / 4kB\"}\\n'\nelse\n  exit 1\nfi\n",
    )
    .expect("write docker script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&docker).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&docker, perms).expect("chmod");
    }

    let old_path = std::env::var("PATH").unwrap_or_default();
    std::env::set_var("PATH", format!("{}:{}", dir.to_string_lossy(), old_path));

    let snap = collect_snapshot();
    assert!(snap.available);
    assert_eq!(snap.samples.len(), 1);
    assert_eq!(snap.samples[0].name, "web");
    assert!(snap.samples[0].image.is_empty());
    assert!(snap.samples[0].state.is_empty());

    std::env::set_var("PATH", old_path);
    fs::remove_file(&docker).expect("cleanup docker script");
    fs::remove_dir_all(&dir).expect("cleanup dir");
}

#[test]
fn collect_snapshot_parses_fake_docker_ps_and_stats_output() {
    let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let dir = unique_temp_dir("bin");
    fs::create_dir_all(&dir).expect("mkdir");
    let docker = dir.join("docker");
    fs::write(
            &docker,
            "#!/bin/sh\nif [ \"$1\" = \"ps\" ]; then\n  printf '{\"ID\":\"abcdef123456\",\"Names\":\"/web\",\"Image\":\"nginx:latest\",\"State\":\"running\"}\\n'\n  printf '{\"ID\":\"deadbeef\",\"Names\":\"/db\",\"Image\":\"postgres\",\"State\":\"exited\"}\\n'\nelif [ \"$1\" = \"stats\" ]; then\n  printf '{\"ID\":\"abcdef\",\"Name\":\"/web\",\"CPUPerc\":\"25%%\",\"MemUsage\":\"128MiB / 1GiB\",\"NetIO\":\"1MiB / 2MiB\",\"BlockIO\":\"3MiB / 4MiB\"}\\n'\nelse\n  exit 1\nfi\n",
        )
        .expect("write docker script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&docker).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&docker, perms).expect("chmod");
    }

    let old_path = std::env::var("PATH").unwrap_or_default();
    std::env::set_var("PATH", format!("{}:{}", dir.to_string_lossy(), old_path));

    let snap = collect_snapshot();
    assert!(snap.available);
    assert_eq!(snap.total, 2);
    assert_eq!(snap.running, 1);
    assert_eq!(snap.stopped, 1);
    assert_eq!(snap.samples.len(), 1);
    assert_eq!(snap.samples[0].name, "web");
    assert_eq!(snap.samples[0].image, "nginx:latest");
    assert_eq!(snap.samples[0].state, "running");

    std::env::set_var("PATH", old_path);
    fs::remove_file(&docker).expect("cleanup docker script");
    fs::remove_dir_all(&dir).expect("cleanup dir");
}

#[test]
fn collect_snapshot_returns_default_when_docker_missing() {
    let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let old_path = std::env::var("PATH").unwrap_or_default();
    std::env::set_var("PATH", "/definitely/missing");
    let snap = collect_snapshot();
    assert!(!snap.available);
    assert_eq!(snap.total, 0);
    std::env::set_var("PATH", old_path);
}

#[test]
fn collect_snapshot_handles_failed_commands_and_invalid_json_lines() {
    let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let dir = unique_temp_dir("bad-json");
    fs::create_dir_all(&dir).expect("mkdir");
    let docker = dir.join("docker");

    fs::write(
            &docker,
            "#!/bin/sh\nif [ \"$1\" = \"ps\" ]; then\n  printf '{bad-json}\n'\n  printf '{\"ID\":\"a1\",\"Names\":\"/ok\",\"Image\":\"img\",\"State\":\"running\"}\n'\n  printf '{\"Names\":\"/noid\",\"Image\":\"img2\",\"State\":\"exited\"}\n'\nelif [ \"$1\" = \"stats\" ]; then\n  printf '{bad-json}\n'\n  printf '{\"ID\":\"a1\",\"Name\":\"\",\"CPUPerc\":\"1%%\",\"MemUsage\":\"1MiB / 2MiB\",\"NetIO\":\"1kB / 2kB\",\"BlockIO\":\"3kB / 4kB\"}\n'\nelse\n  exit 1\nfi\n",
        )
        .expect("write docker script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&docker).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&docker, perms).expect("chmod");
    }

    let old_path = std::env::var("PATH").unwrap_or_default();
    std::env::set_var("PATH", format!("{}:{}", dir.to_string_lossy(), old_path));

    let snap = collect_snapshot();
    assert!(snap.available);
    assert_eq!(snap.total, 2);
    assert!(!snap.samples.is_empty());
    assert!(snap.samples.iter().any(|s| s.name == "ok"));

    std::env::set_var("PATH", old_path);
    fs::remove_file(&docker).expect("cleanup docker script");
    fs::remove_dir_all(&dir).expect("cleanup dir");

    let dir = unique_temp_dir("fail-cmd");
    fs::create_dir_all(&dir).expect("mkdir");
    let docker = dir.join("docker");
    fs::write(&docker, "#!/bin/sh\nprintf 'oops' 1>&2\nexit 5\n").expect("write fail script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&docker).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&docker, perms).expect("chmod");
    }
    let old_path = std::env::var("PATH").unwrap_or_default();
    std::env::set_var("PATH", format!("{}:{}", dir.to_string_lossy(), old_path));
    let snap = collect_snapshot();
    assert!(!snap.available);
    std::env::set_var("PATH", old_path);
    fs::remove_file(&docker).expect("cleanup fail script");
    fs::remove_dir_all(&dir).expect("cleanup dir");
}

#[test]
fn collect_snapshot_skips_blank_lines_in_ps_and_stats() {
    let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let dir = unique_temp_dir("blank-lines");
    fs::create_dir_all(&dir).expect("mkdir");
    let docker = dir.join("docker");
    fs::write(
        &docker,
        "#!/bin/sh\nif [ \"$1\" = \"ps\" ]; then\n  printf '\\n'\n  printf '{\"ID\":\"abcdef123456\",\"Names\":\"/web\",\"Image\":\"nginx:latest\",\"State\":\"running\"}\\n'\nelif [ \"$1\" = \"stats\" ]; then\n  printf '\\n'\n  printf '{\"ID\":\"abcdef\",\"Name\":\"/web\",\"CPUPerc\":\"10%%\",\"MemUsage\":\"8MiB / 128MiB\",\"NetIO\":\"1kB / 2kB\",\"BlockIO\":\"3kB / 4kB\"}\\n'\nelse\n  exit 1\nfi\n",
    )
    .expect("write docker script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&docker).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&docker, perms).expect("chmod");
    }

    let old_path = std::env::var("PATH").unwrap_or_default();
    std::env::set_var("PATH", format!("{}:{}", dir.to_string_lossy(), old_path));

    let snap = collect_snapshot();
    assert!(snap.available);
    assert_eq!(snap.total, 1);
    assert_eq!(snap.samples.len(), 1);

    std::env::set_var("PATH", old_path);
    fs::remove_file(&docker).expect("cleanup docker script");
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
    slow_cmd.args(["-c", "while :; do :; done"]);
    assert_eq!(run_with_timeout(slow_cmd, Duration::from_millis(10)), None);

    let mut err_cmd = Command::new("sh");
    err_cmd.args(["-c", "printf 'ok'"]);
    let errored = run_with_timeout_using_waiter(err_cmd, Duration::from_secs(1), |_child| {
        Err(std::io::Error::other("forced wait error"))
    });
    assert_eq!(errored, None);
}

#[test]
fn enrich_sample_keeps_existing_image_and_state() {
    let sample = DockerSample {
        id: "abc".to_string(),
        name: "/existing".to_string(),
        image: "sample-image".to_string(),
        state: "running".to_string(),
        ..DockerSample::default()
    };
    let enriched = enrich_sample(&sample, "/summary-name", "summary-image", "exited");
    assert_eq!(enriched.name, "existing");
    assert_eq!(enriched.image, "sample-image");
    assert_eq!(enriched.state, "running");
}
