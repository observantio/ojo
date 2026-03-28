use crate::{DockerSample, DockerSnapshot};
use serde_json::Value;
use std::collections::BTreeMap;
use std::process::Child;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tracing::warn;

const CMD_TIMEOUT: Duration = Duration::from_secs(15);

pub(crate) fn collect_snapshot() -> DockerSnapshot {
    let summary = docker_ps_summary();
    let stats = docker_stats();
    let mut snapshot = DockerSnapshot {
        available: summary.available,
        total: summary.total,
        running: summary.running,
        stopped: summary.stopped,
        samples: Vec::new(),
    };
    if !snapshot.available {
        return snapshot;
    }
    for sample in &stats {
        let (name, image, state) =
            resolve_summary_entry(&summary.by_id, &sample.id).unwrap_or(("", "", ""));
        let enriched = enrich_sample(sample, name, image, state);
        snapshot.samples.push(enriched);
    }
    snapshot
}

fn enrich_sample(sample: &DockerSample, name: &str, image: &str, state: &str) -> DockerSample {
    let mut enriched = sample.clone();
    if enriched.name.is_empty() {
        enriched.name = name.trim_start_matches('/').to_string();
    } else {
        enriched.name = enriched.name.trim_start_matches('/').to_string();
    }
    if enriched.image.is_empty() {
        enriched.image = image.to_string();
    }
    if enriched.state.is_empty() {
        enriched.state = state.to_string();
    }
    enriched
}

fn resolve_summary_entry<'a>(
    by_id: &'a BTreeMap<String, (String, String, String)>,
    id: &str,
) -> Option<(&'a str, &'a str, &'a str)> {
    if let Some((name, image, state)) = by_id.get(id) {
        return Some((name.as_str(), image.as_str(), state.as_str()));
    }
    if id.len() >= 6 {
        return by_id.iter().find_map(|(candidate_id, tuple)| {
            if candidate_id.starts_with(id) || id.starts_with(candidate_id) {
                return Some((tuple.0.as_str(), tuple.1.as_str(), tuple.2.as_str()));
            }
            None
        });
    }
    None
}

#[derive(Clone, Debug, Default)]
struct DockerSummary {
    available: bool,
    total: u64,
    running: u64,
    stopped: u64,
    by_id: BTreeMap<String, (String, String, String)>,
}

fn docker_ps_summary() -> DockerSummary {
    let mut cmd = Command::new("docker");
    cmd.args(["ps", "-a", "--no-trunc", "--format", "{{json .}}"]);
    let Some(output) = run_with_timeout(cmd, CMD_TIMEOUT) else {
        return DockerSummary::default();
    };
    if !output.status.success() {
        warn!(stderr = %String::from_utf8_lossy(&output.stderr), "docker ps failed");
        return DockerSummary::default();
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let mut summary = DockerSummary {
        available: true,
        ..DockerSummary::default()
    };
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let id = value.get("ID").and_then(Value::as_str).unwrap_or_default();
        let name = value
            .get("Names")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim_start_matches('/')
            .to_string();
        let image = value
            .get("Image")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let state = value
            .get("State")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        summary.total += 1;
        if state.eq_ignore_ascii_case("running") {
            summary.running += 1;
        } else {
            summary.stopped += 1;
        }
        if !id.is_empty() {
            summary.by_id.insert(id.to_string(), (name, image, state));
        }
    }
    summary
}

fn docker_stats() -> Vec<DockerSample> {
    let mut cmd = Command::new("docker");
    cmd.args(["stats", "--no-stream", "--format", "{{json .}}"]);
    let Some(output) = run_with_timeout(cmd, CMD_TIMEOUT) else {
        return Vec::new();
    };
    if !output.status.success() {
        warn!(stderr = %String::from_utf8_lossy(&output.stderr), "docker stats failed");
        return Vec::new();
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let mut samples = Vec::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let id = value
            .get("ID")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let name = value
            .get("Name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim_start_matches('/')
            .to_string();
        let cpu_text = value
            .get("CPUPerc")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let mem_text = value
            .get("MemUsage")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let net_text = value
            .get("NetIO")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let block_text = value
            .get("BlockIO")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let (mem_usage, mem_limit) = crate::parse_pair_bytes(mem_text);
        let (net_rx, net_tx) = crate::parse_pair_bytes(net_text);
        let (block_read, block_write) = crate::parse_pair_bytes(block_text);
        samples.push(DockerSample {
            id,
            name,
            image: String::new(),
            state: String::new(),
            cpu_ratio: crate::parse_percent(cpu_text),
            mem_usage_bytes: mem_usage,
            mem_limit_bytes: mem_limit,
            net_rx_bytes: net_rx,
            net_tx_bytes: net_tx,
            block_read_bytes: block_read,
            block_write_bytes: block_write,
        });
    }
    samples
}

fn run_with_timeout(cmd: Command, timeout: Duration) -> Option<std::process::Output> {
    run_with_timeout_using_waiter(cmd, timeout, |child| child.try_wait())
}

fn run_with_timeout_using_waiter<W>(
    mut cmd: Command,
    timeout: Duration,
    mut waiter: W,
) -> Option<std::process::Output>
where
    W: FnMut(&mut Child) -> std::io::Result<Option<std::process::ExitStatus>>,
{
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "failed to spawn command");
            return None;
        }
    };
    let start = Instant::now();
    loop {
        match waiter(&mut child) {
            Ok(Some(_)) => return child.wait_with_output().ok(),
            Ok(None) => {
                if start.elapsed() > timeout {
                    let _ = child.kill();
                    let _ = child.wait();
                    warn!("command timed out after {:?}", timeout);
                    return None;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => {
                warn!(error = %e, "error waiting for command");
                let _ = child.kill();
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
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
}
