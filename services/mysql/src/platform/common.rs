use crate::{MysqlConfig, MysqlSnapshot};
use std::process::Child;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tracing::warn;

const CMD_TIMEOUT: Duration = Duration::from_secs(15);

pub(super) fn collect_snapshot_impl(cfg: &MysqlConfig, default_executable: &str) -> MysqlSnapshot {
    let executable = if cfg.executable.trim().is_empty() {
        default_executable
    } else {
        cfg.executable.as_str()
    };
    let mut command = Command::new(executable);
    command.args(["--batch", "--raw", "--skip-column-names"]);
    if let Some(host) = &cfg.host {
        command.args(["-h", host]);
    }
    if let Some(port) = cfg.port {
        command.args(["-P", &port.to_string()]);
    }
    if let Some(user) = &cfg.user {
        command.args(["-u", user]);
    }
    if let Some(password) = &cfg.password {
        command.arg(format!("-p{password}"));
    }
    if let Some(database) = &cfg.database {
        command.args(["-D", database]);
    }
    command.args([
        "-e",
        "SHOW GLOBAL STATUS WHERE Variable_name IN ('Threads_connected','Threads_running','Queries','Slow_queries','Bytes_received','Bytes_sent')",
    ]);

    let Some(output) = run_with_timeout(command, CMD_TIMEOUT) else {
        return MysqlSnapshot::default();
    };
    if !output.status.success() {
        warn!(stderr = %String::from_utf8_lossy(&output.stderr), "mysql command failed");
        return MysqlSnapshot::default();
    }

    let text = String::from_utf8_lossy(&output.stdout);
    parse_mysql_status_output(&text)
}

fn parse_mysql_status_output(text: &str) -> MysqlSnapshot {
    let mut values = std::collections::BTreeMap::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let mut parts = line.split('\t');
        let key = parts.next().unwrap_or_default().trim();
        let value = parts.next().unwrap_or_default().trim();
        if !key.is_empty() {
            values.insert(key.to_string(), value.to_string());
        }
    }
    if values.is_empty() {
        return MysqlSnapshot::default();
    }

    MysqlSnapshot {
        available: true,
        up: true,
        connections: parse_u64(values.get("Threads_connected")),
        threads_running: parse_u64(values.get("Threads_running")),
        queries_total: parse_u64(values.get("Queries")),
        slow_queries_total: parse_u64(values.get("Slow_queries")),
        bytes_received_total: parse_u64(values.get("Bytes_received")),
        bytes_sent_total: parse_u64(values.get("Bytes_sent")),
    }
}

fn parse_u64(value: Option<&String>) -> u64 {
    value
        .map(String::as_str)
        .unwrap_or_default()
        .trim()
        .parse::<u64>()
        .unwrap_or(0)
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
        collect_snapshot_impl, parse_mysql_status_output, parse_u64, run_with_timeout,
        run_with_timeout_using_waiter,
    };
    use crate::MysqlConfig;
    use std::fs;
    use std::process::Command;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
        let dir = unique_temp_dir("script");
        fs::create_dir_all(&dir).expect("mkdir");
        let script = dir.join("fake-mysql.sh");
        let marker = dir.join("args.txt");
        fs::write(
            &script,
            format!(
                "#!/bin/sh\nprintf '%s\n' \"$@\" > {}\nprintf 'Threads_connected\t1\nThreads_running\t2\nQueries\t3\nSlow_queries\t4\nBytes_received\t5\nBytes_sent\t6\n'\n",
                marker.to_string_lossy()
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
            executable: " ".to_string(),
            host: Some("db.example".to_string()),
            port: Some(3307),
            user: Some("root".to_string()),
            password: Some("secret".to_string()),
            database: Some("app".to_string()),
        };
        let snap = collect_snapshot_impl(&cfg, script.to_string_lossy().as_ref());
        assert!(snap.available);
        assert_eq!(snap.connections, 1);
        assert_eq!(snap.bytes_sent_total, 6);

        let args = fs::read_to_string(&marker).expect("read args");
        assert!(args.contains("--batch"));
        assert!(args.contains("-h\ndb.example"));
        assert!(args.contains("-P\n3307"));
        assert!(args.contains("-u\nroot"));
        assert!(args.contains("-psecret"));
        assert!(args.contains("-D\napp"));
        assert!(args.contains("-e"));

        fs::remove_file(&script).expect("cleanup script");
        fs::remove_file(&marker).expect("cleanup marker");
        fs::remove_dir_all(&dir).expect("cleanup dir");
    }

    #[test]
    fn collect_snapshot_impl_returns_default_on_failed_exit_status() {
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
}
