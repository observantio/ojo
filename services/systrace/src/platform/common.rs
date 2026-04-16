use std::collections::BTreeMap;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tracing::warn;

const CMD_TIMEOUT: Duration = Duration::from_secs(15);

pub(super) fn parse_key_value_lines(text: &str) -> BTreeMap<String, f64> {
    let mut out = BTreeMap::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Some((k, v)) = trimmed.split_once('=') else {
            continue;
        };
        let key = k.trim().to_ascii_lowercase();
        let value = v.trim().parse::<f64>().unwrap_or(0.0);
        if !key.is_empty() {
            out.insert(key, value);
        }
    }
    out
}

pub(super) fn run_command_with_timeout(
    command: &str,
    args: &[&str],
) -> Option<std::process::Output> {
    let mut cmd = Command::new(command);
    cmd.args(args);
    run_with_timeout(cmd, CMD_TIMEOUT)
}

fn run_with_timeout(cmd: Command, timeout: Duration) -> Option<std::process::Output> {
    run_with_timeout_using_waiter(cmd, timeout, wait_for_child)
}

fn wait_for_child(child: &mut Child) -> std::io::Result<Option<std::process::ExitStatus>> {
    child.try_wait()
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
    let child_result = cmd.spawn();
    let mut child = match child_result {
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
    use super::*;
    use std::process::Command;

    #[test]
    fn parse_key_value_lines_handles_valid_invalid_and_empty_lines() {
        let parsed = parse_key_value_lines("A=1\n\nB = not-a-number\nno-delimiter\n = 7\nC=3.5\n");
        assert_eq!(parsed.get("a"), Some(&1.0));
        assert_eq!(parsed.get("b"), Some(&0.0));
        assert_eq!(parsed.get("c"), Some(&3.5));
        assert!(!parsed.contains_key(""));
    }

    #[test]
    fn run_command_with_timeout_returns_none_for_missing_binary() {
        let result = run_command_with_timeout("definitely-missing-systrace-binary", &["--version"]);
        assert!(result.is_none());
    }

    #[test]
    fn run_command_with_timeout_covers_success_path() {
        let out = run_command_with_timeout("sh", &["-c", "printf systrace"]).expect("output");
        assert!(out.status.success());
        assert_eq!(String::from_utf8_lossy(&out.stdout), "systrace");
    }

    #[test]
    fn run_with_timeout_using_waiter_covers_success_timeout_and_wait_error() {
        let mut success_cmd = Command::new("sh");
        success_cmd.arg("-c").arg("printf systrace");
        let success = run_with_timeout_using_waiter(success_cmd, Duration::from_secs(2), |child| {
            child.try_wait()
        })
        .expect("successful command output");
        assert!(success.status.success());
        assert_eq!(String::from_utf8_lossy(&success.stdout), "systrace");

        let mut timeout_cmd = Command::new("sh");
        timeout_cmd.arg("-c").arg("sleep 1");
        let timed_out =
            run_with_timeout_using_waiter(timeout_cmd, Duration::from_millis(1), |_| Ok(None));
        assert!(timed_out.is_none());

        let mut wait_error_cmd = Command::new("sh");
        wait_error_cmd.arg("-c").arg("printf nope");
        let wait_error =
            run_with_timeout_using_waiter(wait_error_cmd, Duration::from_secs(1), |_| {
                Err(std::io::Error::other("wait failed"))
            });
        assert!(wait_error.is_none());
    }
}
