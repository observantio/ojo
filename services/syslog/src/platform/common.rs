use std::io::Read;
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use tracing::warn;

#[cfg(any(test, not(coverage)))]
const CMD_TIMEOUT: Duration = Duration::from_secs(12);

#[cfg(any(test, not(coverage)))]
pub(super) fn run_command_with_timeout(command: &str, args: &[&str]) -> Option<Output> {
    let mut cmd = Command::new(command);
    cmd.args(args);
    run_with_timeout(cmd, CMD_TIMEOUT)
}

#[cfg(any(test, not(coverage)))]
fn run_with_timeout(cmd: Command, timeout: Duration) -> Option<Output> {
    run_with_timeout_using_waiter(cmd, timeout, wait_for_child)
}

#[cfg(any(test, not(coverage)))]
fn wait_for_child(child: &mut Child) -> std::io::Result<Option<std::process::ExitStatus>> {
    child.try_wait()
}

#[cfg(any(test, not(coverage)))]
fn run_with_timeout_using_waiter<W>(
    mut cmd: Command,
    timeout: Duration,
    mut waiter: W,
) -> Option<Output>
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

    let stdout = child.stdout.take().map(spawn_output_reader);
    let stderr = child.stderr.take().map(spawn_output_reader);

    let start = Instant::now();
    loop {
        match waiter(&mut child) {
            Ok(Some(status)) => {
                let stdout = join_output_reader(stdout);
                let stderr = join_output_reader(stderr);
                return Some(Output {
                    status,
                    stdout,
                    stderr,
                });
            }
            Ok(None) => {
                if start.elapsed() > timeout {
                    let _ = child.kill();
                    let _ = child.wait();
                    let _ = join_output_reader(stdout);
                    let _ = join_output_reader(stderr);
                    warn!("command timed out after {:?}", timeout);
                    return None;
                }
                std::thread::sleep(Duration::from_millis(75));
            }
            Err(e) => {
                warn!(error = %e, "error waiting for command");
                let _ = child.kill();
                let _ = child.wait();
                let _ = join_output_reader(stdout);
                let _ = join_output_reader(stderr);
                return None;
            }
        }
    }
}

#[cfg(any(test, not(coverage)))]
fn spawn_output_reader(mut reader: impl Read + Send + 'static) -> thread::JoinHandle<Vec<u8>> {
    thread::spawn(move || {
        let mut buffer = Vec::new();
        let _ = reader.read_to_end(&mut buffer);
        buffer
    })
}

#[cfg(any(test, not(coverage)))]
fn join_output_reader(handle: Option<thread::JoinHandle<Vec<u8>>>) -> Vec<u8> {
    handle
        .map(|handle| handle.join().unwrap_or_default())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    #[test]
    fn run_command_with_timeout_returns_none_for_missing_binary() {
        let result = run_command_with_timeout("definitely-missing-syslog-binary", &["--version"]);
        assert!(result.is_none());
    }

    #[test]
    fn run_command_with_timeout_covers_success_path() {
        let out = run_command_with_timeout("sh", &["-c", "printf ok"]).expect("output");
        assert!(out.status.success());
        assert_eq!(String::from_utf8_lossy(&out.stdout), "ok");
    }

    #[test]
    fn run_command_with_timeout_handles_large_output_without_deadlocking() {
        let out = run_command_with_timeout("sh", &["-c", "yes x | head -c 80000"]).expect("output");
        assert!(out.status.success());
        assert!(out.stdout.len() >= 80000);
    }

    #[test]
    fn run_with_timeout_using_waiter_covers_success_timeout_and_wait_error() {
        let mut success_cmd = Command::new("sh");
        success_cmd.arg("-c").arg("printf ok");
        let success = run_with_timeout_using_waiter(success_cmd, Duration::from_secs(2), |child| {
            child.try_wait()
        })
        .expect("successful command output");
        assert!(success.status.success());
        assert_eq!(String::from_utf8_lossy(&success.stdout), "ok");

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
