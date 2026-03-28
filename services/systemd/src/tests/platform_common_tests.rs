use super::{
    parse_key_value_lines, run_command_with_timeout, run_with_timeout_using_waiter, wait_for_child,
};
use std::io;
use std::process::{Child, Command};
use std::time::Duration;

#[test]
fn parse_key_value_lines_handles_valid_and_invalid_rows() {
    let parsed = parse_key_value_lines("active=10\ninvalid\nfailed=2\n=4\n\n");
    assert_eq!(parsed.get("active"), Some(&10));
    assert_eq!(parsed.get("failed"), Some(&2));
    assert!(!parsed.contains_key(""));
}

#[test]
fn run_command_with_timeout_executes_successfully() {
    let output = run_command_with_timeout("sh", &["-c", "printf 'ok'"]).expect("output");
    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), "ok");
}

#[test]
fn run_with_timeout_using_waiter_covers_timeout_wait_error_and_spawn_error() {
    let mut timeout_cmd = Command::new("sh");
    timeout_cmd.args(["-c", "sleep 1"]);
    let timeout =
        run_with_timeout_using_waiter(timeout_cmd, Duration::from_millis(10), wait_for_child);
    assert!(timeout.is_none());

    let mut err_cmd = Command::new("sh");
    err_cmd.args(["-c", "printf 'x'"]);
    let wait_error =
        run_with_timeout_using_waiter(err_cmd, Duration::from_millis(100), |_child: &mut Child| {
            Err(io::Error::other("forced wait error"))
        });
    assert!(wait_error.is_none());

    let spawn_error = run_with_timeout_using_waiter(
        Command::new("/definitely/missing/ojo-systemd-command"),
        Duration::from_millis(100),
        wait_for_child,
    );
    assert!(spawn_error.is_none());
}
