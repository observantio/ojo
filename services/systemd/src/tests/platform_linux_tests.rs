use super::{parse_manager_values, parse_units_table, snapshot_from_outputs};
use std::process::{ExitStatus, Output};

#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;

#[cfg(unix)]
fn fake_output(success: bool, stdout: &str) -> Output {
    Output {
        status: if success {
            ExitStatus::from_raw(0)
        } else {
            ExitStatus::from_raw(1)
        },
        stdout: stdout.as_bytes().to_vec(),
        stderr: Vec::new(),
    }
}

#[test]
fn parse_manager_values_reads_ordered_lines() {
    let values = parse_manager_values("5\n2\n");
    assert_eq!(values.get("njobs"), Some(&5));
    assert_eq!(values.get("nfailedunits"), Some(&2));
}

#[test]
fn parse_units_table_counts_active_states() {
    let text = "a.service loaded active running A\n\nb.service loaded failed failed B\nc.service loaded inactive dead C\nd.service loaded activating start D\n";
    let counts = parse_units_table(text);
    assert_eq!(counts.get("active"), Some(&1));
    assert_eq!(counts.get("failed"), Some(&1));
    assert_eq!(counts.get("inactive"), Some(&1));
    assert_eq!(counts.get("activating"), Some(&1));
}

#[test]
fn parser_helpers_handle_invalid_inputs() {
    let manager = parse_manager_values("bad\nvalue\n");
    assert!(manager.is_empty());

    let manager_empty = parse_manager_values("");
    assert!(manager_empty.is_empty());

    let manager_one_line = parse_manager_values("9\n");
    assert_eq!(manager_one_line.get("njobs"), Some(&9));
    assert_eq!(manager_one_line.get("nfailedunits"), None);

    let counts = parse_units_table("too-short\n\n");
    assert!(counts.is_empty());
}

#[test]
fn collect_snapshot_smoke_is_stable() {
    let snapshot = super::collect_snapshot();
    if snapshot.available {
        assert!(snapshot.units_total >= snapshot.units_active);
    } else {
        assert_eq!(snapshot.units_total, 0);
    }
}

#[test]
#[cfg(unix)]
fn snapshot_from_outputs_covers_units_missing_and_failed_paths() {
    let missing = snapshot_from_outputs(None, None);
    assert!(!missing.available);

    let failed_units = snapshot_from_outputs(
        Some(fake_output(true, "1\n2\n")),
        Some(fake_output(false, "a.service loaded active running A\n")),
    );
    assert!(!failed_units.available);

    let empty_units = snapshot_from_outputs(
        Some(fake_output(true, "1\n2\n")),
        Some(fake_output(true, "short\n")),
    );
    assert!(!empty_units.available);
}

#[test]
#[cfg(unix)]
fn snapshot_from_outputs_covers_manager_fallback_and_success_paths() {
    let units = "a.service loaded active running A\nb.service loaded failed failed B\n";
    let fallback = snapshot_from_outputs(
        Some(fake_output(false, "oops")),
        Some(fake_output(true, units)),
    );
    assert!(fallback.available);
    assert_eq!(fallback.jobs_queued, 0);
    assert_eq!(fallback.units_total, 2);

    let success = snapshot_from_outputs(
        Some(fake_output(true, "7\n3\n")),
        Some(fake_output(true, units)),
    );
    assert!(success.available);
    assert_eq!(success.jobs_queued, 7);
    assert_eq!(success.failed_units_reported, 3);
    assert_eq!(success.units_active, 1);
    assert_eq!(success.units_failed, 1);
}
