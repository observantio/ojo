use std::fs;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_temp_path(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "ojo-systemd-bin-test-{name}-{}-{nanos}",
        std::process::id()
    ))
}

#[test]
fn binary_dump_snapshot_with_explicit_config_succeeds() {
    let config = unique_temp_path("config.yaml");
    fs::write(
        &config,
        "service:\n  name: ojo-systemd-integration\ncollection:\n  poll_interval_secs: 1\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\nstorage:\n  archive_enabled: false\n",
    )
    .expect("write config");

    let exe = std::env::var("CARGO_BIN_EXE_ojo-systemd").expect("systemd test binary path");
    let output = Command::new(exe)
        .arg("--config")
        .arg(config.to_string_lossy().to_string())
        .arg("--dump-snapshot")
        .output()
        .expect("run ojo-systemd --dump-snapshot");

    assert!(output.status.success(), "status={:?}", output.status);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"available\""), "stdout={stdout}");

    fs::remove_file(&config).expect("cleanup config");
}

#[test]
fn binary_run_once_with_explicit_config_succeeds() {
    let config = unique_temp_path("run-once.yaml");
    fs::write(
        &config,
        "service:\n  name: ojo-systemd-integration\ncollection:\n  poll_interval_secs: 1\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\nstorage:\n  archive_enabled: false\n",
    )
    .expect("write config");

    let exe = std::env::var("CARGO_BIN_EXE_ojo-systemd").expect("systemd test binary path");
    let output = Command::new(exe)
        .arg("--config")
        .arg(config.to_string_lossy().to_string())
        .env("OJO_RUN_ONCE", "1")
        .env("OJO_SYSTEMD_SIMULATE_UP", "1")
        .output()
        .expect("run ojo-systemd once");

    assert!(output.status.success(), "status={:?}", output.status);

    fs::remove_file(&config).expect("cleanup config");
}
