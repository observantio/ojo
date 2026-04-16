use std::fs;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_temp_path(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "ojo-mysql-bin-test-{name}-{}-{nanos}",
        std::process::id()
    ))
}

#[test]
fn binary_dump_snapshot_with_explicit_config_succeeds() {
    let config = unique_temp_path("config.yaml");
    fs::write(
        &config,
        "service:\n  name: ojo-mysql-integration\ncollection:\n  poll_interval_secs: 1\nmysql:\n  executable: /definitely/missing/mysql\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
    )
    .expect("write config");

    let exe = std::env::var("CARGO_BIN_EXE_ojo-mysql").expect("mysql test binary path");
    let output = Command::new(exe)
        .arg("--config")
        .arg(config.to_string_lossy().to_string())
        .arg("--dump-snapshot")
        .output()
        .expect("run ojo-mysql --dump-snapshot");

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
        "service:\n  name: ojo-mysql-integration\ncollection:\n  poll_interval_secs: 1\nmysql:\n  executable: /definitely/missing/mysql\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
    )
    .expect("write config");

    let exe = std::env::var("CARGO_BIN_EXE_ojo-mysql").expect("mysql test binary path");
    let output = Command::new(exe)
        .arg("--config")
        .arg(config.to_string_lossy().to_string())
        .env("OJO_RUN_ONCE", "1")
        .output()
        .expect("run ojo-mysql once");

    assert!(output.status.success(), "status={:?}", output.status);

    fs::remove_file(&config).expect("cleanup config");
}
