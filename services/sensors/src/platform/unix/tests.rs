use super::{
    collect_fans, collect_snapshot_from, collect_temps, collect_voltages, read_label, read_scaled,
};
use crate::SensorSnapshot;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_temp_dir(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("ojo-sensors-{name}-{}-{nanos}", std::process::id()))
}

#[test]
fn read_helpers_parse_expected_values() {
    let dir = unique_temp_dir("helpers");
    fs::create_dir_all(&dir).expect("mkdir");
    let value_file = dir.join("value");
    fs::write(&value_file, "2500\n").expect("write value");
    let bad_file = dir.join("bad");
    fs::write(&bad_file, "abc\n").expect("write bad");
    let nan_file = dir.join("nan");
    fs::write(&nan_file, "NaN\n").expect("write nan");
    let inf_file = dir.join("inf");
    fs::write(&inf_file, "inf\n").expect("write inf");
    let label_file = dir.join("temp1_label");
    fs::write(&label_file, " CPU Temp \n").expect("write label");
    let empty_label = dir.join("temp2_label");
    fs::write(&empty_label, "  \n").expect("write empty label");

    assert_eq!(read_scaled(&value_file, 1000.0), Some(2.5));
    assert_eq!(read_scaled(&bad_file, 1000.0), None);
    assert_eq!(read_scaled(&nan_file, 1000.0), None);
    assert_eq!(read_scaled(&inf_file, 1000.0), None);
    assert_eq!(read_scaled(&dir.join("missing"), 1000.0), None);
    assert_eq!(read_label(&dir, "temp1_label").as_deref(), Some("CPU Temp"));
    assert_eq!(read_label(&dir, "temp2_label"), None);
    assert_eq!(read_label(&dir, "missing_label"), None);

    fs::remove_dir_all(&dir).expect("cleanup dir");
}

#[test]
fn collect_helpers_populate_sensor_samples() {
    let dir = unique_temp_dir("hwmon");
    fs::create_dir_all(&dir).expect("mkdir");
    fs::write(dir.join("temp1_input"), "42000\n").expect("write temp");
    fs::write(dir.join("temp1_label"), "Package id 0\n").expect("write temp label");
    fs::write(dir.join("fan1_input"), "1200\n").expect("write fan");
    fs::write(dir.join("in1_input"), "1100\n").expect("write voltage");
    fs::write(dir.join("tempx_input"), "42000\n").expect("write invalid temp index");
    fs::write(dir.join("fan1_bad"), "1200\n").expect("write bad fan suffix");
    fs::write(dir.join("inx_input"), "1100\n").expect("write invalid voltage index");

    let mut snap = SensorSnapshot::default();
    collect_temps(&dir, "chip0", &mut snap);
    collect_fans(&dir, "chip0", &mut snap);
    collect_voltages(&dir, "chip0", &mut snap);

    assert!(snap.available);
    assert_eq!(snap.temperatures.len(), 1);
    assert_eq!(snap.fans.len(), 1);
    assert_eq!(snap.voltages.len(), 1);
    assert_eq!(snap.temperatures[0].label, "Package id 0");
    assert_eq!(snap.fans[0].label, "fan1");
    assert_eq!(snap.voltages[0].label, "in1");

    fs::remove_dir_all(&dir).expect("cleanup dir");
}

#[test]
fn collectors_handle_read_dir_failures_and_snapshot_defaults() {
    let dir = unique_temp_dir("not-dir");
    fs::write(&dir, "not a directory").expect("write marker");

    let mut snap = SensorSnapshot::default();
    collect_temps(&dir, "chip0", &mut snap);
    collect_fans(&dir, "chip0", &mut snap);
    collect_voltages(&dir, "chip0", &mut snap);
    assert!(!snap.available);

    let collected = collect_snapshot_from(&dir);
    assert!(!collected.available);
    assert!(collected.temperatures.is_empty());
    assert!(collected.fans.is_empty());
    assert!(collected.voltages.is_empty());

    fs::remove_file(&dir).expect("cleanup marker");
}

#[test]
fn collect_snapshot_from_uses_unknown_chip_when_name_missing() {
    let root = unique_temp_dir("snapshot-root");
    let hwmon = root.join("hwmon0");
    fs::create_dir_all(&hwmon).expect("mkdir hwmon");
    fs::write(hwmon.join("temp1_input"), "33000\n").expect("write temp");

    let snap = collect_snapshot_from(&root);
    assert!(snap.available);
    assert_eq!(snap.temperatures.len(), 1);
    assert_eq!(snap.temperatures[0].chip, "unknown");
    assert_eq!(snap.temperatures[0].label, "temp1");

    fs::remove_dir_all(&root).expect("cleanup root");
}
