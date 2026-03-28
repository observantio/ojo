#[cfg(any(target_os = "linux", target_os = "android"))]
#[test]
fn collect_snapshot_smoke_linux_like() {
    let snap = super::collect_snapshot(false).expect("collect snapshot");
    assert!(!snap.system.os_type.trim().is_empty());
    assert!(snap.system.ticks_per_second > 0);
}
