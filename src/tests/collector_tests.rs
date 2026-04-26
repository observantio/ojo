#[cfg(any(target_os = "linux", target_os = "android"))]
#[test]
fn collect_snapshot_smoke_linux_like() {
    let snap =
        super::collect_snapshot(false, crate::config::HostType::Auto).expect("collect snapshot");
    assert!(!snap.system.os_type.trim().is_empty());
    assert!(snap.system.ticks_per_second > 0);
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[test]
fn collect_snapshot_rejects_windows_host_type_on_linux_build() {
    let err = super::collect_snapshot(false, crate::config::HostType::Windows)
        .expect_err("windows host type should fail on linux build");
    assert!(err.to_string().contains("host_type=windows"), "{err}");
}
