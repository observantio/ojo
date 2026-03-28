pub fn collect_snapshot(include_process_metrics: bool) -> Result<Snapshot> {
    let mut system = SystemSnapshot::default();
    system.is_windows = false;
    system.os_type = "linux".to_string();
    system.ticks_per_second = 100;

    let mut processes = Vec::new();
    if include_process_metrics {
        processes.push(ProcessSnapshot {
            pid: 1,
            ppid: 0,
            comm: "coverage-proc".to_string(),
            state: "R".to_string(),
            num_threads: 1,
            ..ProcessSnapshot::default()
        });
    }
    system.process_count = processes.len() as u64;

    let mut support_state = BTreeMap::new();
    support_state.insert(
        "system.linux.cgroup.mode".to_string(),
        "coverage_stub".to_string(),
    );

    let mut metric_classification = BTreeMap::new();
    metric_classification.insert(
        "system.cpu.time".to_string(),
        "counter".to_string(),
    );

    Ok(Snapshot {
        system,
        memory: MemorySnapshot::default(),
        load: Some(LoadSnapshot::default()),
        processes,
        support_state,
        metric_classification,
        ..Snapshot::default()
    })
}

#[cfg(test)]
mod coverage_tests {
    use super::collect_snapshot;

    #[test]
    fn collect_snapshot_without_process_metrics_uses_stub_fields() {
        let snap = collect_snapshot(false).expect("collect snapshot");
        assert_eq!(snap.system.os_type, "linux");
        assert_eq!(snap.system.ticks_per_second, 100);
        assert_eq!(snap.system.process_count, 0);
        assert!(snap.processes.is_empty());
        assert!(snap.support_state.contains_key("system.linux.cgroup.mode"));
        assert!(!snap.metric_classification.is_empty());
    }

    #[test]
    fn collect_snapshot_with_process_metrics_returns_one_process() {
        let snap = collect_snapshot(true).expect("collect snapshot");
        assert_eq!(snap.system.process_count, 1);
        assert_eq!(snap.processes.len(), 1);
        assert_eq!(snap.processes[0].pid, 1);
    }
}