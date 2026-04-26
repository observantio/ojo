use crate::config::TieredReplayConfig;
use crate::model::{Snapshot, SystemSnapshot};
use crate::tiered_queue::{BufferedInterval, TieredReplayQueue};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("ojo-tiered-{name}-{}-{nanos}", std::process::id()))
}

fn sample_interval(idx: u64) -> BufferedInterval {
    let snapshot = Snapshot {
        system: SystemSnapshot {
            os_type: "linux".to_string(),
            ticks_per_second: 100,
            uptime_secs: idx as f64,
            ..SystemSnapshot::default()
        },
        ..Snapshot::default()
    };

    BufferedInterval { snapshot }
}

#[test]
fn tiered_queue_spills_and_rehydrates_from_wal() {
    let wal_dir = unique_temp_dir("spill-rehydrate");
    let cfg = TieredReplayConfig {
        enabled: true,
        memory_cap_items: 2,
        wal_dir: wal_dir.to_string_lossy().to_string(),
        wal_segment_max_bytes: 1024,
        wal_segment_max_age_secs: 3600,
        max_replay_per_tick: 16,
    };

    let mut queue = TieredReplayQueue::from_config(&cfg).expect("queue init");
    queue.push(sample_interval(1)).expect("push 1");
    queue.push(sample_interval(2)).expect("push 2");
    queue.push(sample_interval(3)).expect("push 3");

    let drained = queue.drain_batch(8).expect("drain");
    assert_eq!(drained.len(), 3);
    assert!(drained[0].snapshot.system.uptime_secs <= drained[1].snapshot.system.uptime_secs);
    assert!(drained[1].snapshot.system.uptime_secs <= drained[2].snapshot.system.uptime_secs);
    assert!(!queue.has_pending());

    let _ = fs::remove_dir_all(&wal_dir);
}

#[test]
fn tiered_queue_requeue_front_preserves_order() {
    let wal_dir = unique_temp_dir("requeue-order");
    let cfg = TieredReplayConfig {
        enabled: true,
        memory_cap_items: 8,
        wal_dir: wal_dir.to_string_lossy().to_string(),
        wal_segment_max_bytes: 4096,
        wal_segment_max_age_secs: 3600,
        max_replay_per_tick: 16,
    };

    let mut queue = TieredReplayQueue::from_config(&cfg).expect("queue init");
    queue.push(sample_interval(10)).expect("push");
    queue.push(sample_interval(20)).expect("push");

    let batch = queue.drain_batch(2).expect("drain");
    assert_eq!(batch.len(), 2);

    queue.requeue_front(batch).expect("requeue");
    let replay = queue.drain_batch(2).expect("replay");
    assert_eq!(replay.len(), 2);
    assert_eq!(replay[0].snapshot.system.uptime_secs, 10.0);
    assert_eq!(replay[1].snapshot.system.uptime_secs, 20.0);

    let _ = fs::remove_dir_all(&wal_dir);
}

#[test]
fn tiered_queue_honors_disabled_mode() {
    let wal_dir = unique_temp_dir("disabled");
    let cfg = TieredReplayConfig {
        enabled: false,
        memory_cap_items: 1,
        wal_dir: wal_dir.to_string_lossy().to_string(),
        wal_segment_max_bytes: 256,
        wal_segment_max_age_secs: 1,
        max_replay_per_tick: 1,
    };

    let mut queue = TieredReplayQueue::from_config(&cfg).expect("queue init");
    assert!(!queue.is_enabled());
    queue.push(sample_interval(1)).expect("push disabled");
    let drained = queue.drain_batch(8).expect("drain disabled");
    assert!(drained.is_empty());
    assert!(!queue.has_pending());

    let _ = fs::remove_dir_all(&wal_dir);
}
