use super::*;

fn attrs_to_pairs(attrs: Vec<KeyValue>) -> Vec<(String, String)> {
    attrs
        .into_iter()
        .map(|kv| (kv.key.as_str().to_string(), kv.value.to_string()))
        .collect()
}

#[test]
fn parses_pressure_attributes() {
    let attrs = pressure_attrs("memory.some.avg10").expect("expected valid pressure attrs");
    assert_eq!(
        attrs_to_pairs(attrs),
        vec![
            ("resource".to_string(), "memory".to_string()),
            ("scope".to_string(), "some".to_string()),
            ("window".to_string(), "avg10".to_string()),
        ]
    );
}

#[test]
fn rejects_malformed_pressure_attributes() {
    assert!(pressure_attrs("memory.some").is_none());
    assert!(pressure_stall_time_attrs("memory.some.avg10").is_none());
    assert!(netstat_attrs("TcpRetransSegs").is_none());
}

#[test]
fn parses_pipe_separated_attributes() {
    assert_eq!(
        attrs_to_pairs(zoneinfo_attrs("node0|DMA|nr_free_pages").expect("zone attrs")),
        vec![
            ("node".to_string(), "node0".to_string()),
            ("zone".to_string(), "DMA".to_string()),
            ("key".to_string(), "nr_free_pages".to_string()),
        ]
    );
    assert_eq!(
        attrs_to_pairs(slabinfo_attrs("kmalloc-64|active_objs|count").expect("slab attrs")),
        vec![
            ("cache".to_string(), "kmalloc-64".to_string()),
            ("key".to_string(), "active_objs".to_string()),
            ("unit".to_string(), "count".to_string()),
        ]
    );
    assert_eq!(
        attrs_to_pairs(filesystem_attrs("/var|used|bytes").expect("filesystem attrs")),
        vec![
            ("mountpoint".to_string(), "/var".to_string()),
            ("key".to_string(), "used".to_string()),
            ("unit".to_string(), "bytes".to_string()),
        ]
    );
}

#[test]
fn parses_cgroup_attributes_for_simple_and_device_keys() {
    assert_eq!(
        attrs_to_pairs(cgroup_attrs("memory|memory.current|bytes").expect("simple cgroup")),
        vec![
            ("scope".to_string(), "memory".to_string()),
            ("section".to_string(), "memory.current".to_string()),
            ("key".to_string(), "bytes".to_string()),
        ]
    );

    assert_eq!(
        attrs_to_pairs(
            cgroup_attrs("blkio|blkio.throttle.io_service_bytes|8:0|Read").expect("device cgroup")
        ),
        vec![
            ("scope".to_string(), "blkio".to_string()),
            (
                "section".to_string(),
                "blkio.throttle.io_service_bytes".to_string(),
            ),
            ("device".to_string(), "8:0".to_string()),
            ("key".to_string(), "Read".to_string()),
        ]
    );
}

#[test]
fn parses_split_pair_attributes() {
    assert_eq!(
        attrs_to_pairs(interrupts_attrs("IRQ16|cpu0").expect("interrupt attrs")),
        vec![
            ("irq".to_string(), "IRQ16".to_string()),
            ("cpu".to_string(), "cpu0".to_string()),
        ]
    );
    assert_eq!(
        attrs_to_pairs(softirqs_attrs("NET_RX|cpu1").expect("softirq attrs")),
        vec![
            ("type".to_string(), "NET_RX".to_string()),
            ("cpu".to_string(), "cpu1".to_string()),
        ]
    );
    assert_eq!(
        attrs_to_pairs(runqueue_attrs("cpu|2").expect("runqueue attrs")),
        vec![("cpu".to_string(), "2".to_string())]
    );
}

#[test]
fn runqueue_requires_cpu_scope() {
    assert!(runqueue_attrs("all|2").is_none());
}

#[test]
fn rejects_malformed_pipe_separated_attributes() {
    assert!(zoneinfo_attrs("node0|DMA").is_none());
    assert!(buddyinfo_attrs("node0|DMA").is_none());
    assert!(filesystem_attrs("/var|used").is_none());
    assert!(cgroup_attrs("memory|memory.current").is_none());
}
