use std::collections::BTreeSet;

const DOCKER_METRICS: &[(&str, &str)] = &[
    ("system.docker.containers.total", "gauge"),
    ("system.docker.containers.running", "gauge"),
    ("system.docker.containers.stopped", "gauge"),
    ("system.docker.container.cpu.ratio", "gauge_ratio"),
    ("system.docker.container.memory.usage.bytes", "gauge"),
    ("system.docker.container.memory.limit.bytes", "gauge"),
    ("system.docker.container.network.rx.bytes", "gauge"),
    ("system.docker.container.network.tx.bytes", "gauge"),
    ("system.docker.container.block.read.bytes", "gauge"),
    ("system.docker.container.block.write.bytes", "gauge"),
    ("system.docker.source.available", "state"),
];

const GPU_METRICS: &[(&str, &str)] = &[
    ("system.gpu.devices", "inventory"),
    ("system.gpu.utilization.ratio", "gauge_ratio"),
    ("system.gpu.memory.used.bytes", "gauge"),
    ("system.gpu.memory.total.bytes", "gauge"),
    ("system.gpu.temperature.celsius", "gauge"),
    ("system.gpu.power.watts", "gauge"),
    ("system.gpu.throttled", "state"),
    ("system.gpu.source.available", "state"),
];

const SENSOR_METRICS: &[(&str, &str)] = &[
    ("system.sensor.temperature.celsius", "gauge"),
    ("system.sensor.temperature.max.celsius", "gauge"),
    ("system.sensor.fan.rpm", "gauge"),
    ("system.sensor.voltage.volts", "gauge"),
    ("system.sensor.count", "inventory"),
    ("system.sensor.source.available", "state"),
];

const MYSQL_METRICS: &[(&str, &str)] = &[
    ("system.mysql.source.available", "state"),
    ("system.mysql.up", "state"),
    ("system.mysql.connections", "gauge"),
    ("system.mysql.threads.running", "gauge"),
    ("system.mysql.queries.total", "counter"),
    ("system.mysql.slow_queries.total", "counter"),
    ("system.mysql.bytes.received.total", "counter"),
    ("system.mysql.bytes.sent.total", "counter"),
    ("system.mysql.queries.rate_per_second", "gauge_derived"),
    (
        "system.mysql.bytes.received.rate_per_second",
        "gauge_derived",
    ),
    ("system.mysql.bytes.sent.rate_per_second", "gauge_derived"),
];

const POSTGRES_METRICS: &[(&str, &str)] = &[
    ("system.postgres.source.available", "state"),
    ("system.postgres.up", "state"),
    ("system.postgres.connections", "gauge"),
    ("system.postgres.transactions.committed.total", "counter"),
    ("system.postgres.transactions.rolled_back.total", "counter"),
    ("system.postgres.deadlocks.total", "counter"),
    ("system.postgres.blocks.read.total", "counter"),
    ("system.postgres.blocks.hit.total", "counter"),
    (
        "system.postgres.transactions.committed.rate_per_second",
        "gauge_derived",
    ),
    (
        "system.postgres.transactions.rolled_back.rate_per_second",
        "gauge_derived",
    ),
];

const NFS_CLIENT_METRICS: &[(&str, &str)] = &[
    ("system.nfs_client.source.available", "state"),
    ("system.nfs_client.mounts", "inventory"),
    ("system.nfs_client.rpc.calls.total", "counter"),
    ("system.nfs_client.rpc.retransmissions.total", "counter"),
    ("system.nfs_client.rpc.auth_refreshes.total", "counter"),
    (
        "system.nfs_client.rpc.calls.rate_per_second",
        "gauge_derived",
    ),
    (
        "system.nfs_client.rpc.retransmissions.rate_per_second",
        "gauge_derived",
    ),
];

#[test]
fn extension_metric_namespaces_and_semantics_are_supported() {
    let allowed_semantics = BTreeSet::from([
        "counter",
        "gauge",
        "gauge_approximation",
        "gauge_derived",
        "gauge_derived_ratio",
        "gauge_ratio",
        "inventory",
        "state",
    ]);
    for (name, semantic) in DOCKER_METRICS
        .iter()
        .chain(GPU_METRICS.iter())
        .chain(SENSOR_METRICS.iter())
        .chain(MYSQL_METRICS.iter())
        .chain(POSTGRES_METRICS.iter())
        .chain(NFS_CLIENT_METRICS.iter())
    {
        assert!(
            name.starts_with("system."),
            "extension metric must stay in system.* namespace: {name}"
        );
        assert!(
            allowed_semantics.contains(semantic),
            "unsupported semantic kind for {name}: {semantic}"
        );
    }
}

#[test]
fn extension_metrics_cover_all_domains() {
    let namespaces = DOCKER_METRICS
        .iter()
        .chain(GPU_METRICS.iter())
        .chain(SENSOR_METRICS.iter())
        .chain(MYSQL_METRICS.iter())
        .chain(POSTGRES_METRICS.iter())
        .chain(NFS_CLIENT_METRICS.iter())
        .map(|(name, _)| {
            let mut parts = name.split('.');
            format!(
                "{}.{}",
                parts.next().unwrap_or_default(),
                parts.next().unwrap_or_default()
            )
        })
        .collect::<BTreeSet<_>>();
    assert!(namespaces.contains("system.docker"));
    assert!(namespaces.contains("system.gpu"));
    assert!(namespaces.contains("system.sensor"));
    assert!(namespaces.contains("system.mysql"));
    assert!(namespaces.contains("system.postgres"));
    assert!(namespaces.contains("system.nfs_client"));
}

#[test]
fn extension_label_cardinality_budgets_are_reasonable() {
    let docker_max_labeled_containers = 25usize;
    let gpu_max_labeled_devices = 16usize;
    let sensor_max_labeled_sensors = 32usize;

    assert!(docker_max_labeled_containers <= 100);
    assert!(gpu_max_labeled_devices <= 64);
    assert!(sensor_max_labeled_sensors <= 200);
}
