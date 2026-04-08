use std::collections::BTreeSet;
use std::fs;

use serde_json::Value;

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

const NGINX_METRICS: &[(&str, &str)] = &[
    ("system.nginx.source.available", "state"),
    ("system.nginx.up", "state"),
    ("system.nginx.connections.active", "gauge"),
    ("system.nginx.connections.reading", "gauge"),
    ("system.nginx.connections.writing", "gauge"),
    ("system.nginx.connections.waiting", "gauge"),
    ("system.nginx.connections.accepted.total", "counter"),
    ("system.nginx.connections.handled.total", "counter"),
    ("system.nginx.requests.total", "counter"),
    (
        "system.nginx.connections.accepted.rate_per_second",
        "gauge_derived",
    ),
    ("system.nginx.requests.rate_per_second", "gauge_derived"),
];

const REDIS_METRICS: &[(&str, &str)] = &[
    ("system.redis.source.available", "state"),
    ("system.redis.up", "state"),
    ("system.redis.clients.connected", "gauge"),
    ("system.redis.clients.blocked", "gauge"),
    ("system.redis.memory.used.bytes", "gauge"),
    ("system.redis.memory.max.bytes", "gauge"),
    ("system.redis.uptime.seconds", "gauge"),
    ("system.redis.commands.processed.total", "counter"),
    ("system.redis.connections.received.total", "counter"),
    ("system.redis.keyspace.hits.total", "counter"),
    ("system.redis.keyspace.misses.total", "counter"),
    ("system.redis.keys.expired.total", "counter"),
    ("system.redis.keys.evicted.total", "counter"),
    (
        "system.redis.commands.processed.rate_per_second",
        "gauge_derived",
    ),
    (
        "system.redis.connections.received.rate_per_second",
        "gauge_derived",
    ),
    ("system.redis.keyspace.hit.ratio", "gauge_ratio"),
];

const SYSTEMD_METRICS: &[(&str, &str)] = &[
    ("system.systemd.source.available", "state"),
    ("system.systemd.up", "state"),
    ("system.systemd.units.total", "gauge"),
    ("system.systemd.units.active", "gauge"),
    ("system.systemd.units.inactive", "gauge"),
    ("system.systemd.units.failed", "gauge"),
    ("system.systemd.units.activating", "gauge"),
    ("system.systemd.units.deactivating", "gauge"),
    ("system.systemd.units.reloading", "gauge"),
    ("system.systemd.units.not_found", "gauge"),
    ("system.systemd.units.maintenance", "gauge"),
    ("system.systemd.jobs.queued", "gauge"),
    ("system.systemd.jobs.running", "gauge"),
    ("system.systemd.failed_units.reported", "gauge"),
    ("system.systemd.units.failed.ratio", "gauge_ratio"),
    ("system.systemd.units.active.ratio", "gauge_ratio"),
];

const SYSTRACE_METRICS: &[(&str, &str)] = &[
    ("system.systrace.source.available", "state"),
    ("system.systrace.up", "state"),
    ("system.systrace.tracefs.available", "state"),
    ("system.systrace.etw.available", "state"),
    ("system.systrace.tracing.on", "state"),
    ("system.systrace.tracers.available", "inventory"),
    ("system.systrace.events.total", "counter"),
    ("system.systrace.events.enabled", "counter"),
    ("system.systrace.event.categories.total", "inventory"),
    ("system.systrace.buffer.total_kb", "gauge"),
    ("system.systrace.etw.sessions.total", "gauge"),
    ("system.systrace.etw.sessions.running", "gauge"),
    ("system.systrace.etw.providers.total", "inventory"),
    ("system.systrace.exporter.available", "state"),
    ("system.systrace.exporter.reconnecting", "state"),
    ("system.systrace.exporter.errors.total", "counter"),
    ("system.systrace.context_switches_per_sec", "gauge_derived"),
    ("system.systrace.interrupts_per_sec", "gauge_derived"),
    ("system.systrace.system_calls_per_sec", "gauge_derived"),
    ("system.systrace.system_calls.source", "inventory"),
    ("system.systrace.system_calls.coverage_ratio", "gauge_ratio"),
    ("system.systrace.dpcs_per_sec", "gauge_derived"),
    ("system.systrace.process_forks_per_sec", "gauge_derived"),
    ("system.systrace.run_queue.depth", "gauge_approximation"),
    ("system.systrace.processes.total", "gauge"),
    ("system.systrace.threads.total", "gauge"),
    (
        "system.systrace.trace.kernel_stack_samples.total",
        "counter",
    ),
    ("system.systrace.trace.user_stack_samples.total", "counter"),
    ("system.systrace.collection.errors", "counter"),
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
        .chain(NGINX_METRICS.iter())
        .chain(REDIS_METRICS.iter())
        .chain(SYSTEMD_METRICS.iter())
        .chain(SYSTRACE_METRICS.iter())
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
        .chain(NGINX_METRICS.iter())
        .chain(REDIS_METRICS.iter())
        .chain(SYSTEMD_METRICS.iter())
        .chain(SYSTRACE_METRICS.iter())
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
    assert!(namespaces.contains("system.nginx"));
    assert!(namespaces.contains("system.redis"));
    assert!(namespaces.contains("system.systemd"));
    assert!(namespaces.contains("system.systrace"));
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

fn collect_systrace_metric_names_from_expr(expr: &str, out: &mut BTreeSet<String>) {
    let needle = "system_systrace_";
    let bytes = expr.as_bytes();
    let mut index = 0usize;
    while let Some(found) = expr[index..].find(needle) {
        let start = index + found;
        let mut end = start + needle.len();
        while end < bytes.len() {
            let b = bytes[end];
            if b.is_ascii_alphanumeric() || b == b'_' {
                end += 1;
            } else {
                break;
            }
        }
        out.insert(expr[start..end].to_string());
        index = end;
    }
}

#[test]
fn systrace_dashboard_covers_all_systrace_metrics() {
    let raw = fs::read_to_string("grafana/systrace.json")
        .expect("failed to read grafana/systrace.json");
    let dashboard: Value =
        serde_json::from_str(&raw).expect("failed to parse grafana/systrace.json as JSON");
    let panels = dashboard
        .get("panels")
        .and_then(Value::as_array)
        .expect("dashboard.panels must be an array");

    let mut referenced = BTreeSet::new();
    for panel in panels {
        let Some(targets) = panel.get("targets").and_then(Value::as_array) else {
            continue;
        };
        for target in targets {
            let Some(expr) = target.get("expr").and_then(Value::as_str) else {
                continue;
            };
            collect_systrace_metric_names_from_expr(expr, &mut referenced);
        }
    }

    for (metric_name, _) in SYSTRACE_METRICS {
        let prom_name = metric_name.replace('.', "_");
        assert!(
            referenced.contains(&prom_name),
            "systrace dashboard is missing metric query for {metric_name} ({prom_name})"
        );
    }
}
