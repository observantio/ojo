use crate::delta::DerivedMetrics;
use crate::model::Snapshot;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default)]
pub struct MetricView {
    pub columns: Vec<String>,
    pub values: Vec<String>,
}

fn current_time_string() -> String {
    std::process::Command::new("date")
        .arg("+%H:%M:%S")
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
            } else {
                None
            }
        })
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "time unavailable".to_string())
}

fn column_widths(headers: &[String], row: &[String]) -> Vec<usize> {
    headers
        .iter()
        .zip(row.iter())
        .map(|(header, value)| header.len().max(value.len()))
        .collect()
}

fn format_row(cells: &[String], widths: &[usize]) -> String {
    cells
        .iter()
        .enumerate()
        .map(|(idx, value)| format!("{value:<width$}", width = widths[idx]))
        .collect::<Vec<_>>()
        .join("  ")
}

fn normalize_segment(input: &str) -> String {
    let mut out = String::new();
    let mut prev_was_sep = false;

    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            prev_was_sep = false;
        } else if !prev_was_sep {
            out.push('_');
            prev_was_sep = true;
        }
    }

    out.trim_matches('_').to_string()
}

fn register_scalar(
    registry: &mut BTreeMap<String, MetricView>,
    name: impl Into<String>,
    value: impl ToString,
) {
    registry.insert(
        name.into(),
        MetricView {
            columns: vec!["value".to_string()],
            values: vec![value.to_string()],
        },
    );
}

fn register_vector(
    registry: &mut BTreeMap<String, MetricView>,
    name: impl Into<String>,
    columns: Vec<String>,
    values: Vec<String>,
) {
    registry.insert(name.into(), MetricView { columns, values });
}

pub fn build_registry(snap: &Snapshot, derived: &DerivedMetrics) -> BTreeMap<String, MetricView> {
    let mut registry = BTreeMap::new();

    register_scalar(
        &mut registry,
        "system.cpu.utilization",
        format!("{:.6}", derived.cpu_utilization_ratio),
    );
    register_vector(
        &mut registry,
        "system.cpu.core.utilization",
        derived
            .per_cpu_utilization_ratio
            .iter()
            .map(|(cpu, _)| format!("cpu{cpu}"))
            .collect(),
        derived
            .per_cpu_utilization_ratio
            .iter()
            .map(|(_, v)| format!("{v:.6}"))
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.cpu.core.system_ratio",
        derived
            .per_cpu_system_ratio
            .iter()
            .map(|(cpu, _)| format!("cpu{cpu}"))
            .collect(),
        derived
            .per_cpu_system_ratio
            .iter()
            .map(|(_, v)| format!("{v:.6}"))
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.cpu.core.iowait_ratio",
        derived
            .per_cpu_iowait_ratio
            .iter()
            .map(|(cpu, _)| format!("cpu{cpu}"))
            .collect(),
        derived
            .per_cpu_iowait_ratio
            .iter()
            .map(|(_, v)| format!("{v:.6}"))
            .collect(),
    );
    register_scalar(&mut registry, "system.load.1m", format!("{:.6}", snap.load.one));
    register_scalar(&mut registry, "system.load.5m", format!("{:.6}", snap.load.five));
    register_scalar(
        &mut registry,
        "system.load.15m",
        format!("{:.6}", snap.load.fifteen),
    );
    register_scalar(
        &mut registry,
        "system.uptime",
        format!("{:.6}", snap.system.uptime_secs),
    );
    register_scalar(
        &mut registry,
        "system.processes.count",
        snap.system.process_count,
    );
    register_scalar(&mut registry, "system.linux.pid.max", snap.system.pid_max);
    register_scalar(
        &mut registry,
        "system.linux.entropy.available",
        snap.system.entropy_available_bits,
    );
    register_scalar(
        &mut registry,
        "system.linux.entropy.pool_size",
        snap.system.entropy_pool_size_bits,
    );
    register_scalar(
        &mut registry,
        "system.memory.used_ratio",
        format!("{:.6}", derived.memory_used_ratio),
    );
    register_scalar(
        &mut registry,
        "system.swap.used_ratio",
        format!("{:.6}", derived.swap_used_ratio),
    );
    register_scalar(
        &mut registry,
        "system.paging.faults_per_sec",
        format!("{:.6}", derived.page_faults_per_sec),
    );
    register_scalar(
        &mut registry,
        "system.paging.major_faults_per_sec",
        format!("{:.6}", derived.major_page_faults_per_sec),
    );

    for (key, value) in &snap.meminfo_raw {
        register_scalar(
            &mut registry,
            format!("system.memory.{}", normalize_segment(key)),
            value,
        );
    }

    for (key, value) in &snap.vmstat {
        register_scalar(
            &mut registry,
            format!("system.vmstat.{}", normalize_segment(key)),
            value,
        );
    }

    for (key, value) in &snap.net_snmp {
        let parts = key.split('.').map(normalize_segment).collect::<Vec<_>>();
        register_scalar(
            &mut registry,
            format!("system.network.{}", parts.join(".")),
            value,
        );
    }

    for (key, value) in &snap.pressure {
        register_scalar(
            &mut registry,
            format!("system.linux.pressure.{key}"),
            format!("{value:.6}"),
        );
    }

    register_scalar(
        &mut registry,
        "system.network.softnet.processed_per_sec",
        format!("{:.6}", derived.softnet_processed_per_sec),
    );
    register_scalar(
        &mut registry,
        "system.network.softnet.dropped_per_sec",
        format!("{:.6}", derived.softnet_dropped_per_sec),
    );
    register_scalar(
        &mut registry,
        "system.network.softnet.time_squeezed_per_sec",
        format!("{:.6}", derived.softnet_time_squeezed_per_sec),
    );
    register_scalar(
        &mut registry,
        "system.network.softnet.drop_ratio",
        format!("{:.6}", derived.softnet_drop_ratio),
    );
    register_vector(
        &mut registry,
        "system.network.softnet.percpu.processed",
        snap.softnet.iter().map(|cpu| format!("cpu{}", cpu.cpu)).collect(),
        snap.softnet
            .iter()
            .map(|cpu| cpu.processed.to_string())
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.network.softnet.percpu.dropped",
        snap.softnet.iter().map(|cpu| format!("cpu{}", cpu.cpu)).collect(),
        snap.softnet
            .iter()
            .map(|cpu| cpu.dropped.to_string())
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.network.softnet.percpu.time_squeezed",
        snap.softnet.iter().map(|cpu| format!("cpu{}", cpu.cpu)).collect(),
        snap.softnet
            .iter()
            .map(|cpu| cpu.time_squeezed.to_string())
            .collect(),
    );

    register_vector(
        &mut registry,
        "system.disk.device.reads",
        snap.disks.iter().map(|d| d.name.clone()).collect(),
        snap.disks.iter().map(|d| d.reads.to_string()).collect(),
    );
    register_vector(
        &mut registry,
        "system.disk.device.writes",
        snap.disks.iter().map(|d| d.name.clone()).collect(),
        snap.disks.iter().map(|d| d.writes.to_string()).collect(),
    );
    register_vector(
        &mut registry,
        "system.disk.device.read_bytes_per_sec",
        snap.disks.iter().map(|d| d.name.clone()).collect(),
        snap.disks
            .iter()
            .map(|d| {
                format!(
                    "{:.6}",
                    derived
                        .disk_read_bytes_per_sec
                        .get(&d.name)
                        .copied()
                        .unwrap_or(0.0)
                )
            })
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.disk.device.write_bytes_per_sec",
        snap.disks.iter().map(|d| d.name.clone()).collect(),
        snap.disks
            .iter()
            .map(|d| {
                format!(
                    "{:.6}",
                    derived
                        .disk_write_bytes_per_sec
                        .get(&d.name)
                        .copied()
                        .unwrap_or(0.0)
                )
            })
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.disk.device.total_bytes_per_sec",
        snap.disks.iter().map(|d| d.name.clone()).collect(),
        snap.disks
            .iter()
            .map(|d| {
                format!(
                    "{:.6}",
                    derived
                        .disk_total_bytes_per_sec
                        .get(&d.name)
                        .copied()
                        .unwrap_or(0.0)
                )
            })
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.disk.device.await",
        snap.disks.iter().map(|d| d.name.clone()).collect(),
        snap.disks
            .iter()
            .map(|d| {
                let r = derived.disk_read_await_ms.get(&d.name).copied().unwrap_or(0.0);
                let w = derived
                    .disk_write_await_ms
                    .get(&d.name)
                    .copied()
                    .unwrap_or(0.0);
                format!("{:.6}", (r + w) / 2.0)
            })
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.disk.device.read_await",
        snap.disks.iter().map(|d| d.name.clone()).collect(),
        snap.disks
            .iter()
            .map(|d| {
                format!(
                    "{:.6}",
                    derived.disk_read_await_ms.get(&d.name).copied().unwrap_or(0.0)
                )
            })
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.disk.device.write_await",
        snap.disks.iter().map(|d| d.name.clone()).collect(),
        snap.disks
            .iter()
            .map(|d| {
                format!(
                    "{:.6}",
                    derived
                        .disk_write_await_ms
                        .get(&d.name)
                        .copied()
                        .unwrap_or(0.0)
                )
            })
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.disk.device.avg_queue_length",
        snap.disks.iter().map(|d| d.name.clone()).collect(),
        snap.disks
            .iter()
            .map(|d| {
                format!(
                    "{:.6}",
                    derived.disk_queue_depth.get(&d.name).copied().unwrap_or(0.0)
                )
            })
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.disk.device.avg_request_size",
        snap.disks.iter().map(|d| d.name.clone()).collect(),
        snap.disks
            .iter()
            .map(|d| {
                let r = derived
                    .disk_avg_read_size_bytes
                    .get(&d.name)
                    .copied()
                    .unwrap_or(0.0);
                let w = derived
                    .disk_avg_write_size_bytes
                    .get(&d.name)
                    .copied()
                    .unwrap_or(0.0);
                format!("{:.6}", (r + w) / 2.0)
            })
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.disk.device.utilization",
        snap.disks.iter().map(|d| d.name.clone()).collect(),
        snap.disks
            .iter()
            .map(|d| {
                format!(
                    "{:.6}",
                    derived
                        .disk_utilization_ratio
                        .get(&d.name)
                        .copied()
                        .unwrap_or(0.0)
                )
            })
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.disk.device.inflight",
        snap.disks.iter().map(|d| d.name.clone()).collect(),
        snap.disks.iter().map(|d| d.in_progress.to_string()).collect(),
    );

    register_vector(
        &mut registry,
        "system.network.interface.in.bytes_per_sec",
        snap.net.iter().map(|n| n.name.clone()).collect(),
        snap.net
            .iter()
            .map(|n| {
                format!(
                    "{:.6}",
                    derived.net_rx_bytes_per_sec.get(&n.name).copied().unwrap_or(0.0)
                )
            })
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.network.interface.out.bytes_per_sec",
        snap.net.iter().map(|n| n.name.clone()).collect(),
        snap.net
            .iter()
            .map(|n| {
                format!(
                    "{:.6}",
                    derived.net_tx_bytes_per_sec.get(&n.name).copied().unwrap_or(0.0)
                )
            })
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.network.interface.total.bytes_per_sec",
        snap.net.iter().map(|n| n.name.clone()).collect(),
        snap.net
            .iter()
            .map(|n| {
                format!(
                    "{:.6}",
                    derived
                        .net_total_bytes_per_sec
                        .get(&n.name)
                        .copied()
                        .unwrap_or(0.0)
                )
            })
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.network.interface.in.packets_per_sec",
        snap.net.iter().map(|n| n.name.clone()).collect(),
        snap.net
            .iter()
            .map(|n| {
                format!(
                    "{:.6}",
                    derived
                        .net_rx_packets_per_sec
                        .get(&n.name)
                        .copied()
                        .unwrap_or(0.0)
                )
            })
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.network.interface.out.packets_per_sec",
        snap.net.iter().map(|n| n.name.clone()).collect(),
        snap.net
            .iter()
            .map(|n| {
                format!(
                    "{:.6}",
                    derived
                        .net_tx_packets_per_sec
                        .get(&n.name)
                        .copied()
                        .unwrap_or(0.0)
                )
            })
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.network.interface.in.errors_per_sec",
        snap.net.iter().map(|n| n.name.clone()).collect(),
        snap.net
            .iter()
            .map(|n| {
                format!(
                    "{:.6}",
                    derived.net_rx_errs_per_sec.get(&n.name).copied().unwrap_or(0.0)
                )
            })
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.network.interface.out.errors_per_sec",
        snap.net.iter().map(|n| n.name.clone()).collect(),
        snap.net
            .iter()
            .map(|n| {
                format!(
                    "{:.6}",
                    derived.net_tx_errs_per_sec.get(&n.name).copied().unwrap_or(0.0)
                )
            })
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.network.interface.in.drops_per_sec",
        snap.net.iter().map(|n| n.name.clone()).collect(),
        snap.net
            .iter()
            .map(|n| {
                format!(
                    "{:.6}",
                    derived.net_rx_drop_per_sec.get(&n.name).copied().unwrap_or(0.0)
                )
            })
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.network.interface.out.drops_per_sec",
        snap.net.iter().map(|n| n.name.clone()).collect(),
        snap.net
            .iter()
            .map(|n| {
                format!(
                    "{:.6}",
                    derived.net_tx_drop_per_sec.get(&n.name).copied().unwrap_or(0.0)
                )
            })
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.network.interface.in.loss_ratio",
        snap.net.iter().map(|n| n.name.clone()).collect(),
        snap.net
            .iter()
            .map(|n| {
                format!(
                    "{:.6}",
                    derived.net_rx_loss_ratio.get(&n.name).copied().unwrap_or(0.0)
                )
            })
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.network.interface.out.loss_ratio",
        snap.net.iter().map(|n| n.name.clone()).collect(),
        snap.net
            .iter()
            .map(|n| {
                format!(
                    "{:.6}",
                    derived.net_tx_loss_ratio.get(&n.name).copied().unwrap_or(0.0)
                )
            })
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.network.interface.mtu",
        snap.net.iter().map(|n| n.name.clone()).collect(),
        snap.net
            .iter()
            .map(|n| n.mtu.map(|v| v.to_string()).unwrap_or_else(|| "-".to_string()))
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.network.interface.speed_mbps",
        snap.net.iter().map(|n| n.name.clone()).collect(),
        snap.net
            .iter()
            .map(|n| {
                n.speed_mbps
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".to_string())
            })
            .collect(),
    );
    register_vector(
        &mut registry,
        "system.network.interface.up",
        snap.net.iter().map(|n| n.name.clone()).collect(),
        snap.net
            .iter()
            .map(|n| n.carrier_up.map(u64::from).unwrap_or(0).to_string())
            .collect(),
    );

    registry
}

pub fn render_metric_header(view: &MetricView) {
    println!();
    let mut headers = vec!["timestamp".to_string()];
    headers.extend(view.columns.iter().cloned());
    println!("{}", headers.join("  "));
    println!();
}

pub fn render_metric_row(view: &MetricView) {
    let mut headers = vec!["timestamp".to_string()];
    headers.extend(view.columns.iter().cloned());
    let mut row = vec![current_time_string()];
    row.extend(view.values.iter().cloned());
    let widths = column_widths(&headers, &row);
    println!("{}", format_row(&row, &widths));
}

pub fn list_metrics(snap: &Snapshot, derived: &DerivedMetrics) {
    for name in build_registry(snap, derived).keys() {
        println!("{name}");
    }
}
