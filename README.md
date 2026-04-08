<div align="center">

# Ojo - Lightweight OTel Collector

  <img src="assets/circle.png" alt="Ojo icon" width="150" />

  <p>
    <img src="https://img.shields.io/badge/Language-Rust-1f2937?style=flat-square&logo=rust&logoColor=white" alt="Language" />
    <img src="https://img.shields.io/badge/Telemetry-OpenTelemetry%20OTLP-0f766e?style=flat-square" alt="Telemetry" />
    <img src="https://img.shields.io/badge/Dashboards-Grafana-0ea5e9?style=flat-square&logo=grafana&logoColor=white" alt="Dashboards" />
    <img src="https://img.shields.io/badge/Services-Docker%20%7C%20GPU%20%7C%20Sensors%20%7C%20MySQL%20%7C%20Postgres%20%7C%20NFS%20%7C%20Systrace-7c3aed?style=flat-square" alt="Services" />
  </p>
  <p>
    <a href="DEPLOYMENT.md">
      <img src="https://img.shields.io/badge/🚀%20Deploy-Setup%20Guide-0ea5e9?style=flat-square&logo=docker&logoColor=white" alt="Deploy" />
    </a>
    <a href="#quick-start">
      <img src="https://img.shields.io/badge/⚡%20Run-Quick%20Start-16a34a?style=flat-square&logo=rust&logoColor=white" alt="Quick Start" />
    </a>
    <a href="grafana/windows.json">
      <img src="https://img.shields.io/badge/📊%20Grafana-Windows%20Dashboard-f59e0b?style=flat-square" alt="Grafana" />
    </a>
  </p>
</div>

Ojo is a lightweight host metrics agent written in Rust that collects system and process metrics and exports them via OpenTelemetry OTLP.

![Demo](assets/collector.gif)

## Supported Platforms

- Linux
- Windows
- Solaris _(in progress, platform-constrained, built, but not tested)_

## What Ojo Does

- Polls host metrics on a fixed interval
- Optionally collects per-process metrics
- Computes deltas and rates where applicable
- Exports to any OTLP-compatible backend directly or through an OpenTelemetry Collector
- Supports optional extension services for Docker, GPU, sensors, MySQL, Postgres, NFS client stats, and low-level systrace metrics/traces

## Optional extension services (sidecars)

These binaries are separate workspace crates under `services/<name>/`. Each runs independently, reads its own YAML (next to the binary or via `--config`), and exports OTLP metrics to the same endpoint as the main `ojo` agent. Build from the repo root with `cargo build -p <package> --release` or use `cargo run -p <package>` as below.

| Service | Cargo package | Example config | Metric prefix (examples) |
|--------|-----------------|----------------|--------------------------|
| Docker | `ojo-docker` | `services/docker/docker.yaml` | `system.docker.*` |
| GPU (NVIDIA) | `ojo-gpu` | `services/gpu/gpu.yaml` | `system.gpu.*` |
| Sensors | `ojo-sensors` | `services/sensors/sensors.yaml` | `system.sensor.*` |
| MySQL | `ojo-mysql` | `services/mysql/mysql.yaml` | `system.mysql.*` |
| Postgres | `ojo-postgres` | `services/postgres/postgres.yaml` | `system.postgres.*` |
| NFS client | `ojo-nfs-client` | `services/nfs-client/nfs-client.yaml` | `system.nfs_client.*` |
| Systrace | `ojo-systrace` | `services/systrace/systrace.yaml` | `system.systrace.*` |

Shared OTLP and filtering helpers live in `crates/host-collectors`. Grafana dashboards for each extension are under `grafana/` (`docker.json`, `gpu.json`, `sensors.json`, `mysql.json`, `postgres.json`, `nfs-client.json`).

Release archives include the main `ojo` agent and all sidecar services for Linux (`-unix`) and Windows (`-win`).

## Collected Metrics Reference

This section documents the metric families emitted by the core `ojo` host agent and the additional sidecar services.

### Core host agent (`ojo`) by platform

| Metric family / namespace | Linux metrics | Windows metrics | Solaris metrics |
|---|---|---|---|
| Metadata and QA classification | `system.metric.support_state`, `system.metric.classification` | `system.metric.support_state`, `system.metric.classification` | `system.metric.support_state`, `system.metric.classification` |
| CPU time and utilization | `system.cpu.time`, `system.cpu.utilization`, `system.cpu.core.utilization`, `system.cpu.core.system_ratio`, `system.cpu.core.iowait_ratio` | `system.cpu.time`, `system.cpu.utilization`, `system.cpu.core.utilization`, `system.cpu.core.system_ratio` | `system.cpu.time`, `system.cpu.utilization`, `system.cpu.core.utilization`, `system.cpu.core.system_ratio` |
| CPU/load averages | `system.cpu.load_average.1m`, `system.cpu.load_average.5m`, `system.cpu.load_average.15m`, `system.linux.load.runnable`, `system.linux.load.entities`, `system.linux.load.latest_pid` | `system.cpu.load_average.1m`, `system.cpu.load_average.5m`, `system.cpu.load_average.15m` | `system.cpu.load_average.1m`, `system.cpu.load_average.5m`, `system.cpu.load_average.15m` |
| Process counts and lifecycle | `system.process.count`, `system.process.created`, `system.processes.running`, `system.processes.blocked`, `system.processes.forks` | `system.process.count`, `system.process.created`, `system.processes.running` | `system.process.count`, `system.process.created`, `system.processes.running`, `system.processes.blocked`, `system.processes.forks` |
| Memory and swap (base) | `system.memory.total`, `system.memory.free`, `system.memory.available`, `system.swap.total`, `system.swap.free`, `system.memory.used_ratio`, `system.swap.used_ratio`, `system.memory.commit_limit`, `system.memory.committed_as` | `system.memory.total`, `system.memory.free`, `system.memory.available`, `system.swap.total`, `system.swap.free`, `system.memory.used_ratio`, `system.swap.used_ratio`, `system.memory.commit_limit`, `system.memory.committed_as` | `system.memory.total`, `system.memory.free`, `system.memory.available`, `system.swap.total`, `system.swap.free`, `system.memory.used_ratio`, `system.swap.used_ratio`, `system.memory.commit_limit`, `system.memory.committed_as` |
| Memory and swap (extended fields) | `system.memory.buffers`, `system.memory.active`, `system.memory.inactive`, `system.memory.slab`, `system.memory.hugepages_total`, `system.memory.hugepages_free`, `system.swap.cached`, `system.memory.dirty_writeback_ratio` | N/A | `system.memory.buffers`, `system.memory.active`, `system.memory.inactive`, `system.memory.slab`, `system.swap.cached` (collector-dependent) |
| Paging and swap rates | `system.paging.faults`, `system.paging.operations`, `system.swap.operations`, `system.paging.faults_per_sec`, `system.paging.major_faults_per_sec`, `system.paging.page_ins_per_sec`, `system.paging.page_outs_per_sec`, `system.swap.ins_per_sec`, `system.swap.outs_per_sec` | `system.paging.faults`, `system.paging.operations`, `system.swap.operations`, `system.paging.faults_per_sec`, `system.paging.major_faults_per_sec`, `system.paging.page_ins_per_sec`, `system.paging.page_outs_per_sec`, `system.swap.ins_per_sec`, `system.swap.outs_per_sec` | `system.paging.faults`, `system.paging.operations`, `system.swap.operations`, `system.paging.faults_per_sec`, `system.paging.major_faults_per_sec`, `system.paging.page_ins_per_sec`, `system.paging.page_outs_per_sec`, `system.swap.ins_per_sec`, `system.swap.outs_per_sec` |
| Disk throughput, latency, and queueing | `system.disk.io`, `system.disk.operations`, `system.disk.operation_time`, `system.disk.io_time`, `system.disk.read_bytes_per_sec`, `system.disk.write_bytes_per_sec`, `system.disk.total_bytes_per_sec`, `system.disk.ops_per_sec`, `system.disk.read_await`, `system.disk.write_await`, `system.disk.utilization`, `system.disk.queue_depth`, `system.disk.io_in_progress`, `system.disk.pending_operations` | `system.disk.io`, `system.disk.operations`, `system.disk.operation_time`, `system.disk.io_time`, `system.disk.read_bytes_per_sec`, `system.disk.write_bytes_per_sec`, `system.disk.total_bytes_per_sec`, `system.disk.ops_per_sec`, `system.disk.read_await`, `system.disk.write_await`, `system.disk.utilization`, `system.disk.queue_depth`, `system.disk.io_in_progress`, `system.disk.pending_operations` | `system.disk.io`, `system.disk.operations`, `system.disk.operation_time`, `system.disk.io_time`, `system.disk.read_bytes_per_sec`, `system.disk.write_bytes_per_sec`, `system.disk.total_bytes_per_sec`, `system.disk.ops_per_sec`, `system.disk.read_await`, `system.disk.write_await`, `system.disk.utilization`, `system.disk.queue_depth`, `system.disk.io_in_progress`, `system.disk.pending_operations` |
| Network throughput and errors | `system.network.io`, `system.network.packet.count`, `system.network.errors`, `system.network.packet.dropped`, `system.network.rx_bytes_per_sec`, `system.network.tx_bytes_per_sec`, `system.network.total_bytes_per_sec`, `system.network.rx_packets_per_sec`, `system.network.tx_packets_per_sec`, `system.network.rx_errors_per_sec`, `system.network.tx_errors_per_sec`, `system.network.rx_loss_ratio`, `system.network.tx_loss_ratio`, `system.network.mtu` | `system.network.io`, `system.network.packet.count`, `system.network.errors`, `system.network.packet.dropped`, `system.network.rx_bytes_per_sec`, `system.network.tx_bytes_per_sec`, `system.network.total_bytes_per_sec`, `system.network.rx_packets_per_sec`, `system.network.tx_packets_per_sec`, `system.network.rx_errors_per_sec`, `system.network.tx_errors_per_sec`, `system.network.rx_loss_ratio`, `system.network.tx_loss_ratio`, `system.network.mtu` | `system.network.io`, `system.network.packet.count`, `system.network.errors`, `system.network.packet.dropped`, `system.network.rx_bytes_per_sec`, `system.network.tx_bytes_per_sec`, `system.network.total_bytes_per_sec`, `system.network.rx_packets_per_sec`, `system.network.tx_packets_per_sec`, `system.network.rx_errors_per_sec`, `system.network.tx_errors_per_sec`, `system.network.rx_loss_ratio`, `system.network.tx_loss_ratio`, `system.network.mtu` |
| Filesystem and mount inventory | `system.filesystem.usage`, `system.filesystem.mount.state` | `system.filesystem.usage`, `system.filesystem.mount.state` | `system.filesystem.usage`, `system.filesystem.mount.state` |
| Per-process common metrics | `process.cpu.time`, `process.cpu.utilization`, `process.memory.rss`, `process.memory.usage`, `process.thread.count`, `process.start_time`, `process.io.read_bytes`, `process.io.write_bytes`, `process.priority`, `process.memory.vm_size` | `process.cpu.time`, `process.cpu.utilization`, `process.memory.rss`, `process.memory.usage`, `process.thread.count`, `process.start_time`, `process.io.read_bytes`, `process.io.write_bytes`, `process.priority`, `process.memory.vm_size` | `process.cpu.time`, `process.cpu.utilization`, `process.memory.rss`, `process.memory.usage`, `process.thread.count`, `process.start_time`, `process.io.read_bytes`, `process.io.write_bytes`, `process.priority`, `process.memory.vm_size` |
| Linux process extensions | `process.linux.nice`, `process.linux.io.cancelled_write_bytes`, `process.linux.start_time`, `process.linux.scheduler`, `process.oom_score` | N/A | N/A |
| Windows process extensions | N/A | `process.memory.working_set`, `process.memory.peak_working_set`, `process.memory.pagefile_usage`, `process.memory.private_bytes`, `process.memory.commit_charge` | N/A |
| Linux kernel/proc namespaces | `system.linux.pid.max`, `system.linux.entropy`, `system.linux.pressure`, `system.linux.pressure.stall_time`, `system.linux.schedstat`, `system.linux.runqueue.depth`, `system.linux.slab`, `system.linux.cgroup`, `system.linux.swap.device.size`, `system.linux.zoneinfo`, `system.linux.buddy.blocks`, `system.linux.net.snmp`, `system.linux.netstat`, `system.linux.net.softnet.drop_ratio` | N/A | N/A |
| Windows kernel namespaces | N/A | `system.windows.vmstat`, `system.windows.net.snmp`, `system.windows.interrupts`, `system.windows.dpc` | N/A |

Notes:
- Linux has the broadest namespace coverage (including `system.linux.*` and `process.linux.*`).
- Windows omits Linux-only families and publishes Windows-specific families under `system.windows.*`.
- Solaris currently uses the cross-platform families plus whichever optional fields are available from the Solaris collector.
- Current QA snapshot fixtures under `tests/qa/` are Linux-focused; Windows and Solaris rows reflect the recorder/collector implementation paths.

#### Deep Audit: Additional Core Metrics By Platform

The summary table above is grouped by family. The lists below complete the inventory so every currently emitted core metric name is documented.

Linux additional metrics:
`process.context_switches`, `process.cpu.last_id`, `process.disk.io`, `process.io.chars`, `process.io.syscalls`, `process.memory.virtual`, `process.memory.vm_rss`, `process.paging.faults`, `process.parent_pid`, `process.unix.file_descriptor.count`, `system.boot.time`, `system.context_switches`, `system.cpu.cache.size`, `system.cpu.frequency`, `system.cpu.info`, `system.cpu.interrupts`, `system.cpu.softirqs`, `system.disk.avg_read_size`, `system.disk.avg_write_size`, `system.disk.logical_block_size`, `system.disk.physical_block_size`, `system.disk.read_ops_per_sec`, `system.disk.rotational`, `system.disk.time_in_progress`, `system.disk.time_reading`, `system.disk.time_writing`, `system.disk.weighted_time_in_progress`, `system.disk.write_ops_per_sec`, `system.linux.interrupts`, `system.linux.net.ip.in_discards_per_sec`, `system.linux.net.ip.out_discards_per_sec`, `system.linux.net.softnet.cpu.dropped`, `system.linux.net.softnet.cpu.processed`, `system.linux.net.softnet.cpu.time_squeezed`, `system.linux.net.softnet.dropped_per_sec`, `system.linux.net.softnet.processed_per_sec`, `system.linux.net.softnet.time_squeezed_per_sec`, `system.linux.net.tcp.retrans_segs_per_sec`, `system.linux.net.udp.in_errors_per_sec`, `system.linux.net.udp.rcvbuf_errors_per_sec`, `system.linux.softirqs`, `system.linux.swap.device.priority`, `system.linux.swap.device.used`, `system.linux.vmstat`, `system.memory.anon`, `system.memory.anon_hugepages`, `system.memory.cached`, `system.memory.dirty`, `system.memory.hugepage_size`, `system.memory.kernel_stack`, `system.memory.mapped`, `system.memory.page_tables`, `system.memory.shmem`, `system.memory.sreclaimable`, `system.memory.sunreclaim`, `system.memory.writeback`, `system.network.carrier_up`, `system.network.rx_compressed`, `system.network.rx_dropped`, `system.network.rx_drops_per_sec`, `system.network.rx_errors`, `system.network.rx_fifo`, `system.network.rx_frame`, `system.network.rx_multicast`, `system.network.rx_packets`, `system.network.speed`, `system.network.tx_carrier`, `system.network.tx_collisions`, `system.network.tx_compressed`, `system.network.tx_dropped`, `system.network.tx_drops_per_sec`, `system.network.tx_errors`, `system.network.tx_fifo`, `system.network.tx_packets`, `system.network.tx_queue_len`, `system.socket.count`, `system.uptime`.

Windows additional metrics:
`process.context_switches`, `process.disk.io`, `process.io.chars`, `process.io.syscalls`, `process.paging.faults`, `process.parent_pid`, `process.unix.file_descriptor.count`, `system.boot.time`, `system.context_switches`, `system.cpu.cache.size`, `system.cpu.frequency`, `system.cpu.info`, `system.cpu.interrupts`, `system.cpu.softirqs`, `system.disk.avg_read_size`, `system.disk.avg_write_size`, `system.disk.logical_block_size`, `system.disk.physical_block_size`, `system.disk.read_ops_per_sec`, `system.disk.rotational`, `system.disk.time_in_progress`, `system.disk.time_reading`, `system.disk.time_writing`, `system.disk.weighted_time_in_progress`, `system.disk.write_ops_per_sec`, `system.memory.cached`, `system.network.carrier_up`, `system.network.rx_compressed`, `system.network.rx_dropped`, `system.network.rx_drops_per_sec`, `system.network.rx_errors`, `system.network.rx_fifo`, `system.network.rx_frame`, `system.network.rx_multicast`, `system.network.rx_packets`, `system.network.speed`, `system.network.tx_carrier`, `system.network.tx_collisions`, `system.network.tx_compressed`, `system.network.tx_dropped`, `system.network.tx_drops_per_sec`, `system.network.tx_errors`, `system.network.tx_fifo`, `system.network.tx_packets`, `system.network.tx_queue_len`, `system.socket.count`, `system.uptime`.

Solaris additional metrics:
`process.context_switches`, `process.cpu.last_id`, `process.disk.io`, `process.io.chars`, `process.io.syscalls`, `process.memory.virtual`, `process.memory.vm_rss`, `process.paging.faults`, `process.parent_pid`, `process.unix.file_descriptor.count`, `system.boot.time`, `system.context_switches`, `system.cpu.cache.size`, `system.cpu.frequency`, `system.cpu.info`, `system.cpu.interrupts`, `system.cpu.softirqs`, `system.disk.avg_read_size`, `system.disk.avg_write_size`, `system.disk.logical_block_size`, `system.disk.physical_block_size`, `system.disk.read_ops_per_sec`, `system.disk.rotational`, `system.disk.time_in_progress`, `system.disk.time_reading`, `system.disk.time_writing`, `system.disk.weighted_time_in_progress`, `system.disk.write_ops_per_sec`, `system.linux.vmstat`, `system.memory.anon`, `system.memory.anon_hugepages`, `system.memory.cached`, `system.memory.dirty`, `system.memory.hugepage_size`, `system.memory.kernel_stack`, `system.memory.mapped`, `system.memory.page_tables`, `system.memory.shmem`, `system.memory.sreclaimable`, `system.memory.sunreclaim`, `system.memory.writeback`, `system.network.carrier_up`, `system.network.rx_compressed`, `system.network.rx_dropped`, `system.network.rx_drops_per_sec`, `system.network.rx_errors`, `system.network.rx_fifo`, `system.network.rx_frame`, `system.network.rx_multicast`, `system.network.rx_packets`, `system.network.speed`, `system.network.tx_carrier`, `system.network.tx_collisions`, `system.network.tx_compressed`, `system.network.tx_dropped`, `system.network.tx_drops_per_sec`, `system.network.tx_errors`, `system.network.tx_fifo`, `system.network.tx_packets`, `system.network.tx_queue_len`, `system.socket.count`, `system.uptime`.

### Additional sidecar services (contracted metrics)

The extension metric contracts are validated by `tests/qa_extension_metric_contracts.rs`.

| Service | Namespace | Collected metrics (name -> semantic) |
|---|---|---|
| Docker (`ojo-docker`) | `system.docker.*` | `system.docker.containers.total -> gauge`<br>`system.docker.containers.running -> gauge`<br>`system.docker.containers.stopped -> gauge`<br>`system.docker.container.cpu.ratio -> gauge_ratio`<br>`system.docker.container.memory.usage.bytes -> gauge`<br>`system.docker.container.memory.limit.bytes -> gauge`<br>`system.docker.container.network.rx.bytes -> gauge`<br>`system.docker.container.network.tx.bytes -> gauge`<br>`system.docker.container.block.read.bytes -> gauge`<br>`system.docker.container.block.write.bytes -> gauge`<br>`system.docker.source.available -> state` |
| GPU (`ojo-gpu`) | `system.gpu.*` | `system.gpu.devices -> inventory`<br>`system.gpu.utilization.ratio -> gauge_ratio`<br>`system.gpu.memory.used.bytes -> gauge`<br>`system.gpu.memory.total.bytes -> gauge`<br>`system.gpu.temperature.celsius -> gauge`<br>`system.gpu.power.watts -> gauge`<br>`system.gpu.throttled -> state`<br>`system.gpu.source.available -> state` |
| Sensors (`ojo-sensors`) | `system.sensor.*` | `system.sensor.temperature.celsius -> gauge`<br>`system.sensor.temperature.max.celsius -> gauge`<br>`system.sensor.fan.rpm -> gauge`<br>`system.sensor.voltage.volts -> gauge`<br>`system.sensor.count -> inventory`<br>`system.sensor.source.available -> state` |
| MySQL (`ojo-mysql`) | `system.mysql.*` | `system.mysql.source.available -> state`<br>`system.mysql.up -> state`<br>`system.mysql.connections -> gauge`<br>`system.mysql.threads.running -> gauge`<br>`system.mysql.queries.total -> counter`<br>`system.mysql.slow_queries.total -> counter`<br>`system.mysql.bytes.received.total -> counter`<br>`system.mysql.bytes.sent.total -> counter`<br>`system.mysql.queries.rate_per_second -> gauge_derived`<br>`system.mysql.bytes.received.rate_per_second -> gauge_derived`<br>`system.mysql.bytes.sent.rate_per_second -> gauge_derived` |
| Postgres (`ojo-postgres`) | `system.postgres.*` | `system.postgres.source.available -> state`<br>`system.postgres.up -> state`<br>`system.postgres.connections -> gauge`<br>`system.postgres.transactions.committed.total -> counter`<br>`system.postgres.transactions.rolled_back.total -> counter`<br>`system.postgres.deadlocks.total -> counter`<br>`system.postgres.blocks.read.total -> counter`<br>`system.postgres.blocks.hit.total -> counter`<br>`system.postgres.transactions.committed.rate_per_second -> gauge_derived`<br>`system.postgres.transactions.rolled_back.rate_per_second -> gauge_derived` |
| NFS client (`ojo-nfs-client`) | `system.nfs_client.*` | `system.nfs_client.source.available -> state`<br>`system.nfs_client.mounts -> inventory`<br>`system.nfs_client.rpc.calls.total -> counter`<br>`system.nfs_client.rpc.retransmissions.total -> counter`<br>`system.nfs_client.rpc.auth_refreshes.total -> counter`<br>`system.nfs_client.rpc.calls.rate_per_second -> gauge_derived`<br>`system.nfs_client.rpc.retransmissions.rate_per_second -> gauge_derived` |
| Systrace (`ojo-systrace`) | `system.systrace.*` | `system.systrace.source.available -> state`<br>`system.systrace.up -> state`<br>`system.systrace.tracefs.available -> state`<br>`system.systrace.etw.available -> state`<br>`system.systrace.tracing.on -> state`<br>`system.systrace.tracers.available -> inventory`<br>`system.systrace.events.total -> counter`<br>`system.systrace.events.enabled -> counter`<br>`system.systrace.buffer.total_kb -> gauge`<br>`system.systrace.etw.sessions.total -> gauge`<br>`system.systrace.etw.sessions.running -> gauge`<br>`system.systrace.exporter.available -> state`<br>`system.systrace.exporter.reconnecting -> state`<br>`system.systrace.exporter.errors.total -> counter`<br>`system.systrace.context_switches_per_sec -> gauge_derived`<br>`system.systrace.interrupts_per_sec -> gauge_derived`<br>`system.systrace.system_calls_per_sec -> gauge_derived`<br>`system.systrace.system_calls.source -> inventory`<br>`system.systrace.system_calls.coverage_ratio -> gauge_ratio`<br>`system.systrace.dpcs_per_sec -> gauge_derived`<br>`system.systrace.process_forks_per_sec -> gauge_derived`<br>`system.systrace.run_queue.depth -> gauge_approximation`<br>`system.systrace.processes.total -> gauge`<br>`system.systrace.threads.total -> gauge`<br>`system.systrace.trace.kernel_stack_samples.total -> counter`<br>`system.systrace.trace.user_stack_samples.total -> counter`<br>`system.systrace.collection.errors -> counter` |

Allowed semantic kinds for extensions are: `counter`, `gauge`, `gauge_approximation`, `gauge_derived`, `gauge_derived_ratio`, `gauge_ratio`, `inventory`, and `state`.

## Repository Layout

```
src/main.rs                  Runtime loop and exporter flush behavior
src/config.rs                YAML/env config loader and validation
src/linux/                   Linux collector modules
src/windows/                 Windows collector modules
src/solaris/                 Solaris collector modules
src/delta.rs                 Delta and rate derivation
src/metrics/                 OTel metric instruments and recording
linux.yaml                   Linux config example
windows.yaml                 Windows config example
services/docker/docker.yaml  Docker extension config example
services/gpu/gpu.yaml        GPU extension config example
services/sensors/sensors.yaml Sensor extension config example
services/mysql/mysql.yaml    MySQL extension config example
services/postgres/postgres.yaml Postgres extension config example
services/nfs-client/nfs-client.yaml NFS client extension config example
services/systrace/systrace.yaml Systrace extension config example
grafana/docker.json          Docker dashboard
grafana/gpu.json             GPU dashboard
grafana/sensors.json         Sensors dashboard
grafana/mysql.json           MySQL dashboard
grafana/postgres.json        Postgres dashboard
grafana/nfs-client.json      NFS client dashboard
grafana/systrace.json        Systrace dashboard
otel.yaml                    OpenTelemetry Collector example
tests/qa_json_schema.rs      QA snapshot schema tests
tests/qa_platform_metric_contracts.rs  Platform metric namespace tests
tests/qa_extension_metric_contracts.rs Extension metric contract tests
services/docker/             Docker sidecar service crate
services/gpu/                GPU sidecar service crate
services/sensors/            Sensor sidecar service crate
services/mysql/              MySQL sidecar service crate
services/postgres/           Postgres sidecar service crate
services/nfs-client/         NFS client sidecar service crate
services/systrace/           Systrace sidecar service crate
crates/host-collectors/      Shared OTLP and metric helper crate
docker.dev/                  QA Dockerfiles and Compose services
```

## Quick Start

**1. Pick a config**

Use one of the included examples: `linux.yaml` or `windows.yaml`.

**2. Run**

```bash
cargo run -- --config linux.yaml
```

```bash
cargo run -- --config windows.yaml
```

**Optional extension services**

```bash
cargo run -p ojo-docker -- --config services/docker/docker.yaml
```

```bash
cargo run -p ojo-gpu -- --config services/gpu/gpu.yaml
```

```bash
cargo run -p ojo-sensors -- --config services/sensors/sensors.yaml
```

```bash
cargo run -p ojo-mysql -- --config services/mysql/mysql.yaml
```

```bash
cargo run -p ojo-postgres -- --config services/postgres/postgres.yaml
```

```bash
cargo run -p ojo-nfs-client -- --config services/nfs-client/nfs-client.yaml
```

```bash
cargo run -p ojo-systrace -- --config services/systrace/systrace.yaml
```

Each extension can run independently and send OTLP metrics to the same collector endpoint as `ojo`.

You can also run from each service folder:

```bash
cd services/docker && cargo run -- --config docker.yaml
cd services/gpu && cargo run -- --config gpu.yaml
cd services/sensors && cargo run -- --config sensors.yaml
cd services/mysql && cargo run -- --config mysql.yaml
cd services/postgres && cargo run -- --config postgres.yaml
cd services/nfs-client && cargo run -- --config nfs-client.yaml
cd services/systrace && cargo run -- --config systrace.yaml
```

**3. Dump a snapshot for debugging**

```bash
cargo run -- --config linux.yaml --dump-snapshot
```

## Configuration

```yaml
service:
  name: linux
  instance_id: linux-0001

collection:
  poll_interval_secs: 5
  include_process_metrics: true
  # Low-cardinality defaults for process labels.
  process_include_pid_label: false
  process_include_command_label: true
  process_include_state_label: true

export:
  otlp:
    endpoint: "http://127.0.0.1:4355/v1/metrics"
    protocol: http/protobuf
    compression: gzip
    timeout_secs: 10
  batch:
    interval_secs: 5
    timeout_secs: 10
```

## Metric Selection

If `metrics` is omitted, all metrics are exported.

```yaml
metrics:
  include: [system., process.]
  exclude: [process.linux.]
```

Rules:
- `include` and `exclude` are prefix-based
- `exclude` wins over `include`
- an empty `include` means include all metrics

Extension naming guidance:
- Docker metrics use `system.docker.*`
- GPU metrics use `system.gpu.*`
- Sensor metrics use `system.sensor.*`
- MySQL metrics use `system.mysql.*`
- Postgres metrics use `system.postgres.*`
- NFS client metrics use `system.nfs_client.*`
- Systrace metrics use `system.systrace.*`
- Keep custom extensions under `system.*` / `process.*` to preserve QA naming contracts

## Environment Variables

| Variable | Description |
|---|---|
| `PROC_OTEL_CONFIG` | Config file path override |
| `PROC_POLL_INTERVAL_SECS` | Poll interval override |
| `PROC_INCLUDE_PROCESS_METRICS` | Enable process metrics |
| `PROC_PROCESS_INCLUDE_PID_LABEL` | Include `process.pid` attribute on per-process metrics |
| `PROC_PROCESS_INCLUDE_COMMAND_LABEL` | Include `process.command` attribute on per-process metrics |
| `PROC_PROCESS_INCLUDE_STATE_LABEL` | Include `process.state` attribute on per-process metrics |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP endpoint |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | OTLP protocol |
| `OTEL_EXPORTER_OTLP_HEADERS` | OTLP headers |
| `OTEL_EXPORTER_OTLP_COMPRESSION` | OTLP compression |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | OTLP timeout |

## OpenTelemetry Collector

A sample collector config is included in `otel.yaml`.

1. Start the collector
2. Point `export.otlp.endpoint` to its OTLP endpoint
3. Run Ojo and any extension sidecars you need

Suggested deployment patterns:
- Single host: run `ojo` + optional sidecars directly on the host
- Containerized host monitoring: run one sidecar per host domain (docker/gpu/sensors/mysql/postgres/nfs-client/systrace)
- Centralized backend: route all producers through the same OTel Collector

## Docker QA

```bash
docker compose -f docker.dev/docker-compose.yml run --rm qa-ubuntu-2204
```
```bash
docker compose up -d
# if you want to run all the containers
```

## Build and Release

Release details, artifact matrix, and download/run examples are in `DEPLOYMENT.md`.

## Build and Validate

```bash
cargo check
cargo check --workspace
cargo check --workspace --target x86_64-pc-windows-gnu
cargo test
cargo test --test qa_extension_metric_contracts
cargo llvm-cov -p host-collectors --all-features --summary-only --fail-under-lines 100
```

Cross-checking to `x86_64-pc-windows-gnu` from Linux requires a MinGW toolchain (e.g. Debian/Ubuntu: `sudo apt-get install gcc-mingw-w64-x86-64`) so crates like `ring` can compile C code for the Windows target.

## Platform Notes

- Linux-only metrics are omitted on Windows by design
- Unsupported metrics are omitted rather than zeroed
- Windows uses `windows.load.synthetic.*` instead of Linux loadavg
- Windows process handle count is mapped for open-file-descriptor compatibility

## Troubleshooting

**No metrics exported**
- Verify `endpoint` and `protocol` match your backend (`grpc` vs `http/protobuf`)
- Check collector or backend availability
- Enable debug logging: `RUST_LOG=debug cargo run -- --config linux.yaml`

**Process metrics missing**
- Ensure `collection.include_process_metrics: true`
- Verify process permissions
- Check that `metrics` filters are not excluding `process.`
