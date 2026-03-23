# Ojo — OpenTelemetry Host Metrics Agent

Ojo is a lightweight host metrics agent written in Rust that collects system and process metrics and exports them via OpenTelemetry OTLP.

![Demo](assets/collector.gif)

## Supported Platforms

- Linux
- Windows
- Solaris _(in progress, platform-constrained)_

## What Ojo Does

- Polls host metrics on a fixed interval
- Optionally collects per-process metrics
- Computes deltas and rates where applicable
- Exports to any OTLP-compatible backend directly or through an OpenTelemetry Collector
- Supports optional extension services for Docker, GPU, sensors, MySQL, Postgres, and NFS client stats

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

Shared OTLP and filtering helpers live in `crates/host-collectors`. Grafana dashboards for each extension are under `grafana/` (`docker.json`, `gpu.json`, `sensors.json`, `mysql.json`, `postgres.json`, `nfs-client.json`).

Release archives for the **main** `ojo` agent list the core binary only; to run an extension, build from source or add the corresponding crate to your release pipeline.

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
grafana/docker.json          Docker dashboard
grafana/gpu.json             GPU dashboard
grafana/sensors.json         Sensors dashboard
grafana/mysql.json           MySQL dashboard
grafana/postgres.json        Postgres dashboard
grafana/nfs-client.json      NFS client dashboard
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

Each extension can run independently and send OTLP metrics to the same collector endpoint as `ojo`.

You can also run from each service folder:

```bash
cd services/docker && cargo run -- --config docker.yaml
cd services/gpu && cargo run -- --config gpu.yaml
cd services/sensors && cargo run -- --config sensors.yaml
cd services/mysql && cargo run -- --config mysql.yaml
cd services/postgres && cargo run -- --config postgres.yaml
cd services/nfs-client && cargo run -- --config nfs-client.yaml
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
- Containerized host monitoring: run one sidecar per host domain (docker/gpu/sensors)
- Containerized host monitoring: run one sidecar per host domain (docker/gpu/sensors/mysql/postgres/nfs-client)
- Centralized backend: route all producers through the same OTel Collector

## Docker QA

```bash
docker compose -f docker.dev/docker-compose.yml run --rm qa-ubuntu-2204
```

```bash
docker compose -f docker.dev/docker-compose.yml run --rm qa-windows-2022-gnu
```

The Windows GNU check writes output to `tests/qa/windows-2022-check.json`.

## Build and Release

Releases are published via `.github/workflows/ci.yml` on `v*` tag push or manual dispatch.

**Artifacts built on every release:**

| Artifact | Target |
|---|---|
| `ojo-{version}-linux-x86_64` | `x86_64-unknown-linux-gnu` |
| `ojo-{version}-linux-aarch64` | `aarch64-unknown-linux-gnu` |
| `ojo-{version}-windows-x86_64.exe` | `x86_64-pc-windows-gnu` |

**Optional legacy artifacts (manual dispatch only):**

| Artifact | Target |
|---|---|
| `ojo-{version}-linux-x86_64-legacy` | x86_64 Linux, SSE4.2/AVX/AVX2 disabled |
| `ojo-{version}-windows-i686-legacy.exe` | `i686-pc-windows-gnu` |

**Download and run — Linux:**

```bash
curl -L https://github.com/observantio/ojo/releases/download/v0.0.1/ojo-v0.0.1-linux-aarch64 -o ojo
chmod +x ojo
./ojo --config linux.yaml
```

**Download and run — Windows (PowerShell):**

```powershell
Invoke-WebRequest -Uri https://github.com/observantio/ojo/releases/download/v0.0.1/ojo-v0.0.1-windows-x86_64.exe -OutFile ojo.exe
.\ojo.exe --config windows.yaml
```

## Build and Validate

```bash
cargo check
cargo check --workspace
cargo check --workspace --target x86_64-pc-windows-gnu
cargo test
cargo test --test qa_extension_metric_contracts
cargo llvm-cov -p host-collectors --all-features --all-targets --summary-only --fail-under-lines 70
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
