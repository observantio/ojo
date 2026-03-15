# Ojo - Lightweight OpenTelemetry Host Metrics Agent

Ojo is a small Rust-based system metrics agent that collects host and process telemetry and exports it using OpenTelemetry OTLP.

![Demo Ojo](assets/OJO.gif)

It supports Linux and Windows, with platform-specific collectors under the hood, and can send metrics to any OTLP-compatible receiver (for example, OpenTelemetry Collector).

## What Ojo Collects

Ojo focuses on:

- `system.*` metrics (CPU, memory, disk, network, load, paging)
- `process.*` metrics (optional, controlled by config)

The collector computes delta/rate metrics between polling intervals where appropriate.

## Repository Layout

- `src/main.rs`: agent loop, polling, recording, flush, shutdown
- `src/config.rs`: config loading and environment mapping
- `src/linuxcollect.rs`: Linux host/process collection
- `src/wincollect.rs`: Windows host/process collection
- `src/delta.rs`: rate/delta derivation logic
- `src/metrics.rs`: OpenTelemetry instrument creation and recording
- `linux.yaml`: sample Linux agent config
- `windows.yaml`: sample Windows agent config
- `otel.yaml`: sample OpenTelemetry Collector pipeline
- `grafana/ojo.json`: sample Grafana dashboard

## Prerequisites

1. Rust toolchain (`cargo`, `rustc`) installed.
2. Network connectivity from Ojo to your OTLP endpoint.
3. If using process metrics:
- Linux: permissions to read `/proc` data for target processes.
- Windows: run with sufficient privileges to query process/system APIs.

## Quick Start

### 1. Choose or edit config

Use one of the included config files:

- `linux.yaml`
- `windows.yaml`

Set at least:

- `export.otlp.endpoint`
- `export.otlp.protocol`

For HTTP OTLP, endpoint typically includes a path like `/v1/metrics`.

### 2. Run Ojo

Run with explicit config path:

```bash
# Linux
cargo run -- --config linux.yaml

# Windows
cargo run -- --config windows.yaml
```

This is the recommended way to run Ojo for development

### 2b. Build an optimized binary first (release mode)

If you want maximum runtime performance, build the optimized binary first:

```bash
cargo build --release
```

Then run the compiled binary directly:

```bash
# Linux
./target/release/ojo --config linux.yaml

# Windows (PowerShell or CMD)
target\\release\\ojo.exe --config windows.yaml
```

If you still prefer `cargo run`, use release mode so it builds/runs optimized code:

```bash
# Linux
cargo run --release -- --config linux.yaml

# Windows
cargo run --release -- --config windows.yaml
```

### 3. Optional: run with default config name

If `--config` is not provided, Ojo looks for:

- `PROC_OTEL_CONFIG` env var, otherwise
- `ojo.yaml`

Example:

```bash
PROC_OTEL_CONFIG=linux.yaml cargo run
```

## Configuration Reference

Top-level sections:

- `service`
- `collection`
- `export`
- `metrics`

### service

```yaml
service:
  name: linux
  instance_id: linux-0001
```

- `name`: exported as service name.
- `instance_id`: unique ID for host/agent instance.

### collection

```yaml
collection:
  poll_interval_secs: 5
  include_process_metrics: true
```

- `poll_interval_secs`: polling cadence.
- `include_process_metrics`: enable/disable process metrics.

### export

```yaml
export:
  otlp:
    endpoint: "http://127.0.0.1:4317"
    protocol: grpc
    headers:
      x-otlp-token: "token"
    compression: gzip
    timeout_secs: 10
  batch:
    interval_secs: 5
    timeout_secs: 10
```

`otlp` fields:

- `endpoint`: OTLP endpoint URL.
- `protocol`: `grpc` or `http/protobuf`.
- `token` and `token_header`: convenience auth header config.
- `headers`: additional static OTLP headers.
- `compression`: exporter compression.
- `timeout_secs`: OTLP export timeout.

`batch` fields:

- `interval_secs`: maps to `OTEL_METRIC_EXPORT_INTERVAL` (milliseconds internally).
- `timeout_secs`: maps to `OTEL_METRIC_EXPORT_TIMEOUT` (milliseconds internally).

### metrics

```yaml
metrics:
  include:
    - system.
    - process.
  exclude:
    - system.linux.
```

- Prefix-based filtering.
- `include` empty means include all.
- `exclude` always wins over include.

## OpenTelemetry Collector Example

`otel.yaml` in this repo is an example pipeline that:

1. Receives OTLP metrics over HTTP (`:4355`) and gRPC (`:4356`).
2. Applies memory limiter and batch processors.
3. Exports using Prometheus remote write.

Start collector with your preferred distribution, for example:

```bash
otelcol --config otel.yaml
```

Then point Ojo config endpoint to collector HTTP:

```yaml
export:
  otlp:
    endpoint: "http://<collector-host>:4355/v1/metrics"
    protocol: http/protobuf
```

## Logging and Runtime Behavior

- Default log level is `info`.
- Override with `RUST_LOG`, for example:

```bash
RUST_LOG=debug cargo run -- --config linux.yaml
```

- On successful export connectivity, Ojo logs `Connected Successfully`.
- On transient export failure, Ojo logs reconnect warnings and retries on next poll.
- `Ctrl+C` triggers graceful shutdown.

## Platform Notes

Ojo intentionally avoids forcing fake values for unsupported metrics.

- Linux-only metrics are emitted on Linux.
- On Windows, unsupported Linux-specific fields are omitted (no data) rather than emitted as `0`.
- If Windows disk performance counters are unavailable for a disk, disk rate/pending/time metrics for that disk are omitted.

This helps dashboards distinguish "real zero" from "metric not available on this platform".

## Troubleshooting

### No metrics arriving

1. Verify Ojo is running with the expected config file.
2. Check endpoint/protocol match (`grpc` vs `http/protobuf`).
3. Confirm collector is listening on the configured host/port.
4. Set `RUST_LOG=debug` and inspect export/flush logs.

### Windows shows missing Linux metrics

Expected behavior. Unsupported Linux-specific metrics are omitted by design.

### Process metrics are empty

1. Ensure `include_process_metrics: true`.
2. Check runtime permissions.
3. Verify include/exclude filters are not removing `process.*`.

## Development

Build:

```bash
cargo build
```

Run tests (if present):

```bash
cargo test
```

Format/lint (if configured in your environment):

```bash
cargo fmt
cargo clippy --all-targets --all-features
```

## Example Commands

```bash
# Linux run
cargo run -- --config linux.yaml

# Windows run
cargo run -- --config windows.yaml

# Use env var-based config selection
PROC_OTEL_CONFIG=windows.yaml cargo run

# Debug logging
RUST_LOG=debug cargo run -- --config linux.yaml
```
