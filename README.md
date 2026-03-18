# Ojo - Powerful OpenTelemtry Agent for Deep Analaysis

Ojo is a lightweight host metrics agent written in Rust.
It collects system and process metrics and exports them via OpenTelemetry OTLP.

Supported collectors:
- Linux
- Windows
- Solaris (in-progress and platform-constrained)

![Demo Ojo](assets/collector.gif)

## What Ojo Does

- Polls host metrics on a fixed interval
- Optionally includes per-process metrics
- Computes deltas/rates where needed
- Exports to any OTLP-compatible backend (directly or through OpenTelemetry Collector)

## Repository Layout

- `src/main.rs`: runtime loop, flush/reconnect behavior
- `src/config.rs`: YAML/env config loader and validation
- `src/linuxcollect.rs`: Linux collector
- `src/wincollect.rs`: Windows collector
- `src/solarcollect.rs`: Solaris collector
- `src/delta.rs`: delta/rate derivation
- `src/metrics.rs`: OTEL metric instruments and recording
- `linux.yaml`: Linux config example
- `windows.yaml`: Windows config example
- `otel.yaml`: OpenTelemetry Collector example
- `docker.dev/`: QA Dockerfiles and compose services

## Quick Start

### 1. Pick a config file

Use one of the included examples:
- `linux.yaml`
- `windows.yaml`

### 2. Run Ojo

```bash
cargo run -- --config linux.yaml
```

Or on Windows:

```bash
cargo run -- --config windows.yaml
```

### 3. Dump one snapshot for debugging

```bash
cargo run -- --config linux.yaml --dump-snapshot
```

## Configuration

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

### collection

```yaml
collection:
  poll_interval_secs: 5
  include_process_metrics: true
```

### export

```yaml
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

## Metric Selection (New)

If `metrics` is omitted, Ojo exports all metrics.

You can select metric groups in three ways.

### 1) Single group

```yaml
metrics: cpu
```

### 2) Multiple groups

```yaml
metrics: [cpu, memory, disk]
```

### 3) Advanced section form

```yaml
metrics:
  groups: [cpu, memory]
  include: [system.linux.net.]
  exclude: [process.]
```

Rules:
- `groups` expands to metric-name prefixes
- `include`/`exclude` are prefix-based
- `exclude` wins over `include`
- If `metrics` is not defined, all metrics are exported

Supported groups:
- `cpu`
- `memory`
- `disk`
- `network`
- `process`
- `filesystem`
- `linux`
- `windows`
- `host`

If an unknown group is configured, Ojo fails fast with a config error.

## Environment Variables

Important env overrides:
- `PROC_OTEL_CONFIG`
- `PROC_POLL_INTERVAL_SECS`
- `PROC_INCLUDE_PROCESS_METRICS`
- `OTEL_EXPORTER_OTLP_ENDPOINT`
- `OTEL_EXPORTER_OTLP_PROTOCOL`
- `OTEL_EXPORTER_OTLP_HEADERS`
- `OTEL_EXPORTER_OTLP_COMPRESSION`
- `OTEL_EXPORTER_OTLP_TIMEOUT`

## OpenTelemetry Collector

A sample collector config is included in `otel.yaml`.

Typical flow:
1. Start collector
2. Point Ojo `export.otlp.endpoint` to collector OTLP endpoint
3. Run Ojo

## Docker QA

QA services are defined in `docker.dev/docker-compose.yml`.

Run a Linux QA service example:

```bash
docker compose -f docker.dev/docker-compose.yml run --rm qa-ubuntu-2204
```

Run Windows GNU cross-target check JSON output:

```bash
docker compose -f docker.dev/docker-compose.yml run --rm qa-windows-2022-gnu
```

This writes:
- `tests/qa/windows-2022-check.json`

## Build and Validate

```bash
cargo check
cargo check --target x86_64-pc-windows-gnu
cargo test
```

## Platform Notes

- Linux-only metrics are omitted on Windows by design
- Unsupported metrics are omitted rather than forced to zero
- Windows uses synthetic load under `windows.load.synthetic.*` (not Linux loadavg equivalent)
- Windows process handle count is mapped as compatibility for open-file-descriptor style views

## Troubleshooting

### No metrics exported

- Verify endpoint/protocol match backend (`grpc` vs `http/protobuf`)
- Check collector/backend availability
- Enable debug logs:

```bash
RUST_LOG=debug cargo run -- --config linux.yaml
```

### Process metrics missing

- Ensure `collection.include_process_metrics: true`
- Verify permissions
- Check `metrics` filters are not excluding `process.`

## Development Notes

- Keep collector behavior best-effort and explicit in support-state metadata
- Prefer adding schema/compat notes rather than silently changing semantics
- Validate with `cargo check` for host and cross-target when touching platform code
