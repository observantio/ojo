# GPU Service

The GPU service collects accelerator utilization, memory, thermal, and power telemetry and publishes OTEL metrics.

## What it collects
- Device up/availability
- Utilization and memory usage
- Temperature and power draw
- Device inventory counts

## Configuration
Primary config file: `services/gpu/gpu.yaml`

Important keys:
- `collection.poll_interval_secs`
- `gpu.include_device_labels`
- `gpu.max_labeled_devices`
- `export.otlp.*`
- `metrics.include` / `metrics.exclude`
- `storage.archive_*`

## Archive output
When enabled, each polling snapshot is persisted as NDJSON with rotation and retention.

## Run
```bash
cargo run -p ojo-gpu -- --config services/gpu/gpu.yaml
```
