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

## Storage and archive
GPU archives are parquet-based:
- `storage.archive_enabled`
- `storage.archive_dir`
- `storage.archive_max_file_bytes`
- `storage.archive_retain_files`
- `storage.archive_file_stem`
- `storage.archive_format` (`parquet`)
- `storage.archive_mode` (`trend`, `lossless`, `forensic`)
- `storage.archive_window_secs` (used by `trend`)
- `storage.archive_compression` (`zstd`)

Archive modes:
- `trend`: compact lossy summaries for long-term trend analytics.
- `lossless`: full-fidelity row archival with parquet + zstd compression.
- `forensic`: compatibility row mode.

Typical files:
- `<archive_file_stem>-trend.parquet`
- `<archive_file_stem>-lossless.parquet`

Replay archives (all modes):
```bash
cargo run --bin archive-replay -- \
  --archive-dir services/gpu/data \
  --endpoint http://localhost:4320/otlp/v1/metrics \
  --protocol otlp
```

## Run
```bash
cargo run -p ojo-gpu -- --config services/gpu/gpu.yaml
```

Shell log output (no endpoint required):
```bash
cargo run --bin archive-replay -- \
  --archive-dir <archive_dir> \
  --protocol shell-logs
```
