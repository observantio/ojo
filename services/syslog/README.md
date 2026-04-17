# Syslog Service

Syslog captures operating-system, process, and application log streams and exports them through OTEL-compatible logs plus metrics.

## Highlights
- Linux journald and dmesg collection, Windows ETW parity paths
- Optional file-watch ingestion from YAML-configured paths
- Bounded in-memory buffer with backpressure-safe drop policy
- Retry and reconnect logic for exporter outages
- Rotating parquet archive modes (trend + lossless)

## Configuration
Primary config file: `services/syslog/syslog.yaml`

Key sections:
- `collection.poll_interval_secs`
- `collection.max_lines_per_source`
- `collection.max_message_bytes`
- `watch.files[]`
- `pipeline.buffer_capacity_records`
- `pipeline.export_batch_size`
- `pipeline.retry_backoff_secs`
- `storage.archive_enabled`
- `storage.archive_dir`
- `storage.archive_max_file_bytes`
- `storage.archive_retain_files`
- `storage.archive_format`
- `storage.archive_mode`
- `storage.archive_window_secs`
- `storage.archive_compression`
- `export.otlp.*` and `export.logs.*`
- `metrics.include` / `metrics.exclude`

## Archive modes
- `trend` (default): compact lossy summaries for long-term trend analysis.
- `lossless`: preserves full log payload fidelity while using parquet + zstd compression.
- `forensic`: compatibility mode for row-level archival behavior.

`archive_window_secs` applies to `trend` mode. Rotation and retention are controlled by `archive_max_file_bytes` and `archive_retain_files`.

## Replay to Mimir
Use the replay utility to push archived parquet files (trend/lossless/forensic) into OTLP or remote-write endpoints:

```bash
cargo run --bin archive-replay -- \
  --archive-dir services/syslog/data \
  --endpoint http://localhost:4320/otlp/v1/metrics \
  --protocol otlp
```

Remote write example:

```bash
cargo run --bin archive-replay -- \
  --archive-dir services/syslog/data \
  --endpoint http://localhost:4320/mimir/api/v1/push \
  --protocol remote-write
```

## Run
```bash
cargo run -p ojo-syslog -- --config services/syslog/syslog.yaml
```

Shell log output (no endpoint required):
```bash
cargo run --bin archive-replay -- \
  --archive-dir <archive_dir> \
  --protocol shell-logs
```
