# Syslog Service

Syslog captures operating-system, process, and application log streams and exports them through OTEL-compatible logs plus metrics.

## Highlights
- Linux journald and dmesg collection, Windows ETW parity paths
- Optional file-watch ingestion from YAML-configured paths
- Bounded in-memory buffer with backpressure-safe drop policy
- Retry and reconnect logic for exporter outages
- Rotating NDJSON snapshot archive

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
- `export.otlp.*` and `export.logs.*`
- `metrics.include` / `metrics.exclude`

## Run
```bash
cargo run -p ojo-syslog -- --config services/syslog/syslog.yaml
```
