# Docker Service

The Docker service collects container runtime health and capacity signals and exports them as OTEL metrics.

## What it collects
- Container availability and running state
- CPU, memory, network, and restart counters
- Optional low-cardinality container label dimensions

## Configuration
Primary config file: `services/docker/docker.yaml`

Key sections:
- `service`: service name and instance id
- `collection.poll_interval_secs`: polling cadence
- `docker.include_container_labels`: toggle per-container labels
- `docker.max_labeled_containers`: cap labeled container count
- `export.otlp`: OTLP endpoint/protocol/timeouts
- `metrics.include` and `metrics.exclude`: prefix filters
- `storage`: archive controls

Archive settings:
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
- `trend`: compact lossy summaries (`min/max/avg/count/first/last`) per window.
- `lossless`: full-fidelity row archival with efficient parquet + zstd compression.
- `forensic`: compatibility row mode.

Typical files:
- `<archive_file_stem>-trend.parquet`
- `<archive_file_stem>-lossless.parquet`

Replay archives (all modes):
```bash
cargo run --bin archive-replay -- \
  --archive-dir services/docker/data \
  --endpoint http://localhost:4320/otlp/v1/metrics \
  --protocol otlp
```

## Run
```bash
cargo run -p ojo-docker -- --config services/docker/docker.yaml
```

Use `--once` for one-shot collection.

Shell log output (no endpoint required):
```bash
cargo run --bin archive-replay -- \
  --archive-dir <archive_dir> \
  --protocol shell-logs
```
