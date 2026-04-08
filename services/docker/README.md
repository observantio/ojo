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

## Run
```bash
cargo run -p ojo-docker -- --config services/docker/docker.yaml
```

Use `--once` for one-shot collection.
