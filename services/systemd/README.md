# Systemd Service

The Systemd service tracks unit and job state health and exports OTEL metrics.

## What it collects
- Source availability and up signal
- Unit totals by state (active, inactive, failed, etc.)
- Job queue/running counts
- Failed-unit and active-unit ratios

## Configuration
Primary config file: `services/systemd/systemd.yaml`

Key sections:
- `collection.poll_interval_secs`
- `export.otlp.*`
- `metrics.include` / `metrics.exclude`
- `storage.archive_*`

## Run
```bash
cargo run -p ojo-systemd -- --config services/systemd/systemd.yaml
```

Use `--dump-snapshot` for a one-time JSON snapshot.
