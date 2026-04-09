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

## Quick Connectivity Check
Run once with info logs to verify collection and OTLP export:

```bash
RUST_LOG=info cargo run -p ojo-systemd -- --once --config services/systemd/systemd.yaml
```

Look for:
- `first systemd snapshot collected` (local collection is working)
- `initial OTLP flush completed` (export flush succeeded)
- `Systemd exporter connected successfully` (exporter reachable)
- `Systemd exporter disconnected, reconnecting` or `Systemd exporter disconnected; still unavailable` (exporter failure/retry state)
- `Systemd exporter reconnected successfully` (recovery after failure)
