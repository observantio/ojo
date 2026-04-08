# Sensors Service

The Sensors service collects host hardware sensor readings and exports OTEL metrics.

## What it collects
- Temperature signals
- Fan RPM readings
- Voltage readings
- Sensor inventory counts

## Configuration
Primary config file: `services/sensors/sensors.yaml`

Key options:
- `collection.poll_interval_secs`
- `sensors.include_sensor_labels`
- `sensors.max_labeled_sensors`
- `export.otlp.*`
- `metrics.include` / `metrics.exclude`
- `storage.archive_*`

## Storage and archive
Snapshot NDJSON archival is controlled by `storage` and uses file rotation by size and file-count retention.

## Run
```bash
cargo run -p ojo-sensors -- --config services/sensors/sensors.yaml
```
