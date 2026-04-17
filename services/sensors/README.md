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
Sensors archives are parquet-based and controlled by `storage`:
- `storage.archive_enabled`: enable/disable archive writer.
- `storage.archive_dir`: output directory.
- `storage.archive_max_file_bytes`: rotate when active file reaches this size.
- `storage.archive_retain_files`: number of rotated files to keep.
- `storage.archive_file_stem`: output file prefix.
- `storage.archive_format`: currently `parquet`.
- `storage.archive_mode`:
  - `trend` (default): compact lossy window summaries (`min/max/avg/count/first/last`).
  - `lossless`: full-fidelity row archives with columnar compression.
  - `forensic`: compatibility mode, same behavior class as lossless row archival.
- `storage.archive_window_secs`: summary window size for `trend`.
- `storage.archive_compression`: parquet compression codec (`zstd`).

Typical files:
- trend mode: `<archive_file_stem>-trend.parquet`
- lossless mode: `<archive_file_stem>-lossless.parquet`
- forensic mode: `<archive_file_stem>-forensic.parquet`

## Replay archives
Replay archives (all modes) to OTLP/Mimir:

```bash
cargo run --bin archive-replay -- \
  --archive-dir services/sensors/data \
  --endpoint http://localhost:4320/otlp/v1/metrics \
  --protocol otlp
```

## Run
```bash
cargo run -p ojo-sensors -- --config services/sensors/sensors.yaml
```

Shell log output (no endpoint required):
```bash
cargo run --bin archive-replay -- \
  --archive-dir <archive_dir> \
  --protocol shell-logs
```
