# MySQL Service

The MySQL service polls server status counters and exports OTEL metrics for health and throughput.

## What it collects
- Reachability and up state
- Connections and active threads
- Query and slow-query counters
- Ingress/egress byte counters and rates

## Configuration
Primary config file: `services/mysql/mysql.yaml`

Connection settings are under `mysql`:
- `executable`
- `host`, `port`
- `user`, `password`
- `database`

Other key sections:
- `collection.poll_interval_secs`
- `export.otlp.*`
- `metrics.include` / `metrics.exclude`
- `storage.archive_*`

## Storage and archive
MySQL archives are parquet-based and controlled by `storage`:
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
- `trend`: compact lossy summaries for trend analytics.
- `lossless`: full-fidelity row archival with parquet + zstd compression.
- `forensic`: compatibility row mode.

Replay archives (all modes):
```bash
cargo run --bin archive-replay -- \
  --archive-dir services/mysql/data \
  --endpoint http://localhost:4320/otlp/v1/metrics \
  --protocol otlp
```

## Run
```bash
cargo run -p ojo-mysql -- --config services/mysql/mysql.yaml
```

Use `--dump-snapshot` to print one snapshot as JSON.

Shell log output (no endpoint required):
```bash
cargo run --bin archive-replay -- \
  --archive-dir <archive_dir> \
  --protocol shell-logs
```
