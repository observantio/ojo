# Postgres Service

The Postgres service reads database status and transaction counters and exports OTEL metrics.

## What it collects
- Reachability and up state
- Connection count
- Commit and rollback totals and rates
- Deadlocks and block hit/read counters

## Configuration
Primary config file: `services/postgres/postgres.yaml`

Postgres connection settings:
- `postgres.executable`
- `postgres.uri` (optional DSN)

Also configurable:
- `collection.poll_interval_secs`
- `export.otlp.*`
- `metrics.include` / `metrics.exclude`
- `storage.archive_*`

## Storage and archive
Postgres archives are parquet-based and controlled by `storage`:
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
  --archive-dir services/postgres/data \
  --endpoint http://localhost:4320/otlp/v1/metrics \
  --protocol otlp
```

## Run
```bash
cargo run -p ojo-postgres -- --config services/postgres/postgres.yaml
```

Shell log output (no endpoint required):
```bash
cargo run --bin archive-replay -- \
  --archive-dir <archive_dir> \
  --protocol shell-logs
```
