# Redis Service

The Redis service polls `redis-cli` INFO counters and emits OTEL metrics for availability, usage, and efficiency.

## What it collects
- Up and availability
- Client and blocking counts
- Memory usage and uptime
- Command and connection counters/rates
- Keyspace hit ratio and key eviction/expiration counters

## Configuration
Primary config file: `services/redis/redis.yaml`

Redis connection settings:
- `redis.executable`
- `redis.host`, `redis.port`
- `redis.username`, `redis.password`

Also configurable:
- `collection.poll_interval_secs`
- `export.otlp.*`
- `metrics.include` / `metrics.exclude`
- `storage.archive_*`

## Storage and archive
Redis archives are parquet-based and controlled by `storage`:
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
  --archive-dir services/redis/data \
  --endpoint http://localhost:4320/otlp/v1/metrics \
  --protocol otlp
```

## Run
```bash
cargo run -p ojo-redis -- --config services/redis/redis.yaml
```

Shell log output (no endpoint required):
```bash
cargo run --bin archive-replay -- \
  --archive-dir <archive_dir> \
  --protocol shell-logs
```
