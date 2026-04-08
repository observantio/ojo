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

## Run
```bash
cargo run -p ojo-redis -- --config services/redis/redis.yaml
```
