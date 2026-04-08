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

## Run
```bash
cargo run -p ojo-mysql -- --config services/mysql/mysql.yaml
```

Use `--dump-snapshot` to print one snapshot as JSON.
