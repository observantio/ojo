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

## Run
```bash
cargo run -p ojo-postgres -- --config services/postgres/postgres.yaml
```
