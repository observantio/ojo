# NFS Client Service

The NFS client service captures NFS RPC and mount telemetry from host client tooling.

## What it collects
- Source availability
- Mount count
- RPC call totals and rates
- Retransmission and auth-refresh counters

## Configuration
Primary config file: `services/nfs-client/nfs-client.yaml`

Main options:
- `nfs_client.executable` (optional command override)
- `collection.poll_interval_secs`
- `export.otlp.*`
- `metrics.include` / `metrics.exclude`
- `storage.archive_*`

## Run
```bash
cargo run -p ojo-nfs-client -- --config services/nfs-client/nfs-client.yaml
```
