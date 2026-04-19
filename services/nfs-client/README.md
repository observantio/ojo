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

## Storage and archive
NFS client archives are parquet-based and controlled by `storage`:
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
  --archive-dir services/nfs-client/data \
  --endpoint http://localhost:4320/otlp/v1/metrics \
  --protocol otlp
```

## Run
```bash
cargo run -p ojo-nfs-client -- --config services/nfs-client/nfs-client.yaml
```

Shell log output (no endpoint required):
```bash
cargo run --bin archive-replay -- \
  --archive-dir <archive_dir> \
  --protocol shell-logs
```
