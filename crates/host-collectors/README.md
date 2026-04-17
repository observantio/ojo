# Host Collectors

Shared runtime library used by `ojo` and all sidecar services.

## What it provides
- OTLP meter/tracer provider setup helpers.
- Prefix-based metric filtering helpers.
- Unified archive writer implementation used across services.

## Archive configuration contract
The shared archive config is modeled by `ArchiveStorageConfig` and consumed by all services:

- `archive_enabled`
- `archive_dir`
- `archive_max_file_bytes`
- `archive_retain_files`
- `archive_file_stem`
- `archive_format` (currently `parquet`)
- `archive_mode` (`trend`, `lossless`, `forensic`)
- `archive_window_secs` (trend mode)
- `archive_compression` (`zstd`)

## Archive modes
- `trend`: compact lossy summaries by time window (`min/max/avg/count/first/last`).
- `lossless`: full-fidelity row archival with parquet + zstd compression.
- `forensic`: compatibility row mode with row-level payload retention.

## Typical outputs
- trend: `<archive_file_stem>-trend.parquet`
- lossless: `<archive_file_stem>-lossless.parquet`
- forensic: `<archive_file_stem>-forensic.parquet`

## Replay
Use the workspace replay tool:

```bash
cargo run --bin archive-replay -- \
  --archive-dir <archive_dir> \
  --endpoint http://localhost:4320/otlp/v1/metrics \
  --protocol otlp
```

Remote-write example:

```bash
cargo run --bin archive-replay -- \
  --archive-dir <archive_dir> \
  --endpoint http://localhost:4320/mimir/api/v1/push \
  --protocol remote-write
```

Shell log output (no endpoint required):
```bash
cargo run --bin archive-replay -- \
  --archive-dir <archive_dir> \
  --protocol shell-logs
```
