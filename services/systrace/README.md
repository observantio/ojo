# Systrace Service

Systrace provides deep host tracing coverage across kernel, process, and runtime signals with OTEL-safe metric cardinality.

## Highlights
- Linux tracefs and Windows ETW parity signals
- Coverage-oriented counters for syscall, scheduler, and stream continuity
- Rotating snapshot archive support

## Configuration
Primary config file: `services/systrace/systrace.yaml`

Important sections:
- `collection.poll_interval_secs`
- `collection.trace_stream_max_lines`
- `storage.archive_enabled`
- `storage.archive_dir`
- `storage.archive_max_file_bytes`
- `storage.archive_retain_files`
- `storage.archive_format`
- `storage.archive_mode`
- `storage.archive_window_secs`
- `storage.archive_compression`
- `instrumentation.*`
- `export.otlp.*`
- `metrics.include` / `metrics.exclude`
- `traces.*`

## Storage and archive
Systrace archives are parquet-based and controlled by `storage`:
- `archive_mode: trend` writes compact lossy summaries.
- `archive_mode: lossless` writes full-fidelity row archives.
- `archive_mode: forensic` is compatibility row mode.

Replay archives (all modes):
```bash
cargo run --bin archive-replay -- \
  --archive-dir services/systrace/data \
  --endpoint http://localhost:4320/otlp/v1/metrics \
  --protocol otlp
```

## Run
```bash
cargo run -p ojo-systrace -- --config services/systrace/systrace.yaml
```

Shell log output (no endpoint required):
```bash
cargo run --bin archive-replay -- \
  --archive-dir <archive_dir> \
  --protocol shell-logs
```
