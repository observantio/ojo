# Systrace Service

Systrace provides deep host tracing coverage across kernel, process, and runtime signals with OTEL-safe metric cardinality.

## Highlights
- Linux tracefs and Windows ETW parity signals
- Coverage-oriented counters for syscall, scheduler, and stream continuity
- Optional validation dataset and runtime probe profile configuration
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
- `instrumentation.*`
- `validation.dataset_dir`
- `export.otlp.*`
- `metrics.include` / `metrics.exclude`
- `traces.*`

## Run
```bash
cargo run -p ojo-systrace -- --config services/systrace/systrace.yaml
```
