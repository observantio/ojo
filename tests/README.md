# QA Harness

This folder contains reproducible QA scripts and output artifacts.

- `tests/scripts/qa_sweep.sh`: local snapshot capture (`--dump-snapshot` to `tests/qa/local.json`)
- `tests/scripts/run_container_qa.sh`: container runner that writes distro QA output files
- `tests/qa/*.qa.txt`: generated QA outputs
- `tests/qa_json_schema.rs`: validates QA JSON schema, namespaces, and semantic tags

## QA JSON Namespace Argument

`tests/qa_json_schema.rs` accepts expected metric namespaces via environment variable:

- `QA_EXPECTED_METRIC_NAMESPACES`: comma-separated list, default is `process,system`

Examples:

```bash
cargo test --test qa_json_schema
QA_EXPECTED_METRIC_NAMESPACES=process,system cargo test --test qa_json_schema
```
