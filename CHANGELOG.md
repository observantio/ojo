# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added
- **Extension services (six OTLP sidecars)** — workspace members under `services/` that ship metrics independently of the main agent:
  - `ojo-docker` — Docker container stats (`system.docker.*`)
  - `ojo-gpu` — NVIDIA GPU stats (`system.gpu.*`)
  - `ojo-sensors` — hardware sensors (`system.sensor.*`)
  - `ojo-mysql` — MySQL server stats (`system.mysql.*`)
  - `ojo-postgres` — PostgreSQL stats (`system.postgres.*`)
  - `ojo-nfs-client` — NFS client RPC stats (`system.nfs_client.*`)
- **`crates/host-collectors`** — shared OTLP meter provider setup, metric prefix filtering, and hostname helpers for the main agent and all sidecars.
- **Grafana dashboards** in `grafana/` for each extension (`docker.json`, `gpu.json`, `sensors.json`, `mysql.json`, `postgres.json`, `nfs-client.json`).
- **Extension metric contracts** — `tests/qa_extension_metric_contracts.rs` (and related smoke tests) for extension namespaces.
- Quality gates workflow (`.github/workflows/quality.yml`) for pull requests and `main` pushes:
  - `cargo fmt --all -- --check`
  - `cargo check --all-targets`
  - `cargo clippy --all-targets -- -D warnings`
  - `cargo test --all-targets --all-features`
  - cross-target check for `x86_64-pc-windows-gnu`
- Coverage enforcement in CI using `cargo-llvm-cov` with a line threshold gate:
  - `cargo llvm-cov --workspace --all-features --all-targets --summary-only --fail-under-lines 70`
- Platform metric contract test suite in `tests/qa_platform_metric_contracts.rs` to validate fixture OS namespace boundaries.
- New process-label cardinality controls:
  - `collection.process_include_pid_label`
  - `collection.process_include_command_label`
  - `collection.process_include_state_label`
  - plus env overrides `PROC_PROCESS_INCLUDE_*`.


### Changed
- Removed duplicate process-count alias emission (`system.processes.count`) in favor of canonical `system.process.count`.
- Added explicit `system.os_type` to snapshot model and collectors to improve OS-aware metric gating.
- Restricted Linux-prefixed metric families to Linux/Android code paths so Solaris does not emit Linux namespaces.
- Refreshed documentation and sample configs (`README.md`, `linux.yaml`, `windows.yaml`, `tests/README.md`) to match current behavior and filtering semantics.
- Replaced tokenized sample header in `otel.yaml` with an environment placeholder (`${MIMIR_OTLP_TOKEN}`).

### Documentation
- **`README.md`** — major update for extensions and operators:
  - New **“Optional extension services (sidecars)”** section: table of all six services with Cargo package names, example YAML paths, metric prefixes, `host-collectors` and Grafana pointers, `cargo build -p` / `cargo run -p` examples, and clarification that release archives ship the main `ojo` binary (extensions built from source or separate release assets).
  - Repository layout and **Metric selection** guidance extended for MySQL, Postgres, and NFS client prefixes.
  - Deployment patterns already mention extension sidecars alongside the core agent.

## [0.0.1] - 2026-03-21

### Added
- GitHub Actions release workflow (`.github/workflows/ci.yml`):
  - Unified matrix build job covering Linux x86_64, Linux aarch64, Linux x86_64 (Legacy), Windows x86_64, Windows i686 (Legacy)
  - Optional legacy builds via `build_legacy_linux` and `build_legacy_windows` dispatch inputs
  - Shared `resolve-version` job output to eliminate repeated version resolution
  - Release job guarded with `if: ${{ !failure() && !cancelled() }}` to handle skipped legacy jobs correctly
- `docker.dev` multi-stage Dockerfiles for all distro images to reduce final image size and remove build tool residue
- Release assets:
  - `ojo-v0.0.1-linux-x86_64`
  - `ojo-v0.0.1-linux-aarch64`
  - `ojo-v0.0.1-linux-x86_64-legacy`
  - `ojo-v0.0.1-windows-x86_64.exe`
  - `ojo-v0.0.1-windows-i686-legacy.exe`

### Documentation
- `README.md` updated with build and release instructions and release asset run examples
- Project behavior documented: Ojo is a lightweight OpenTelemetry host metrics agent that collects system and process metrics (CPU, memory, disk, network, filesystem, process), computes deltas/rates, and exports to OTLP endpoints with configurable polling, filtering, and batching