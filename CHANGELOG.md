# Changelog

All notable changes to this project will be documented in this file.

## [0.0.3] - 2026-04-15

### Added

**Test Coverage**
- Expanded coverage across core and sidecar services to above 90%, including deterministic one-shot execution paths for integration-style `main` tests.
- Broadened Linux collector helper tests: cgroup/support-state helpers, cache parsers, key formatting, and scope normalization.
- Added additional config/catalog edge-case tests.

**New Sidecar Services** *(cross-platform)*
| Service | Metric Namespace |
|---|---|
| `ojo-redis` | `system.redis.*` |
| `ojo-nginx` | `system.nginx.*` |
| `ojo-systemd` | `system.systemd.*` |
| `ojo-syslog` | `system.syslog.*` |
| `ojo-systrace` | `system.systrace.*` |

**Grafana Dashboards**
- `grafana/redis.json`, `grafana/nginx.json`, `grafana/systemd.json`, `grafana/syslog.json`, `grafana/systrace.json`

**`--dump-snapshot` Mode**
- Added JSON one-shot snapshot output to all sidecar services: `ojo-docker`, `ojo-gpu`, `ojo-mysql`, `ojo-nfs-client`, `ojo-nginx`, `ojo-postgres`, `ojo-redis`, `ojo-sensors`, `ojo-systemd`, `ojo-syslog`, `ojo-systrace`.

**OpenTelemetry**
- Added `run_otel_collector.sh` at project root and documented the simplified OTel Collector startup workflow in `DEPLOYMENT.md`.

---

### Changed

**CI & Coverage**
- `host-collectors` coverage gate now enforced at 100% line coverage via `cargo llvm-cov -p host-collectors --all-features --summary-only --fail-under-lines 100`.
- Updated service and core test paths to prevent flaky Ctrl-C handler re-registration failures across repeated test runs.

**Linux / Config**
- Updated Linux snapshot support-state assertions to match current key naming (`system.linux.cgroup.mode`).
- Config validation now accepts `PROC_POLL_INTERVAL_SECS` when YAML omits `collection.poll_interval_secs`.

**Systrace**
- Trace export now groups sampled lines by inferred component (e.g. `kernel.userstack`) instead of one child span per line — reduces Tempo service-graph fan-out while preserving trace context.
- Linux event discovery uses a single `events/` traversal to compute both counts and enabled-event inventory, reducing per-poll overhead in trace-heavy environments.
- Refined span topology and service-graph compatibility for clearer parent/child relationships.

**Observability & Metrics**
- Added Redis/NGINX-style connection lifecycle logging (`connected`, `failed`, `reconnected`, `still unavailable`, `disconnected`) to MySQL, Postgres, Syslog, and Systrace sidecars.
- Expanded NGINX exporter lifecycle reporting with explicit OTLP state transitions and health metrics: `system.nginx.exporter.available`, `system.nginx.exporter.reconnecting`, `system.nginx.exporter.errors.total`.
- Updated NGINX and Redis Grafana traffic-rate queries with PromQL fallback derivation from counter totals when direct rate gauges are sparse.

---

### Fixed

- Resolved Clippy `bool_assert_comparison` warning in GPU platform tests by switching to idiomatic boolean assertions.
- Improved Redis command-spawn failure messaging to clearly surface missing client executable cases (e.g. `redis-cli` not on `PATH`).

### Documentation
- Expanded deployment and README operator guidance with practical configuration/cardinality tuning details and a broader collected-metrics reference.
- Updated `README.md` sidecar coverage to include all current services (`ojo-docker`, `ojo-gpu`, `ojo-mysql`, `ojo-nfs-client`, `ojo-nginx`, `ojo-postgres`, `ojo-redis`, `ojo-sensors`, `ojo-syslog`, `ojo-systemd`, `ojo-systrace`), including service table, Grafana/dashboard references, repository layout, quick-start commands, and extension metric-prefix guidance; also fixed a broken quick-start code fence.

## [0.0.2] - 2026-03-26

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
  - `cargo check --workspace --all-targets`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `cargo test --workspace --all-targets --all-features`
  - cross-target check for `x86_64-pc-windows-gnu`
- Coverage enforcement in CI using `cargo-llvm-cov` with a line threshold gate on `host-collectors` only (workspace-wide % is too low for a 70% bar without broad integration tests):
  - `cargo llvm-cov -p host-collectors --all-features --all-targets --summary-only --fail-under-lines 70`
- Platform metric contract test suite in `tests/qa_platform_metric_contracts.rs` to validate fixture OS namespace boundaries.
- New process-label cardinality controls:
  - `collection.process_include_pid_label`
  - `collection.process_include_command_label`
  - `collection.process_include_state_label`
  - plus env overrides `PROC_PROCESS_INCLUDE_*`.


### Changed
- **Release workflow (`.github/workflows/ci.yml`):** now publishes non-legacy release assets for:
  - core `ojo` on Linux x86_64, Linux aarch64, and Windows x86_64
  - all six extension services on Linux (`-unix`) and Windows (`-win`) with names like:
    - `ojo-docker-unix-v0.0.2`
    - `ojo-docker-win-v0.0.2.exe`
    - and equivalent `ojo-gpu`, `ojo-sensors`, `ojo-mysql`, `ojo-postgres`, `ojo-nfs-client` assets
- **`host-collectors` tests:** Added coverage for `build_meter_provider` (gRPC with Tokio runtime, HTTP/protobuf, unknown protocol error, export interval), `hostname()`, and extra `default_protocol_for_endpoint` cases; `tokio` dev-dependency for gRPC exporter tests. Line coverage for the crate is now ~95% under `llvm-cov` (meets the 70% CI gate; earlier runs were ~42% with only three small tests).
- **Clippy (Rust 1.94):** Removed redundant `as u64` casts on `libc::statvfs` fields in `src/linux/slab_filesystem_collector.rs` (`clippy::unnecessary_cast`). NFS client `unix.rs`: `map_or(false, …)` → `is_some_and(…)` (`clippy::unnecessary_map_or`).
- **CI quality workflow:** `cargo check`, `cargo clippy`, and `cargo test` now pass `--workspace` so all extension crates are linted and tested like local `cargo clippy --workspace`. Windows GNU cross-check installs `gcc-mingw-w64-x86-64` (for `ring` / rustls) and runs `cargo check --workspace --all-targets --target x86_64-pc-windows-gnu`.
- CI coverage (`quality.yml`): `cargo llvm-cov` now uses `-p host-collectors` for `--fail-under-lines 70`. Workspace-wide coverage was ~4% (most crates are binaries with little test execution), so the previous command always failed CI; `rustfmt` output applied so `cargo fmt --check` passes.
- Removed duplicate process-count alias emission (`system.processes.count`) in favor of canonical `system.process.count`.
- Added explicit `system.os_type` to snapshot model and collectors to improve OS-aware metric gating.
- Restricted Linux-prefixed metric families to Linux/Android code paths so Solaris does not emit Linux namespaces.
- Refreshed documentation and sample configs (`README.md`, `linux.yaml`, `windows.yaml`, `tests/README.md`) to match current behavior and filtering semantics.
- Replaced tokenized sample header in `otel.yaml` with an environment placeholder (`${MIMIR_OTLP_TOKEN}`).

### Documentation
- **`README.md`** — major update for extensions and operators:
  - New **“Optional extension services (sidecars)”** section: table of all six services with Cargo package names, example YAML paths, metric prefixes, `host-collectors` and Grafana pointers, `cargo build -p` / `cargo run -p` examples, and release-asset naming for Linux (`-unix`) and Windows (`-win`).
  - Repository layout and **Metric selection** guidance extended for MySQL, Postgres, and NFS client prefixes.
  - Deployment patterns and release/download examples updated for `v0.0.2`.
- **`DEPLOYMENT.md`** — added dedicated deployment and release guide:
  - release artifact matrix for core + sidecars
  - quick download/run examples
  - OpenTelemetry Collector prerequisite and Docker run command with `MIMIR_OTLP_TOKEN`

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
