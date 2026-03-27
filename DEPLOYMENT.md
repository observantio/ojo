# Deployment and Releases

## Build and Release

Releases are published via `.github/workflows/ci.yml` on `v*` tag push or manual dispatch.

## OpenTelemetry Collector Requirement

Before running `ojo` or sidecars, ensure an OpenTelemetry Collector is running and reachable from your agent config (`export.otlp.endpoint`).

If you want to run one quickly with Docker:

```bash
export MIMIR_OTLP_TOKEN="<token from watchdog>"

docker run --rm -it \
  -p 4355:4355 \
  -p 4356:4356 \
  -v $(pwd)/otel.yaml:/etc/otelcol-contrib/config.yaml \
  -e MIMIR_OTLP_TOKEN=$MIMIR_OTLP_TOKEN \
  otel/opentelemetry-collector-contrib:latest \
  --config /etc/otelcol-contrib/config.yaml
```

If you have `otelcol-contrib` installed locally, you can run it directly:

```bash
export MIMIR_OTLP_TOKEN="<token from watchdog>"
sudo otelcol-contrib --config otel.yaml
```

If you prefer Docker and your Docker daemon requires elevated privileges, use:

```bash
export MIMIR_OTLP_TOKEN="<token from watchdog>"

sudo docker run --rm -it \
  -p 4355:4355 \
  -p 4356:4356 \
  -v $(pwd)/otel.yaml:/etc/otelcol-contrib/config.yaml \
  -e MIMIR_OTLP_TOKEN=$MIMIR_OTLP_TOKEN \
  otel/opentelemetry-collector-contrib:latest \
  --config /etc/otelcol-contrib/config.yaml
```

### Artifacts built on every release

| Artifact | Target |
|---|---|
| `ojo-{version}-linux-x86_64` | `x86_64-unknown-linux-gnu` |
| `ojo-{version}-linux-aarch64` | `aarch64-unknown-linux-gnu` |
| `ojo-{version}-windows-x86_64.exe` | `x86_64-pc-windows-gnu` |
| `ojo-docker-unix-{version}` | `x86_64-unknown-linux-gnu` |
| `ojo-docker-win-{version}.exe` | `x86_64-pc-windows-gnu` |
| `ojo-gpu-unix-{version}` | `x86_64-unknown-linux-gnu` |
| `ojo-gpu-win-{version}.exe` | `x86_64-pc-windows-gnu` |
| `ojo-sensors-unix-{version}` | `x86_64-unknown-linux-gnu` |
| `ojo-sensors-win-{version}.exe` | `x86_64-pc-windows-gnu` |
| `ojo-mysql-unix-{version}` | `x86_64-unknown-linux-gnu` |
| `ojo-mysql-win-{version}.exe` | `x86_64-pc-windows-gnu` |
| `ojo-postgres-unix-{version}` | `x86_64-unknown-linux-gnu` |
| `ojo-postgres-win-{version}.exe` | `x86_64-pc-windows-gnu` |
| `ojo-nfs-client-unix-{version}` | `x86_64-unknown-linux-gnu` |
| `ojo-nfs-client-win-{version}.exe` | `x86_64-pc-windows-gnu` |

### Download and run - Linux

```bash
curl -L https://github.com/observantio/ojo/releases/download/v0.0.2/ojo-v0.0.2-linux-aarch64 -o ojo
chmod +x ojo
./ojo --config linux.yaml
```

### Download and run - Windows (PowerShell)

```powershell
Invoke-WebRequest -Uri https://github.com/observantio/ojo/releases/download/v0.0.2/ojo-v0.0.2-windows-x86_64.exe -OutFile ojo.exe
.\ojo.exe --config windows.yaml
```

### Download and run a sidecar - Linux (Docker service)

```bash
curl -L https://github.com/observantio/ojo/releases/download/v0.0.2/ojo-docker-unix-v0.0.2 -o ojo-docker
chmod +x ojo-docker
./ojo-docker --config services/docker/docker.yaml
```

## Configuration Guide: linux.yaml and docker.yaml

This section focuses on practical tuning for label/cardinality control.

### Configure `linux.yaml` (core `ojo` agent)

Example baseline:

```yaml
service:
  name: linux
  instance_id: linux-0001

collection:
  poll_interval_secs: 5
  include_process_metrics: true
  process_include_pid_label: true
  process_include_command_label: true
  process_include_state_label: true

export:
  otlp:
    endpoint: "http://127.0.0.1:4355/v1/metrics"
    protocol: http/protobuf
    compression: gzip
    timeout_secs: 10
  batch:
    interval_secs: 5
    timeout_secs: 10

metrics:
  include: [system., process.]
  exclude: []
```

High-cardinality controls:

| Key | Effect | Cardinality impact |
|---|---|---|
| `collection.include_process_metrics` | Enables/disables all `process.*` metrics | Biggest cardinality lever |
| `collection.process_include_pid_label` | Adds/removes `process.pid` attribute | Very high impact |
| `collection.process_include_command_label` | Adds/removes `process.command` attribute | High impact |
| `collection.process_include_state_label` | Adds/removes `process.state` attribute | Moderate impact |
| `metrics.exclude` | Prefix-based metric removal | Hard cap for selected namespaces |

Low-cardinality profile (keep process metrics, remove process labels):

```yaml
collection:
  include_process_metrics: true
  process_include_pid_label: false
  process_include_command_label: false
  process_include_state_label: false
```

No per-process cardinality profile (remove process metrics):

```yaml
collection:
  include_process_metrics: false

metrics:
  include: [system.]
  exclude: [process.]
```

Environment variable overrides for label control:

```bash
export PROC_INCLUDE_PROCESS_METRICS=false
export PROC_PROCESS_INCLUDE_PID_LABEL=false
export PROC_PROCESS_INCLUDE_COMMAND_LABEL=false
export PROC_PROCESS_INCLUDE_STATE_LABEL=false
```

### Configure `services/docker/docker.yaml` (`ojo-docker` sidecar)

Example baseline:

```yaml
service:
  name: ojo-docker
  instance_id: docker-0001

collection:
  poll_interval_secs: 10

docker:
  include_container_labels: true
  max_labeled_containers: 25

export:
  otlp:
    endpoint: "http://127.0.0.1:4355/v1/metrics"
    protocol: http/protobuf
    compression: gzip
    timeout_secs: 10
  batch:
    interval_secs: 5
    timeout_secs: 10

metrics:
  include: [system.docker.]
  exclude: []
```

Docker cardinality controls:

| Key | Effect | Cardinality impact |
|---|---|---|
| `docker.include_container_labels` | Emits/removes per-container attributes (`container.id`, `container.name`, `container.image`, `container.state`) | Primary label control |
| `docker.max_labeled_containers` | Caps number of containers that get labeled series | Bounded cardinality when labels are enabled |
| `metrics.exclude` | Prefix-based metric removal (for example `system.docker.container.`) | Hard cap for container-level series |

Important behavior:
- If `include_container_labels: false`, labeled per-container series are not emitted.
- If `include_container_labels: true` and `max_labeled_containers: 0`, no labeled container series are emitted.
- Aggregate Docker metrics still emit without container labels unless filtered out with `metrics.exclude`.

Low-cardinality Docker profile (no container labels, keep aggregate container metrics):

```yaml
docker:
  include_container_labels: false
  max_labeled_containers: 0
```

Minimal Docker profile (counts and source state only):

```yaml
docker:
  include_container_labels: false

metrics:
  include:
    - system.docker.containers.
    - system.docker.source.available
  exclude:
    - system.docker.container.
```

Environment variable override for Docker config path:

```bash
export OJO_DOCKER_CONFIG=services/docker/docker.yaml
```
