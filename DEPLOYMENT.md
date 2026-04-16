# Deployment and Releases

## Build and Release

Releases are published via `.github/workflows/ci.yml` on `v*` tag push or manual dispatch.

---

# OpenTelemetry Collector (Required)

Observantio requires a **host-level OpenTelemetry Collector** to properly collect system metrics (CPU, memory, disk, etc).

⚠️ Running the collector in Docker will **not capture host metrics correctly** unless heavily modified.
Use the provided script instead.

---

# Running the OpenTelemetry Collector for Observantio

## What it does

`run_otel_collector.sh`:

* Installs `otelcol-contrib` if not present
* Runs it directly on the host
* Loads configuration from `root`
* Ensures proper access to host-level telemetry

---

## Configuration

Default expected config location:

```
otel.yaml (assuming at root)
```

You can override this via the `-c` flag.

---

## Get the OTLP Token

From Watchdog:

* Copy the **OTLP token** (not tenant key)
* Token is shown once → regenerate if lost

---

## Run the Collector

From the repository root:

```bash
sudo bash run_otel_collector.sh -t <OTLP_TOKEN> -c <CONFIG_PATH>
```

The same token is used for both the Mimir metrics exporter and Tempo traces exporter.

### Example

```bash
sudo bash run_otel_collector.sh \
  -t bo_xxxxxxxxxxxxxxxxx \
  -c otel.yaml
```

---

## How it works internally

The script ensures `otelcol-contrib` is available, then runs:

```bash
otelcol-contrib --config "$CONFIG_FILE"
```

Running on the host allows:

* Full access to `/proc`, `/sys`
* Accurate CPU, memory, disk metrics
* Proper integration with `ojo` collectors


## Configure Collector → Watchdog (Mimir Endpoint)

Ensure your OpenTelemetry Collector is pointing to the correct Watchdog (Mimir) endpoint.

### Example

```yaml
endpoint: "http://localhost:4320/mimir/api/v1/push"
```

---

## Choosing the Correct Endpoint

Use the appropriate host depending on your setup:

### Local (Linux host)

```yaml
endpoint: "http://localhost:4320/mimir/api/v1/push"
```

### Docker (Windows / Mac)

```yaml
endpoint: "http://host.docker.internal:4320/mimir/api/v1/push"
```

### Remote Server

```yaml
endpoint: "http://<SERVER_IP_OR_DNS>:4320/mimir/api/v1/push"
```

* Ensure port `4320` is reachable (firewall / security groups)

### Kubernetes

```yaml
endpoint: "http://<mimir-service-name>:4320/mimir/api/v1/push"
```

* Use the internal service DNS
* Example: `mimir.monitoring.svc.cluster.local`

---

## Key Notes

* The endpoint **must be reachable from the collector runtime**
* `host.docker.internal` works on:

  * Docker Desktop (Windows/Mac)
  * ❌ Not reliable on native Linux Docker
* If running collector on host → use `localhost`
* If running collector in container → ensure correct networking

---

## Quick Debug

```bash
curl http://localhost:4320/ready
```

* Should return `ready` (or HTTP 200)



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
| `ojo-systrace-unix-{version}` | `x86_64-unknown-linux-gnu` |
| `ojo-systrace-win-{version}.exe` | `x86_64-pc-windows-gnu` |
| `ojo-syslog-unix-{version}` | `x86_64-unknown-linux-gnu` |
| `ojo-syslog-win-{version}.exe` | `x86_64-pc-windows-gnu` |

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
Ensure you point the export to your otel collector, if it is not running or connection is not made, ojo will say it is not connected and keep a fixed `QUEUE` buffer until the collector is connected.

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
