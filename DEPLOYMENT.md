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

If your Docker daemon requires elevated privileges, use:

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
