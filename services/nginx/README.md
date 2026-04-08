# NGINX Service

The NGINX service scrapes status data and exports request and connection metrics.

## What it collects
- Service up and source availability
- Active/reading/writing/waiting connections
- Accepted/handled/request totals and rates

## Configuration
Primary config file: `services/nginx/nginx.yaml`

NGINX-specific keys:
- `nginx.executable`
- `nginx.status_url`

Common keys:
- `collection.poll_interval_secs`
- `export.otlp.*`
- `metrics.include` / `metrics.exclude`
- `storage.archive_*`

## Run
```bash
cargo run -p ojo-nginx -- --config services/nginx/nginx.yaml
```
