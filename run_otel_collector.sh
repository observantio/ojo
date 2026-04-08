#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_CONFIG_FILE="./otel.yaml"
CONFIG_FILE="$DEFAULT_CONFIG_FILE"
OTEL_VERSION_DEFAULT="0.149.0"
OTEL_VERSION="${OTEL_VERSION:-$OTEL_VERSION_DEFAULT}"

usage() {
    echo "Usage: $0 -t <token> [-v <otel_version>] [-c <config_file>]"
    echo "Example: $0 -t <token> -v 0.149.0 -c /path/to/otelcollector.yaml"
    exit 1
}

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "Missing required command: $1"
        exit 1
    }
}

detect_arch() {
    case "$(uname -m)" in
        x86_64|amd64) echo "amd64" ;;
        aarch64|arm64) echo "arm64" ;;
        *)
            echo "Unsupported architecture: $(uname -m)"
            exit 1
            ;;
    esac
}

install_otelcol_contrib() {
    require_cmd curl
    require_cmd sudo
    require_cmd dpkg

    local arch tmp_dir deb_file url
    arch="$(detect_arch)"
    tmp_dir="$(mktemp -d)"
    deb_file="$tmp_dir/otelcol-contrib_${OTEL_VERSION}_linux_${arch}.deb"
    url="https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v${OTEL_VERSION}/otelcol-contrib_${OTEL_VERSION}_linux_${arch}.deb"

    echo "otelcol-contrib not found. Installing v${OTEL_VERSION} for ${arch}..."
    curl -fL "$url" -o "$deb_file"
    sudo dpkg -i "$deb_file" || {
        sudo apt-get update
        sudo apt-get -f install -y
        sudo dpkg -i "$deb_file"
    }
    rm -rf "$tmp_dir"

    command -v otelcol-contrib >/dev/null 2>&1 || {
        echo "otelcol-contrib installation failed"
        exit 1
    }
}

while getopts ":t:v:c:" opt; do
    case "$opt" in
        t) MIMIR_OTLP_TOKEN="$OPTARG" ;;
        v) OTEL_VERSION="$OPTARG" ;;
        c) CONFIG_FILE="$OPTARG" ;;
        *) usage ;;
    esac
done

[ -n "${MIMIR_OTLP_TOKEN:-}" ] || usage
[ -n "${OTEL_VERSION:-}" ] || OTEL_VERSION="$OTEL_VERSION_DEFAULT"
[ -f "$CONFIG_FILE" ] || { echo "Missing config file: $CONFIG_FILE"; exit 1; }

if ! command -v otelcol-contrib >/dev/null 2>&1; then
    install_otelcol_contrib
fi

exec env MIMIR_OTLP_TOKEN="$MIMIR_OTLP_TOKEN" \
    OTEL_OTLP_TOKEN="$MIMIR_OTLP_TOKEN" \
    otelcol-contrib --config "$CONFIG_FILE"