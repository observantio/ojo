#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
QA_DIR="$ROOT_DIR/tests/qa"
LOCAL_OUT="$QA_DIR/local.json"

mkdir -p "$QA_DIR"

cd "$ROOT_DIR"

echo "Collecting one interval of metrics (json) to $LOCAL_OUT"
cargo run --quiet -- --dump-snapshot >"$LOCAL_OUT"

echo "Collecting one interval of metrics (json) to $LOCAL_OUT"
cargo run --quiet -- --dump-snapshot >"$LOCAL_OUT"

echo "Wrote $LOCAL_OUT"
