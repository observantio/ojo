#!/usr/bin/env bash
set -euo pipefail

DISTRO="${1:-unknown}"
ROOT_DIR="${2:-/workspace}"
OUT_DIR="$ROOT_DIR/tests/qa"
OUT_FILE="$OUT_DIR/${DISTRO}.json"

mkdir -p "$OUT_DIR"
cd "$ROOT_DIR"

rm -rf target

if [ "$(uname -m)" = "aarch64" ]; then
  export CFLAGS="${CFLAGS:-} -D__ARM_ARCH=8"
fi

cargo run --quiet -- --dump-snapshot >"$OUT_FILE" 2>/dev/null

echo "Wrote $OUT_FILE"
