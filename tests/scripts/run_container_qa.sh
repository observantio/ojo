#!/usr/bin/env bash
set -euo pipefail

DISTRO="${1:-unknown}"
ROOT_DIR="${2:-/workspace}"
OUT_DIR="$ROOT_DIR/tests/qa"
OUT_FILE="$OUT_DIR/${DISTRO}.json"
TMP_FILE="$OUT_FILE.tmp"
TARGET_DIR="$ROOT_DIR/target-qa/${DISTRO}"
BUILD_JOBS="${CARGO_BUILD_JOBS:-1}"
QA_TIMEOUT_SECS="${QA_TIMEOUT_SECS:-1800}"

mkdir -p "$OUT_DIR"
cd "$ROOT_DIR"

mkdir -p "$TARGET_DIR"
export CARGO_TARGET_DIR="$TARGET_DIR"
export CARGO_BUILD_JOBS="$BUILD_JOBS"
trap 'rm -f "$TMP_FILE"' EXIT

if [ "$(uname -m)" = "aarch64" ]; then
  export CFLAGS="${CFLAGS:-} -D__ARM_ARCH=8"
fi

echo "[$DISTRO] starting snapshot build/run (jobs=$CARGO_BUILD_JOBS timeout=${QA_TIMEOUT_SECS}s target=$CARGO_TARGET_DIR)"
if command -v timeout >/dev/null 2>&1; then
  timeout "$QA_TIMEOUT_SECS" cargo run -j "$CARGO_BUILD_JOBS" -- --dump-snapshot >"$TMP_FILE"
else
  cargo run -j "$CARGO_BUILD_JOBS" -- --dump-snapshot >"$TMP_FILE"
fi
mv "$TMP_FILE" "$OUT_FILE"

echo "Wrote $OUT_FILE"
