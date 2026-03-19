#!/usr/bin/env bash
set -euo pipefail

RSS_CSV=/tmp/ojo-rss.csv

RSS_BIN=../../target/release/ojo
ASAN_BIN=../../target/asan/aarch64-unknown-linux-gnu/release/ojo
CONFIG=../../linux.yaml

RSS_DURATION=600
ASAN_DURATION=600

cleanup() {
  if [[ -n "${PID:-}" ]] && kill -0 "$PID" 2>/dev/null; then
    kill -INT "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
  fi
}
trap cleanup INT TERM EXIT

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing command: $1" >&2
    exit 1
  }
}

[[ -x "$RSS_BIN" ]] || { echo "missing RSS binary: $RSS_BIN" >&2; exit 1; }
[[ -x "$ASAN_BIN" ]] || { echo "missing ASan binary: $ASAN_BIN" >&2; exit 1; }
[[ -f "$CONFIG" ]] || { echo "missing config: $CONFIG" >&2; exit 1; }

require_cmd awk
require_cmd date
require_cmd timeout
require_cmd grep

echo "timestamp,rss_kb" > "$RSS_CSV"

echo "Starting normal RSS run..."
"$RSS_BIN" --config "$CONFIG" &
PID=$!
START=$(date +%s)

while kill -0 "$PID" 2>/dev/null; do
  NOW=$(date +%s)
  ELAPSED=$((NOW - START))
  RSS_KB="$(awk '/^VmRSS:/ { print $2; found=1; exit } END { if (!found) print 0 }' "/proc/$PID/status" 2>/dev/null || echo 0)"
  printf '%s,%s\n' "$NOW" "${RSS_KB:-0}" >> "$RSS_CSV"

  if (( ELAPSED >= RSS_DURATION )); then
    kill -INT "$PID" 2>/dev/null || true
    break
  fi

  sleep 1
done

wait "$PID" 2>/dev/null || true
unset PID

echo "Starting ASan/LSan leak check..."

export ASAN_OPTIONS='detect_leaks=1:abort_on_error=0:halt_on_error=0:detect_stack_use_after_return=1'
export LSAN_OPTIONS='verbosity=1:report_objects=1:print_suppressions=0'
export RUST_BACKTRACE=1

set +e
ASAN_OUTPUT="$(
  timeout --signal=INT "${ASAN_DURATION}s" \
    "$ASAN_BIN" --config "$CONFIG" 2>&1
)"
status=$?
set -e

printf '%s\n' "$ASAN_OUTPUT"

echo "DONE"
echo "RSS saved to: $RSS_CSV"

if [[ "$status" -ne 0 && "$status" -ne 124 && "$status" -ne 130 ]]; then
  echo "ASan/LSan run failed with status $status" >&2
  exit "$status"
fi

if grep -Eq 'ERROR: LeakSanitizer: detected memory leaks|ERROR: AddressSanitizer|SUMMARY: AddressSanitizer|SUMMARY: LeakSanitizer' <<<"$ASAN_OUTPUT"; then
  echo "Leak summary: SANITIZER ERRORS DETECTED"
  grep -E 'ERROR: LeakSanitizer:|ERROR: AddressSanitizer:|SUMMARY: AddressSanitizer:|SUMMARY: LeakSanitizer:' <<<"$ASAN_OUTPUT" || true
  exit 101
fi

echo "Leak summary: no ASan/LSan errors detected"