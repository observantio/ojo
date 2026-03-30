#!/usr/bin/env bash
set -euo pipefail

exec docker run --rm \
  --platform linux/amd64 \
  -v /home/stefan:/home/stefan \
  -v /tmp:/tmp \
  -w "$PWD" \
  --entrypoint /usr/local/bin/sparcv9-sun-solaris2.10-gcc \
  japaric/sparcv9-sun-solaris \
  -L/home/stefan/ojo/solaris-builder/solaris-compat/sparcv9 \
  "$@" \
  -lxnet7compat
