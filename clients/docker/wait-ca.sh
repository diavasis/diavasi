#!/bin/sh
set -eu
while [ ! -s "${DIAVASI_CA:-}" ]; do
  sleep 0.2
done
