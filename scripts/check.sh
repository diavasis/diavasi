#!/usr/bin/env bash
# Format, lint, test, license check, and coverage.
# Uses DATABASE_URL, MONGODB_URL, and REDIS_URL when set. Otherwise the
# Compose Postgres on port 5433, MongoDB on port 27017, and Redis on port
# 6379, so the adapter tests run and show up in coverage.

set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -z "${DATABASE_URL:-}" ]]; then
  export DATABASE_URL="postgres://diavasi:diavasi@127.0.0.1:5433/diavasi"
fi

if [[ -z "${MONGODB_URL:-}" ]]; then
  export MONGODB_URL="mongodb://127.0.0.1:27017"
fi

if [[ -z "${REDIS_URL:-}" ]]; then
  export REDIS_URL="redis://127.0.0.1:6379"
fi

# Library line coverage stays near 88% when the Postgres adapter tests run.
# The Stage 0 transport harness and the Stage 7 end-to-end bench are not
# exercised by `cargo test`, so they are left out of this number.
min_line_coverage=85

echo "==> fmt"
cargo fmt --check

echo "==> clippy"
cargo clippy --all-targets --all-features -- -D warnings

echo "==> test"
cargo test --all --all-features

echo "==> deny"
cargo deny check

echo "==> coverage"
cargo llvm-cov --workspace --all-features \
  --ignore-filename-regex 'transport_bench|e2e_bench' \
  --summary-only \
  --fail-under-lines "$min_line_coverage"
