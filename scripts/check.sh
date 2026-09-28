#!/usr/bin/env bash
# Format, lint, docs, test, license check, and coverage.
# Uses DATABASE_URL, MONGODB_URL, REDIS_URL, and SCYLLA_URL when set.
# Otherwise the Compose Postgres on port 5433, MongoDB on port 27017, Redis
# on port 6379, and ScyllaDB on port 9042, so the adapter tests run and show
# up in coverage.

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

if [[ -z "${SCYLLA_URL:-}" ]]; then
  export SCYLLA_URL="127.0.0.1:9042"
fi

# Database tests fail instead of skipping when a URL is missing.
export DIAVASI_REQUIRE_DB=1

# Library line coverage stays near 88% when the Postgres adapter tests run.
# The transport benchmark (its harness and protocol) and the end-to-end bench
# are not exercised by `cargo test`, so they are left out of this number.
min_line_coverage=85

echo "==> fmt"
cargo fmt --check

echo "==> clippy"
cargo clippy --all-targets --all-features -- -D warnings

echo "==> docs"
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features --workspace \
  --exclude diavasi --exclude diavasi-bench

echo "==> test"
cargo test --all --all-features

echo "==> deny"
cargo deny check

echo "==> coverage"
cargo llvm-cov --workspace --all-features \
  --ignore-filename-regex 'transport_bench|bench_protocol|e2e_bench' \
  --summary-only \
  --fail-under-lines "$min_line_coverage"
