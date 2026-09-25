#!/usr/bin/env bash
# Start a synthetic group and run every SDK against it.
# Set DIAVASI_SDK_REQUIRE=1 to fail when a toolchain is missing.
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"

token=${DIAVASI_API_TOKEN_SUITE:-sdk-suite-token}
http_port=${DIAVASI_HTTP_PORT:-17700}
data_port=${DIAVASI_DATA_PORT:-17710}
store=$(mktemp -d)
pid=""

cleanup() {
  if [[ -n "$pid" ]]; then
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
  fi
  rm -rf "$store"
}
trap cleanup EXIT

require() {
  local bin=$1
  local name=$2
  if command -v "$bin" >/dev/null 2>&1; then
    return 0
  fi
  if [[ "${DIAVASI_SDK_REQUIRE:-}" == 1 ]]; then
    echo "missing toolchain: $bin" >&2
    exit 1
  fi
  echo "skip ${name}: ${bin} is not installed"
  return 1
}

cargo build -p diavasi-cli
"$root/target/debug/diavasi" serve \
  --bind "127.0.0.1:${http_port}" \
  --data-bind "127.0.0.1:${data_port}" \
  --store "$store/state" \
  --token "$token" >/tmp/diavasi-sdk-suite.log 2>&1 &
pid=$!

ready=0
for _ in $(seq 1 100); do
  if curl -sf "http://127.0.0.1:${http_port}/health" >/dev/null; then
    ready=1
    break
  fi
  sleep 0.2
done
if [[ "$ready" != 1 ]]; then
  echo "data plane did not start" >&2
  cat /tmp/diavasi-sdk-suite.log >&2 || true
  exit 1
fi

body='{"group_id":"sdk","total_records":8,"payload_size":8,"max_buffer_records":64,"max_buffer_bytes":65536,"batch_max_records":4,"batch_timeout_ms":200,"ordering_contract":"synthetic-u64"}'
curl -sf -H "Authorization: Bearer ${token}" -H "content-type: application/json" -d "$body" \
  "http://127.0.0.1:${http_port}/v1/groups" >/dev/null
curl -sf -X POST -H "Authorization: Bearer ${token}" \
  "http://127.0.0.1:${http_port}/v1/groups/sdk/start" >/dev/null

export DIAVASI_DATA_ADDR="127.0.0.1:${data_port}"
export DIAVASI_CA="${store}/dataplane-ca.crt"
export DIAVASI_API_TOKEN="$token"
export DIAVASI_GROUP=sdk
export DIAVASI_TOTAL=8

base=(--addr "$DIAVASI_DATA_ADDR" --ca "$DIAVASI_CA" --total 8)

create_group() {
  local id=$1
  local body
  body=$(GROUP_ID="$id" python3 - <<'PY'
import json, os
print(json.dumps({
    "group_id": os.environ["GROUP_ID"],
    "total_records": 8,
    "payload_size": 8,
    "max_buffer_records": 64,
    "max_buffer_bytes": 65536,
    "batch_max_records": 4,
    "batch_timeout_ms": 200,
    "ordering_contract": "synthetic-u64",
}))
PY
)
  curl -sf -H "Authorization: Bearer ${token}" -H "content-type: application/json" -d "$body" \
    "http://127.0.0.1:${http_port}/v1/groups" >/dev/null
  curl -sf -X POST -H "Authorization: Bearer ${token}" \
    "http://127.0.0.1:${http_port}/v1/groups/${id}/start" >/dev/null
}

ids_of() {
  awk '/^record_ids /{ $1=""; sub(/^ /,""); print }'
}

check_reconnect() {
  python3 - "$1" "$2" <<'PY'
import sys
def ids(text):
    for line in text.splitlines():
        if line.startswith("record_ids "):
            return line.split()[1:]
    raise SystemExit("missing record_ids")
a = ids(sys.argv[1])
b = ids(sys.argv[2])
if not a or set(a) & set(b):
    raise SystemExit(f"acked records were replayed: {a} {b}")
union = set(a) | set(b)
expect = {str(i) for i in range(1, 9)}
if union != expect:
    raise SystemExit(f"union {sorted(union, key=int)} != 1..8")
PY
}

run_sdk() {
  local name=$1
  shift
  echo "== ${name} =="
  create_group "${name}-full"
  create_group "${name}-re"
  local full first second bad missing
  full=$("$@" "${base[@]}" --token "$token" --group "${name}-full" --consumer "${name}-full")
  echo "$full"
  local count
  count=$(echo "$full" | ids_of | wc -w | tr -d ' ')
  if [[ "$count" != 8 ]]; then
    echo "${name} consumed ${count} records" >&2
    exit 1
  fi
  first=$("$@" "${base[@]}" --token "$token" --group "${name}-re" --consumer "${name}-re" --halt-after 1)
  local got rest
  got=$(echo "$first" | ids_of | wc -w | tr -d ' ')
  rest=$((8 - got))
  if [[ "$rest" -le 0 ]]; then
    echo "${name} halted after every record" >&2
    exit 1
  fi
  second=$("$@" "${base[@]}" --token "$token" --group "${name}-re" --consumer "${name}-re" --total "$rest")
  check_reconnect "$first" "$second"
  set +e
  bad=$("$@" "${base[@]}" --token wrong-token --group "${name}-full" --consumer "${name}-bad" 2>&1)
  local bad_code=$?
  missing=$("$@" "${base[@]}" --token "$token" --group sdk-missing --consumer "${name}-missing" 2>&1)
  local missing_code=$?
  set -e
  if [[ "$bad_code" -eq 0 ]] || ! grep -Eiq 'unauth|unauthorized' <<<"$bad"; then
    echo "${name} bad token did not fail: ${bad}" >&2
    exit 1
  fi
  if [[ "$missing_code" -eq 0 ]] || ! grep -Eiq 'protocol error 5' <<<"$missing"; then
    echo "${name} missing group did not fail: ${missing}" >&2
    exit 1
  fi
}

elixir_consume() {
  (cd "$root/clients/elixir" && mix diavasi.consume "$@")
}
go_consume() {
  (cd "$root/clients/go" && go run ./cmd/consume "$@")
}
js_consume() {
  (cd "$root/clients/js" && node examples/consume.js "$@")
}
java_consume() {
  "$root/clients/java/build/install/diavasi-data/bin/diavasi-data" "$@"
}
csharp_consume() {
  dotnet run --project "$root/clients/csharp/Diavasi.Data/Diavasi.Data.csproj" -c Release --no-build -- "$@"
}

py=python3
if [[ -x "$root/clients/python/.venv/bin/python" ]]; then
  py="$root/clients/python/.venv/bin/python"
fi
export PYTHONPATH="$root/clients/python${PYTHONPATH:+:$PYTHONPATH}"
run_sdk python "$py" -m diavasi_data
create_group python-lib
(cd "$root/clients/python" && DIAVASI_GROUP=python-lib PYTHONPATH=. "$py" -m unittest test_consume.py)

if require elixir elixir; then
  (cd "$root/clients/elixir" && mix deps.get && mix compile)
  export MIX_QUIET=1
  run_sdk elixir elixir_consume
  create_group elixir-lib
  (cd "$root/clients/elixir" && DIAVASI_GROUP=elixir-lib mix test)
fi

if require cargo rust; then
  create_group rust-lib
  DIAVASI_GROUP=rust-lib cargo test --manifest-path "$root/clients/rust/Cargo.toml"
  cargo build --manifest-path "$root/clients/rust/Cargo.toml" --bin diavasi-consume
  run_sdk rust "$root/clients/rust/target/debug/diavasi-consume"
fi

if require go go; then
  create_group go-lib
  (cd "$root/clients/go" && DIAVASI_GROUP=go-lib go test ./...)
  run_sdk go go_consume
fi

if require node javascript; then
  (cd "$root/clients/js" && npm install)
  create_group js-lib
  (cd "$root/clients/js" && DIAVASI_GROUP=js-lib node --test)
  run_sdk js js_consume
fi

if require java java && require gradle java; then
  create_group java-lib
  (cd "$root/clients/java" && DIAVASI_GROUP=java-lib gradle --no-daemon test installDist)
  run_sdk java java_consume
fi

if require dotnet csharp; then
  create_group csharp-lib
  DIAVASI_GROUP=csharp-lib dotnet test "$root/clients/csharp/Diavasi.Data.Tests/Diavasi.Data.Tests.csproj" -c Release
  dotnet build "$root/clients/csharp/Diavasi.Data/Diavasi.Data.csproj" -c Release
  run_sdk csharp csharp_consume
fi

make -C "$root/clients/c" test-proto
if pkg-config --exists grpc; then
  make -C "$root/clients/c" diavasi_consume consume_test
  create_group c-lib
  DIAVASI_GROUP=c-lib ./clients/c/consume_test
  run_sdk c "$root/clients/c/diavasi_consume"
elif [[ "${DIAVASI_SDK_REQUIRE:-}" == 1 ]]; then
  echo "missing toolchain: grpc" >&2
  exit 1
else
  echo "skip c: grpc headers are not installed"
fi

echo "suite finished"
