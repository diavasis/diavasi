# Tutorial: Stage 12 observability

This tutorial scrapes metrics, checks readiness, and reads group diagnostics for a synthetic group. The server is the same `diavasi serve` as the earlier stages.

Related code: `crates/diavasi-core/src/observe.rs`, `crates/diavasi-core/src/control/`, `crates/diavasi-core/src/runtime/`. Decision record: [ADR 0013](../adr/0013-observability.md). Reference: [observability](../observability.md).

## 1. Start the server

From the repo root:

```bash
cargo build -p diavasi
export PATH="$PWD/target/debug:$PATH"
mkdir -p /tmp/diavasi-sdk
export DIAVASI_API_TOKEN=sdk-demo
diavasi serve --bind 127.0.0.1:7700 --data-bind 127.0.0.1:7710 \
  --store /tmp/diavasi-sdk/state --token sdk-demo
```

## 2. Liveness, readiness, and a group

In a second terminal, from the repo root:

```bash
export PATH="$PWD/target/debug:$PATH"
export DIAVASI_URL=http://127.0.0.1:7700
export DIAVASI_API_TOKEN=sdk-demo

curl -fsS http://127.0.0.1:7700/health
curl -fsS http://127.0.0.1:7700/ready

curl -fsS -X DELETE -H "Authorization: Bearer sdk-demo" \
  http://127.0.0.1:7700/v1/groups/demo || true
diavasi group create --group-id demo --total-records 8
diavasi group start demo

curl -fsS -H "Authorization: Bearer sdk-demo" http://127.0.0.1:7700/metrics
diavasi group diagnostics demo
```

`/health` and `/ready` print `ok`. `/metrics` is Prometheus text and includes `diavasi_groups_running`. `group diagnostics` shows `running=true`. In that text line, `fetched` and `acked` are record counters. `fetched` climbs as the synthetic source fills the buffer. `acked` stays 0 until a consumer acks. `lag` is buffer records plus in-flight records.

JSON for the same view:

```bash
diavasi --output json group diagnostics demo
```

## 3. After pause

```bash
diavasi group pause demo
diavasi group diagnostics demo
curl -fsS -H "Authorization: Bearer sdk-demo" http://127.0.0.1:7700/metrics | head
```

Diagnostics reports `running=false` and `reason=paused`. The buffer gauges for `demo` are gone from the scrape. `diavasi_group_records_fetched_total` can still show what this process already read.

## 4. Logs

Stop the server and start it again with JSON lines:

```bash
DIAVASI_LOG_FORMAT=json diavasi serve --bind 127.0.0.1:7700 --data-bind 127.0.0.1:7710 \
  --store /tmp/diavasi-sdk/state --token sdk-demo
```

`RUST_LOG` defaults to `info`. `RUST_LOG=debug` also prints the `group_fetch` and `group_ack` spans. Those spans are not exported anywhere else.
