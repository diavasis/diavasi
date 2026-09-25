# Python client

`diavasi_data.consume` is a thin client of `diavasi.data.v1`. It opens a TLS stream, sends the bearer token, Hello version 1, then JoinGroup. The iterator yields each batch. Continuing the iterator acks that `batch_id`. The client stores no cursor and does not dedupe on `record_id`. A dropped stream is how unacked batches return. Reconnect with the same `consumer_id` and the server replays them.

The PyPI name `diavasi-data` is reserved and unpublished. Install from this repo.

## Install

```bash
python -m venv clients/python/.venv
clients/python/.venv/bin/pip install -r clients/python/requirements.txt
export PYTHONPATH=clients/python
```

## Library

```python
from diavasi_data import CallError, ProtocolError, consume

try:
    for batch in consume(
        addr="127.0.0.1:7710",
        ca="/tmp/diavasi-sdk/dataplane-ca.crt",
        token="sdk-demo",
        group_id="demo",
        consumer_id="python",
        expect_records=8,
    ):
        for record in batch.records:
            print(f"batch {batch.batch_id} record {record.record_id} ({len(record.payload)} bytes)")
except ProtocolError as err:
    print(f"protocol {err.code}: {err.message}")
except CallError as err:
    print(f"grpc {err.status}: {err.message}")
```

The same program is `clients/python/examples/process.py`. From the repo root, after the server and the `demo` group are up:

```bash
PYTHONPATH=clients/python clients/python/.venv/bin/python clients/python/examples/process.py
```

`consume()` acks in a `finally` around the yield, so breaking out of the loop still acks the current batch. `expect_records` sends Leave once that many records are acked. `halt_after_acks` closes after that many acks and does not send Leave. `Session` is the same stream when the caller wants to call `ack` itself.

`ProtocolError` carries the protocol code. `CallError` carries a gRPC status. A bad token raises `CallError` with status `UNAUTHENTICATED` and message `unauthorized`. A group that is not running raises `ProtocolError` with code 5.

| Code | Meaning |
| --- | --- |
| 1 | Bad version |
| 2 | Bad state |
| 3 | Unknown ack |
| 4 | Duplicate ack |
| 5 | Group is not running |
| 6 | Unsupported |
| 7 | Internal |
| 8 | Heartbeat timeout |

## Example

```bash
PYTHONPATH=clients/python python -m diavasi_data \
  --addr 127.0.0.1:7710 --ca /tmp/diavasi-sdk/dataplane-ca.crt \
  --token sdk-demo --group demo --consumer python --total 8
```

Flags: `--addr`, `--ca`, `--token`, `--group`, `--consumer`, `--total`, `--max-in-flight` (default 1), `--halt-after`. The last occurrence of a flag wins. The example prints `record_ids` and `batch_ids`.

```bash
docker compose -f clients/docker-compose.yml --profile python up --abort-on-container-exit
```

## Test

From `clients/python`, `python -m unittest test_consume.py` returns immediately until `DIAVASI_DATA_ADDR`, `DIAVASI_CA`, and `DIAVASI_API_TOKEN` are set. With those set, it consumes `DIAVASI_TOTAL` records (default 8) from `DIAVASI_GROUP`.

## Stage 0 bench

The TCP bench client in this tree speaks a different protocol from `data.proto`.

```bash
mise install
mise exec -- python -m venv clients/python/.venv
mise exec -- clients/python/.venv/bin/pip install -r clients/python/requirements.txt
mise exec -- clients/python/.venv/bin/python -m diavasi_bench.gen_proto
```

```bash
# terminal 1
cargo run -p diavasi --bin diavasi-transport-bench --features transport-bench -- \
  --transport tcp --role server --listen 127.0.0.1:9800 --smoke

# terminal 2
cd clients/python
PYTHONPATH=. .venv/bin/python -m diavasi_bench \
  --connect 127.0.0.1:9800 --total-records 200 \
  --output ../../docs/bench/results.jsonl
```
