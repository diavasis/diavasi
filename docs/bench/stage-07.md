# Stage 7 end-to-end benchmark

Path: PostgreSQL keyset fetch, group buffer, TLS gRPC consumers, ack, redb checkpoint.

The harness is `diavasi-e2e-bench` in `diavasi-adapter-postgres`. It is separate from `diavasi-transport-bench`. One invocation loads a table, starts the server, drains it, and appends one JSONL row.

Checked-in rows were taken on an Apple M1 Max with 64GB of memory, release build, Compose Postgres 16 on `127.0.0.1:5433` (`postgres:16-alpine`). `records_per_sec` counts delivered records, including at-least-once redeliveries. `mib_per_sec` is payload-column bytes, not protobuf size. Ack latency is the time from receiving a batch until the ack is queued, so it includes `--ack-delay-ms`.

## Command

```bash
export DATABASE_URL=postgres://diavasi:diavasi@127.0.0.1:5433/diavasi
cargo run --release -p diavasi-adapter-postgres --bin diavasi-e2e-bench -- \
  --smoke --output docs/bench/stage-07.jsonl
```

`--smoke` is 20_000 rows, 64-byte payload, batch size 200, one consumer, one group. CI runs that smoke without `--release` and writes to a temporary file. The matrix below is manual. Change one flag at a time and keep the others at the smoke defaults (`--rows 20000 --payload-bytes 64 --batch-max-records 200 --consumers 1 --groups 1 --ack-delay-ms 0 --max-buffer-records 4096 --max-in-flight 1`).

| Axis | Flag |
| --- | --- |
| Row size | `--payload-bytes` |
| Batch size | `--batch-max-records` |
| Consumers | `--consumers` |
| Groups | `--groups` |
| Ack latency | `--ack-delay-ms` |
| Buffer | `--max-buffer-records` (and `--max-buffer-bytes`) |
| Row count | `--rows` |

The million-row point in the table was produced with:

```bash
cargo run --release -p diavasi-adapter-postgres --bin diavasi-e2e-bench -- \
  --label rows-1m --rows 1000000 --output docs/bench/stage-07.jsonl
```

## Results

| Label | Records/s | MiB/s | Ack p50 / p99 | Notes |
| --- | --- | --- | --- | --- |
| smoke | 16299 | 0.995 | 0 / 3 µs | 20_000 × 64 bytes, batch 200, 1.23s |
| payload-1024 | 14003 | 13.675 | 0 / 11 µs | Same rows, 1024-byte payload |
| batch-20 | 1808 | 0.110 | 0 / 4 µs | 1000 acks, each checkpointed in redb |
| consumers-4 | 19810 | 1.209 | 0 / 2 µs | 20_600 deliveries, 20_000 distinct ids |
| groups-2 | 21507 | 1.313 | 0 / 4 µs | Two groups, 40_000 deliveries, one connection each |
| ack-5ms | 12039 | 0.735 | 6.2 / 11.7 ms | Delay is included in the ack latency |
| buffer-128 | 9445 | 0.576 | 1 / 5 µs | Buffer smaller than the batch cap, 157 batches |
| rows-200k | 18605 | 1.136 | 0 / 5 µs | 200_000 rows in 10.7s |
| rows-1m | 17523 | 1.070 | 0 / 4 µs | 1_000_000 rows in 57s |

`consumers-4` redelivered 600 records. That is the at-least-once contract: every id from 1 to 20_000 was delivered, and a few batches were delivered twice. Two groups read the same table independently, so the record count is the table size times the group count.

The batch-size row is the expensive axis. A 20-record batch does ten times as many contiguous checkpoints as a 200-record batch, and records/s fall by about the same factor. Payload width and a second group do not. No further change was made to the fetch interval or the keyset SQL.

Full rows, including buffer and batch settings, are in [stage-07.jsonl](stage-07.jsonl).
