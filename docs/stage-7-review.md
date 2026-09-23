# Stage 7 review

Status: complete. Stop here. Do not start Stage 8.

## What shipped

- `diavasi-e2e-bench` measures PostgreSQL, the group buffer, gRPC consumers, acks, and the redb checkpoint. Results and the rerun command are in [docs/bench/stage-07.md](bench/stage-07.md).
- [docs/resource-model.md](resource-model.md) separates an idle group (one owner, two tickers, one Postgres connection, empty buffer) from an active group (full buffer plus in-flight batches).
- CI runs the 20_000-row smoke. The rest of the matrix, including a million rows, stays manual.

## What the measurement changed

Two bounds were wrong in practice, and both dropped records or hid them until `batch_timeout`.

- A timed-out batch could not re-enter a buffer that fetch had already refilled, and the tick error was ignored. The batch now stays in flight and a later tick retries it. The buffer cap still holds.
- `tokio::select!` on the data plane could drop an assign after the owner had moved the batch into inflight. The batch then sat until `batch_timeout`, and a 20_000-row run took 30s. The assign future is now kept across heartbeats, and a reply whose caller is gone is requeued immediately. The same run is about 1.2s and stays there across repeats.

Checkpoint cost is visible and was not rewritten. A 20-record batch is about 1.8e3 records/s; a 200-record batch is about 1.6e4. Each ack that extends the committed cursor is one redb transaction.

## Left as they are

Consumers per group, groups per process, and supervisor restarts after a fetch error are unbounded on purpose. A restart cap would fight crash recovery. There is no process-wide memory accountant.

Fetch interval and the keyset SQL were not tuned. The resource model states the cost that was measured.
