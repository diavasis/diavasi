# Elixir client

`Diavasi.Data.Client` is a supervised consumer of `diavasi.data.v1`. It opens a TLS stream, sends the bearer token, Hello version 1, then JoinGroup. `stream/1` yields batches. The caller acks with `ack/2`. `leave/1` is the clean stop. Dropping the process returns unacked batches to the server. The client stores no cursor and does not dedupe on `record_id`. Reconnect with the same consumer id and the server replays them.

The Hex package name `diavasi` is reserved and unpublished. The Mix app in this tree is `:diavasi_bench`.

## Install

```elixir
{:diavasi_bench, path: "clients/elixir"}
```

## Library

```elixir
{:ok, pid} = Diavasi.Data.Client.start_link(
  addr: "127.0.0.1:7710",
  ca: "/tmp/diavasi-sdk/dataplane-ca.crt",
  token: "sdk-demo",
  group: "demo",
  consumer: "elixir",
  max_in_flight: 1
)

pid
|> Diavasi.Data.Client.stream()
|> Enum.each(fn batch ->
  IO.inspect(batch.batch_id)
  Diavasi.Data.Client.ack(pid, batch.batch_id)
end)

Diavasi.Data.Client.leave(pid)
```

`run/1` consumes `:total` records and acks each batch. `:halt_after` closes after that many acks and does not send Leave. `disconnect/1` closes the stream the same way.

A bad token fails the start with a gRPC unauthorized error. A group that is not running fails with `protocol error 5`. Protocol codes are 1 bad version, 2 bad state, 3 unknown ack, 4 duplicate ack, 5 group not running, 6 unsupported, 7 internal, 8 heartbeat timeout.

## Example

```bash
cd clients/elixir
mix diavasi.consume --addr 127.0.0.1:7710 --ca /tmp/diavasi-sdk/dataplane-ca.crt \
  --token sdk-demo --group demo --consumer elixir --total 8
```

Flags: `--addr`, `--ca`, `--token`, `--group`, `--consumer`, `--total`, `--max-in-flight` (default 1), `--halt-after`. The task prints `record_ids` and `batch_ids`.

```bash
docker compose -f clients/docker-compose.yml --profile elixir up --abort-on-container-exit
```

Livebook notes are in `notebooks/`. The Compose `notebook` profile serves [notebooks/demo.livemd](notebooks/demo.livemd) on port 8080.

## Test

`mix test` passes without a server. With `DIAVASI_DATA_ADDR`, `DIAVASI_CA`, and `DIAVASI_API_TOKEN` set, it consumes `DIAVASI_TOTAL` records (default 8) from `DIAVASI_GROUP`.

## Stage 0 bench

The TCP bench task in this tree speaks a different protocol from `data.proto`.

```bash
mise install
cd clients/elixir
mise exec -- mix deps.get
```

```bash
# terminal 1
cargo run -p diavasi --bin diavasi-transport-bench --features transport-bench -- \
  --transport tcp --role server --listen 127.0.0.1:9800 --total-records 200 --smoke

# terminal 2
cd clients/elixir
mise exec -- mix diavasi.bench --connect 127.0.0.1:9800 --total-records 200 \
  --output ../../docs/bench/results.jsonl
```
