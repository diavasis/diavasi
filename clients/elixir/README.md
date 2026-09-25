# Diavasi Elixir client

`Diavasi.Data.Client` is a supervised consumer of the TLS gRPC data plane. `stream/1` yields batches. The caller acks with `ack/2`. The Stage 0 TCP bench task is still in this tree.

```bash
mix diavasi.consume --addr 127.0.0.1:7710 --ca /tmp/diavasi-sdk/dataplane-ca.crt \
  --token sdk-demo --group demo --consumer elixir --total 8
```

`mix test` skips the server assertion until `DIAVASI_DATA_ADDR`, `DIAVASI_CA`, and `DIAVASI_API_TOKEN` are set. Livebook notes live in `notebooks/`.

## Stage 0 bench

## Setup (mise)

```bash
mise install
cd clients/elixir
mise exec -- mix deps.get
```

## Run against a Rust TCP server

```bash
# terminal 1
cargo run -p diavasi --bin diavasi-transport-bench --features transport-bench -- \
  --transport tcp --role server --listen 127.0.0.1:9800 --total-records 200 --smoke

# terminal 2
cd clients/elixir
mise exec -- mix diavasi.bench --connect 127.0.0.1:9800 --total-records 200 \
  --output ../../docs/bench/results.jsonl
```
