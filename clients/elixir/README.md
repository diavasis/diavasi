# Diavasi Stage 0 Elixir client

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
