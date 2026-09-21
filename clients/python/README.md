# Diavasi Stage 0 Python client

## Setup (mise)

From the repo root:

```bash
mise install
mise exec -- python -m venv clients/python/.venv
mise exec -- clients/python/.venv/bin/pip install -r clients/python/requirements.txt
mise exec -- clients/python/.venv/bin/python -m diavasi_bench.gen_proto
```

`PYTHONPATH` must include `clients/python` when running.

## Run against a Rust TCP server

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
