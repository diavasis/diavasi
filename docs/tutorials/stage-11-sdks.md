# Stage 11: client SDKs

The server owns the cursor. Each SDK opens `DataPlane.Consume`, sends `Hello` version 1, joins, and yields `RecordBatch` values. The caller acks by `batch_id`. `record_id` can be 0, so clients do not dedupe on it. A dropped stream is how unacked batches come back. See [ADR 0012](../adr/0012-client-sdks.md).

This walkthrough uses a synthetic group, so no database is required.

## Server

```bash
cargo build -p diavasi-cli
export PATH="$PWD/target/debug:$PATH"
diavasi serve --bind 127.0.0.1:7700 --data-bind 127.0.0.1:7710 \
  --store /tmp/diavasi-sdk/state --token sdk-demo
```

The CA is written next to the store, at `/tmp/diavasi-sdk/dataplane-ca.crt`.

```bash
curl -H "Authorization: Bearer sdk-demo" -H "content-type: application/json" \
  -d '{"group_id":"demo","total_records":8,"payload_size":8,"max_buffer_records":64,"max_buffer_bytes":65536,"batch_max_records":4,"batch_timeout_ms":200,"ordering_contract":"synthetic-u64"}' \
  http://127.0.0.1:7700/v1/groups
curl -X POST -H "Authorization: Bearer sdk-demo" http://127.0.0.1:7700/v1/groups/demo/start
```

## Elixir, Rust, and Python

Elixir, from `clients/elixir`:

```bash
mix diavasi.consume --addr 127.0.0.1:7710 --ca /tmp/diavasi-sdk/dataplane-ca.crt \
  --token sdk-demo --group demo --consumer elixir --total 8
```

`Diavasi.Data.Client` is a supervised process. `stream/1` yields batches and `ack/2` confirms one `batch_id`.

Rust, from `clients/rust`:

```bash
cargo run --bin diavasi-consume -- --addr 127.0.0.1:7710 \
  --ca /tmp/diavasi-sdk/dataplane-ca.crt --token sdk-demo \
  --group demo --consumer rust --total 8
```

Python, from `clients/python` with `PYTHONPATH` set to that directory:

```bash
python -m diavasi_data --addr 127.0.0.1:7710 --ca /tmp/diavasi-sdk/dataplane-ca.crt \
  --token sdk-demo --group demo --consumer python --total 8
```

`consume()` is the iterator. `Session.ack` is the same call when the application wants to ack itself.

Each example prints `batch_ids` and then a one-line count. The same flags exist for Go (`clients/go`), JavaScript (`clients/js`), Java (`clients/java`), C# (`clients/csharp`), and C (`clients/c`).

## Compose

One file, `clients/docker-compose.yml`, builds the server, writes the CA onto a volume, and creates the synthetic group `demo`. There is no database container. Each language is a profile:

```bash
docker compose -f clients/docker-compose.yml --profile python up --abort-on-container-exit
```

Use `elixir`, `rust`, `go`, `js`, `java`, `csharp`, or `c` the same way. `--profile all` starts every demo after the seed. The CA and token come from the shared volume and the environment.

## Notebooks

```bash
docker compose -f clients/docker-compose.yml --profile notebook up
```

JupyterLab listens on port 8888. The image has kernels for Python, Rust, Go, JavaScript, Java, and C#. The first build takes a long time because those toolchains are installed in separate stages. Elixir uses the Livebook service in the same Compose file, on port 8080, with `clients/elixir/notebooks/demo.livemd`. C stays on the Compose profile. There is no C kernel in the Jupyter image.

`clients/scripts/compat.sh` runs every SDK against one synthetic group: a full consume, a reconnect with the same consumer id, a bad token, and a group that is not running.
