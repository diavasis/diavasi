# Stage 11: client SDKs

The server owns the cursor. Each SDK opens `DataPlane.Consume`, sends `Hello` version 1, joins, and yields batches. The caller acks by `batch_id`. `record_id` can be 0, so clients do not dedupe on it. A dropped stream is how unacked batches come back. See [ADR 0012](../adr/0012-client-sdks.md).

## Get started

The root [README](../../README.md) starts the server and the Python consumer with one Compose command. That path uses a synthetic group named `demo` and needs no database.

```bash
docker compose -f clients/docker-compose.yml --profile python up --abort-on-container-exit
```

## Clients

Each language has its own page: the library call, the example, the errors, and the test.

- [Elixir](../../clients/elixir/README.md)
- [Rust](../../clients/rust/README.md)
- [Python](../../clients/python/README.md)
- [Go](../../clients/go/README.md)
- [JavaScript](../../clients/js/README.md)
- [Java](../../clients/java/README.md)
- [C#](../../clients/csharp/README.md)
- [C](../../clients/c/README.md)
- [Zig](../../clients/zig/README.md), calling the C library

The index, including Compose profiles and the notebook image, is [clients/README.md](../../clients/README.md).

## A server you start yourself

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

Point any client at `127.0.0.1:7710`, that CA, and the token `sdk-demo`. The example flags are on each client page.

`clients/scripts/compat.sh` runs every installed SDK against its own synthetic groups: a full consume, a reconnect with the same consumer id, a bad token, and a group that is not running.
