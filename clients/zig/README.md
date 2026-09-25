# Zig client

`consume` is a thin client of `diavasi.data.v1`. It opens a TLS stream, sends the bearer token, Hello version 1, then JoinGroup, and acks each batch. The caller stores no cursor and does not dedupe on `record_id`. A dropped stream is how unacked batches return. Reconnect with the same consumer id and the server replays them.

The transport is the C library in [`clients/c`](../c/README.md). `libgrpc` and `pkg-config` must be installed. Zig 0.16 is what this `build.zig` targets.

## Library

```zig
const diavasi = @import("diavasi");

fn onRecord(batch_id: u64, record_id: u64, payload: []const u8, ctx: ?*anyopaque) void {
    _ = ctx;
    std.debug.print("batch {d} record {d} ({d} bytes)\n", .{ batch_id, record_id, payload.len });
}

const outcome = try diavasi.consume(allocator, .{
    .addr = "127.0.0.1:7710",
    .ca_path = "/tmp/diavasi-sdk/dataplane-ca.crt",
    .token = "sdk-demo",
    .group_id = "demo",
    .consumer_id = "zig",
    .expect_records = 8,
    .on_record = onRecord,
});
switch (outcome) {
    .report => |report| {
        defer report.deinit(allocator);
        // report.record_ids and report.batch_ids
    },
    .failure => |failure| {
        defer failure.deinit(allocator);
        // failure.code is 1 through 8, or -1 for transport and auth
        // failure.message is the server or gRPC text
    },
}
```

`on_record` runs before the ack. `payload` is valid only for that call. `max_in_flight` defaults to 1. `halt_after_acks` of 0 is ignored. A positive value closes after that many acks and does not send Leave. `expect_records` of 0 reads until the stream ends. A positive value sends Leave once that many records are acked.

`failure.code` 1 through 8 is bad version, bad state, unknown ack, duplicate ack, group not running, unsupported, internal, heartbeat timeout. `-1` is transport or auth. A bad token is `grpc UNAUTHENTICATED: unauthorized`. A group that is not running is protocol code 5.

## Run

Start the server from the repo root:

```bash
cargo build -p diavasi-cli
export PATH="$PWD/target/debug:$PATH"
mkdir -p /tmp/diavasi-sdk
diavasi serve --bind 127.0.0.1:7700 --data-bind 127.0.0.1:7710 \
  --store /tmp/diavasi-sdk/state --token sdk-demo
```

In a second terminal, from the repo root:

```bash
curl -fsS -X DELETE -H "Authorization: Bearer sdk-demo" \
  http://127.0.0.1:7700/v1/groups/demo || true
curl -fsS -H "Authorization: Bearer sdk-demo" -H "content-type: application/json" \
  -d '{"group_id":"demo","total_records":8,"payload_size":8,"max_buffer_records":64,"max_buffer_bytes":65536,"batch_max_records":4,"batch_timeout_ms":200,"ordering_contract":"synthetic-u64"}' \
  http://127.0.0.1:7700/v1/groups
curl -fsS -X POST -H "Authorization: Bearer sdk-demo" \
  http://127.0.0.1:7700/v1/groups/demo/start

cd clients/zig
zig build run
```

`src/main.zig` is that program. Optional flags, last occurrence wins: `--addr`, `--ca`, `--token`, `--group`, `--consumer`.

Another Zig program in this repo depends on the module by pointing `build.zig` at `src/root.zig` and linking `libgrpc` the same way `clients/zig/build.zig` does.
