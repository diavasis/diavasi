# C client

`diavasi_consume` is a thin client of `diavasi.data.v1` on the gRPC C stack. It opens a TLS stream, sends the bearer token, Hello version 1, then JoinGroup, and acks each batch. The caller stores no cursor and does not dedupe on `record_id`. A dropped stream is how unacked batches return. Reconnect with the same consumer id and the server replays them.

There is no package registry for this client. Build it from this tree. There is no Jupyter kernel. Zig calls this library from [clients/zig/README.md](../zig/README.md). The Compose image is documented under Developing Diavasi in the root README.

## Install

`libgrpc` and `pkg-config` must be installed. `make` in `clients/c` builds `diavasi_consume`.

## Library

```c
#include "diavasi.h"

static void on_record(uint64_t batch_id, uint64_t record_id,
                      const uint8_t *payload, size_t payload_len, void *user) {
    (void)payload;
    (void)user;
    printf("batch %llu record %llu (%zu bytes)\n",
           (unsigned long long)batch_id,
           (unsigned long long)record_id,
           payload_len);
}

diavasi_options options = {
    .addr = "127.0.0.1:7710",
    .ca_path = "/tmp/diavasi-sdk/dataplane-ca.crt",
    .token = "sdk-demo",
    .group_id = "demo",
    .consumer_id = "c",
    .max_in_flight = 1,
    .expect_records = 8,
    .on_record = on_record,
};
diavasi_report report;
char error[256] = {0};
int rc = diavasi_consume(&options, &report, error, sizeof error);
if (rc >= 1 && rc <= 8) {
    fprintf(stderr, "protocol %d: %s\n", rc, error);
} else if (rc != 0) {
    fprintf(stderr, "%s\n", error);
}
diavasi_report_free(&report);
```

`on_record` runs before the batch is acked. `payload` is valid only for that call. `max_in_flight` defaults to 1 when left 0. `halt_after_acks` closes after that many acks and does not send Leave. `expect_records` sends Leave once that many records are acked.

Return 0 on success. Protocol codes 1 through 8 are returned as that integer: bad version, bad state, unknown ack, duplicate ack, group not running, unsupported, internal, heartbeat timeout. Any other failure returns -1. `error` receives the message. A bad token sets `grpc UNAUTHENTICATED: unauthorized`. A group that is not running sets `protocol error 5`.

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

cd clients/c
make process
./process
```

`examples/process.c` is that program. `make` also builds `./diavasi_consume`, the flag client used by the compatibility suite:

```bash
./diavasi_consume --addr 127.0.0.1:7710 --ca /tmp/diavasi-sdk/dataplane-ca.crt \
  --token sdk-demo --group demo --consumer c --total 8
```

Flags: `--addr`, `--ca`, `--token`, `--group`, `--consumer`, `--total`, `--max-in-flight` (default 1), `--halt-after`. The last occurrence of a flag wins. The program prints `record_ids` and `batch_ids`.

## Test

`make test-proto` checks the frame codec and does not need a server or libgrpc. `make test` also builds the gRPC program when `pkg-config` can see `grpc`. That program exits 0 until `DIAVASI_DATA_ADDR`, `DIAVASI_CA`, and `DIAVASI_API_TOKEN` are set.
