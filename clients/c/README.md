# C client

`diavasi_consume` is a thin client of `diavasi.data.v1` on the gRPC C stack. It opens a TLS stream, sends the bearer token, Hello version 1, then JoinGroup, and acks each batch. The caller stores no cursor and does not dedupe on `record_id`. A dropped stream is how unacked batches return. Reconnect with the same consumer id and the server replays them.

There is no package registry for this client. Build it from this tree. There is no Jupyter kernel. The demo is the Compose profile.

## Install

`libgrpc` and `pkg-config` must be installed. `make` in `clients/c` builds `diavasi_consume`.

## Library

```c
#include "diavasi.h"

diavasi_options options = {
    .addr = "127.0.0.1:7710",
    .ca_path = "/tmp/diavasi-sdk/dataplane-ca.crt",
    .token = "sdk-demo",
    .group_id = "demo",
    .consumer_id = "c",
    .max_in_flight = 1,
    .expect_records = 8,
};
diavasi_report report;
char error[256];
int rc = diavasi_consume(&options, &report, error, sizeof error);
diavasi_report_free(&report);
```

`max_in_flight` defaults to 1 when left 0. `halt_after_acks` closes after that many acks and does not send Leave. `expect_records` sends Leave once that many records are acked.

Return 0 on success. Protocol codes 1 through 8 are returned as that integer: bad version, bad state, unknown ack, duplicate ack, group not running, unsupported, internal, heartbeat timeout. Any other failure returns -1. `error` receives the message. A bad token sets `grpc UNAUTHENTICATED: unauthorized`. A group that is not running sets `protocol error 5`.

## Example

```bash
cd clients/c
make
./diavasi_consume --addr 127.0.0.1:7710 --ca /tmp/diavasi-sdk/dataplane-ca.crt \
  --token sdk-demo --group demo --consumer c --total 8
```

Flags: `--addr`, `--ca`, `--token`, `--group`, `--consumer`, `--total`, `--max-in-flight` (default 1), `--halt-after`. The last occurrence of a flag wins. The program prints `record_ids` and `batch_ids`.

```bash
docker compose -f clients/docker-compose.yml --profile c up --abort-on-container-exit
```

## Test

`make test-proto` checks the frame codec and does not need a server or libgrpc. `make test` also builds the gRPC program when `pkg-config` can see `grpc`. That program exits 0 until `DIAVASI_DATA_ADDR`, `DIAVASI_CA`, and `DIAVASI_API_TOKEN` are set.
