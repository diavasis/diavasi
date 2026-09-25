# JavaScript client

`@diavasi/data` is a thin client of `diavasi.data.v1`, built on `@grpc/grpc-js`. `consume` opens a TLS stream, sends the bearer token, Hello version 1, then JoinGroup, and acks each batch. The client stores no cursor and does not dedupe on `record_id`. A dropped stream is how unacked batches return. Reconnect with the same consumer id and the server replays them.

The npm name `@diavasi/data` is reserved. The package in this tree is `private` and unpublished. TypeScript types are in `index.d.ts`.

## Install

```bash
npm install ./clients/js
```

From `clients/js`, `npm install` installs the gRPC dependencies used by the example.

## Library

```js
const { CallError, ProtocolError, consume } = require("@diavasi/data");

try {
  const report = await consume({
    addr: "127.0.0.1:7710",
    ca: "/tmp/diavasi-sdk/dataplane-ca.crt",
    token: "sdk-demo",
    groupId: "demo",
    consumerId: "js",
    expectRecords: 8,
    onBatch(batch) {
      for (const record of batch.records) {
        console.log(
          `batch ${batch.batchId} record ${record.recordId} (${record.payload.length} bytes)`,
        );
      }
    },
  });
  console.log(report.recordIds);
} catch (err) {
  if (err instanceof ProtocolError) {
    console.error(`protocol ${err.code}: ${err.message}`);
  } else if (err instanceof CallError) {
    console.error(`grpc ${err.status}: ${err.message}`);
  } else {
    throw err;
  }
}
```

`onBatch` runs before the ack. `maxInFlight` defaults to 1. `haltAfterAcks` closes after that many acks and does not send Leave. `expectRecords` sends Leave once that many records are acked. `protoPath` overrides the path to `data.proto` (the default walks to `crates/diavasi/proto/data.proto`; set `DIAVASI_PROTO` in the Compose image).

`ProtocolError` carries codes 1 through 8: bad version, bad state, unknown ack, duplicate ack, group not running, unsupported, internal, heartbeat timeout. `CallError` is a gRPC status. A bad token is `UNAUTHENTICATED` with message `unauthorized`. A group that is not running is protocol code 5.

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

cd clients/js
npm install
node examples/process.js
```

`examples/process.js` is the program above. `examples/consume.js` is the flag client used by the compatibility suite:

```bash
node examples/consume.js --addr 127.0.0.1:7710 --ca /tmp/diavasi-sdk/dataplane-ca.crt \
  --token sdk-demo --group demo --consumer js --total 8
```

Flags: `--addr`, `--ca`, `--token`, `--group`, `--consumer`, `--total`, `--max-in-flight` (default 1), `--halt-after`. The last occurrence of a flag wins. The example prints `record_ids` and `batch_ids`.

```bash
docker compose -f clients/docker-compose.yml --profile js up --abort-on-container-exit
```

## Test

`node --test` skips until `DIAVASI_DATA_ADDR`, `DIAVASI_CA`, and `DIAVASI_API_TOKEN` are set. With those set, it consumes `DIAVASI_TOTAL` records (default 8) from `DIAVASI_GROUP`.
