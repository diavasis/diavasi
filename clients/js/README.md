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
const { consume } = require("@diavasi/data");

const report = await consume({
  addr: "127.0.0.1:7710",
  ca: "/tmp/diavasi-sdk/dataplane-ca.crt",
  token: "sdk-demo",
  groupId: "demo",
  consumerId: "js",
  expectRecords: 8,
});
console.log(report.batchIds);
```

`maxInFlight` defaults to 1. `haltAfterAcks` closes after that many acks and does not send Leave. `expectRecords` sends Leave once that many records are acked. `protoPath` overrides the path to `data.proto` (the default walks to `crates/diavasi/proto/data.proto`; set `DIAVASI_PROTO` in the Compose image).

`ProtocolError` carries codes 1 through 8: bad version, bad state, unknown ack, duplicate ack, group not running, unsupported, internal, heartbeat timeout. `CallError` is a gRPC status. A bad token is `UNAUTHENTICATED` with message `unauthorized`. A group that is not running is protocol code 5.

## Example

```bash
cd clients/js
npm install
node examples/consume.js --addr 127.0.0.1:7710 --ca /tmp/diavasi-sdk/dataplane-ca.crt \
  --token sdk-demo --group demo --consumer js --total 8
```

Flags: `--addr`, `--ca`, `--token`, `--group`, `--consumer`, `--total`, `--max-in-flight` (default 1), `--halt-after`. The last occurrence of a flag wins. The example prints `record_ids` and `batch_ids`.

```bash
docker compose -f clients/docker-compose.yml --profile js up --abort-on-container-exit
```

## Test

`node --test` skips until `DIAVASI_DATA_ADDR`, `DIAVASI_CA`, and `DIAVASI_API_TOKEN` are set. With those set, it consumes `DIAVASI_TOTAL` records (default 8) from `DIAVASI_GROUP`.
