# Java client

`dev.diavasi.data.DiavasiClient` is a thin client of `diavasi.data.v1`, built with grpc-java. `consume` opens a TLS stream, sends the bearer token, Hello version 1, then JoinGroup, and acks each batch. The client stores no cursor and does not dedupe on `record_id`. A dropped stream is how unacked batches return. Reconnect with the same consumer id and the server replays them.

Generated stubs come from `crates/diavasi/proto/data.proto`. A Maven Central publish is reserved and has not happened.

## Install

Build from this repo with JDK 21:

```bash
cd clients/java
gradle installDist
```

## Library

```java
var options = new DiavasiClient.Options();
options.addr = "127.0.0.1:7710";
options.ca = "/tmp/diavasi-sdk/dataplane-ca.crt";
options.token = "sdk-demo";
options.groupId = "demo";
options.consumerId = "java";
options.expectRecords = 8;
DiavasiClient.Report report = DiavasiClient.consume(options);
```

`maxInFlight` defaults to 1. `haltAfterAcks` closes after that many acks and does not send Leave. `expectRecords` sends Leave once that many records are acked.

`ProtocolException` carries codes 1 through 8: bad version, bad state, unknown ack, duplicate ack, group not running, unsupported, internal, heartbeat timeout. `CallException` is a gRPC status. A bad token is `UNAUTHENTICATED` with message `unauthorized`. A group that is not running is protocol code 5.

## Example

```bash
cd clients/java
./build/install/diavasi-data/bin/diavasi-data \
  --addr 127.0.0.1:7710 --ca /tmp/diavasi-sdk/dataplane-ca.crt \
  --token sdk-demo --group demo --consumer java --total 8
```

Flags: `--addr`, `--ca`, `--token`, `--group`, `--consumer`, `--total`, `--max-in-flight` (default 1), `--halt-after`. The last occurrence of a flag wins. The program prints `record_ids` and `batch_ids`.

```bash
docker compose -f clients/docker-compose.yml --profile java up --abort-on-container-exit
```

## Test

`gradle test` skips until `DIAVASI_DATA_ADDR`, `DIAVASI_CA`, and `DIAVASI_API_TOKEN` are set. With those set, it consumes `DIAVASI_TOTAL` records (default 8) from `DIAVASI_GROUP`.
