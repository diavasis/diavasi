# C# client

`Diavasi.Data.DiavasiClient` is a thin client of `diavasi.data.v1`, built with `Grpc.Net.Client`. `ConsumeAsync` opens a TLS stream, sends the bearer token, Hello version 1, then JoinGroup, and acks each batch. The client stores no cursor and does not dedupe on `record_id`. A dropped stream is how unacked batches return. Reconnect with the same consumer id and the server replays them.

A NuGet publish is reserved and has not happened. The project targets `net8.0`.

## Install

Reference `clients/csharp/Diavasi.Data/Diavasi.Data.csproj` from the application. The project generates C# from `crates/diavasi/proto/data.proto`. On linux/arm64, pass `-p:Protobuf_ProtocFullPath` to a system `protoc`. The bundled Grpc.Tools `protoc` exits 139 there. Debian's `protobuf-compiler` is the one the Compose image uses.

## Library

```csharp
var report = await DiavasiClient.ConsumeAsync(new Options
{
    Addr = "127.0.0.1:7710",
    Ca = "/tmp/diavasi-sdk/dataplane-ca.crt",
    Token = "sdk-demo",
    GroupId = "demo",
    ConsumerId = "csharp",
    ExpectRecords = 8,
});
```

`MaxInFlight` defaults to 1. `HaltAfterAcks` closes after that many acks and does not send Leave. `ExpectRecords` sends Leave once that many records are acked.

`ProtocolException` carries codes 1 through 8: bad version, bad state, unknown ack, duplicate ack, group not running, unsupported, internal, heartbeat timeout. `CallException` is a gRPC status. A bad token is `UNAUTHENTICATED` with message `unauthorized`. A group that is not running is protocol code 5.

## Example

```bash
dotnet run --project clients/csharp/Diavasi.Data -- \
  --addr 127.0.0.1:7710 --ca /tmp/diavasi-sdk/dataplane-ca.crt \
  --token sdk-demo --group demo --consumer csharp --total 8
```

Flags: `--addr`, `--ca`, `--token`, `--group`, `--consumer`, `--total`, `--max-in-flight` (default 1), `--halt-after`. The last occurrence of a flag wins. The program prints `record_ids` and `batch_ids`.

```bash
docker compose -f clients/docker-compose.yml --profile csharp up --abort-on-container-exit
```

## Test

`dotnet test clients/csharp/Diavasi.Data.Tests` returns without asserting until `DIAVASI_DATA_ADDR`, `DIAVASI_CA`, and `DIAVASI_API_TOKEN` are set. With those set, it consumes `DIAVASI_TOTAL` records (default 8) from `DIAVASI_GROUP`.
