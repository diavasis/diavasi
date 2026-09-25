# Rust client

`diavasi-client` is a thin client of `diavasi.data.v1`. `run` opens a TLS stream, sends the bearer token, Hello version 1, then JoinGroup, and acks each batch. The crate stores no cursor and does not dedupe on `record_id`. A dropped stream is how unacked batches return. Reconnect with the same consumer id and the server replays them.

This crate is not a dependency of the server. The crates.io name `diavasi-client` is reserved and unpublished.

## Install

From a crate in this repo:

```toml
[dependencies]
diavasi-client = { path = "clients/rust" }
```

## Library

```rust
let pem = std::fs::read("/tmp/diavasi-sdk/dataplane-ca.crt")?;
let mut opts = diavasi_client::Options::new(
    "127.0.0.1:7710",
    pem,
    "sdk-demo",
    "demo",
    "rust",
);
opts.expect_records = Some(8);
let report = diavasi_client::run(opts)?;
println!("{:?}", report.batch_ids);
```

`max_in_flight` defaults to 1. `halt_after_acks` closes after that many acks and does not send Leave. `expect_records` sends Leave once that many records are acked. `run_async` is the same call on the caller's runtime.

`ClientError::Protocol` carries codes 1 through 8: bad version, bad state, unknown ack, duplicate ack, group not running, unsupported, internal, heartbeat timeout. `ClientError::Call` is a gRPC status. A bad token is `UNAUTHENTICATED` with message `unauthorized`. A group that is not running is protocol code 5.

## Example

From `clients/rust`:

```bash
cargo run --bin diavasi-consume -- \
  --addr 127.0.0.1:7710 --ca /tmp/diavasi-sdk/dataplane-ca.crt \
  --token sdk-demo --group demo --consumer rust --total 8
```

Flags: `--addr`, `--ca`, `--token`, `--group`, `--consumer`, `--total`, `--max-in-flight` (default 1), `--halt-after`. The last occurrence of a flag wins. The binary prints `record_ids` and `batch_ids`.

```bash
docker compose -f clients/docker-compose.yml --profile rust up --abort-on-container-exit
```

## Test

`cargo test --manifest-path clients/rust/Cargo.toml` skips the server cases until `DIAVASI_DATA_ADDR`, `DIAVASI_CA`, and `DIAVASI_API_TOKEN` are set. With those set, it consumes `DIAVASI_TOTAL` records (default 8) from `DIAVASI_GROUP`.
