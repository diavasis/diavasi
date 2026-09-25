# diavasi-client

Rust client for `diavasi.data.v1`. It is not a dependency of the server crate.

```bash
cargo run --bin diavasi-consume -- \
  --addr 127.0.0.1:7710 --ca /tmp/diavasi-sdk/dataplane-ca.crt \
  --token sdk-demo --group demo --consumer rust --total 8
```

`cargo test` skips the server cases until `DIAVASI_DATA_ADDR`, `DIAVASI_CA`, and `DIAVASI_API_TOKEN` are set.
