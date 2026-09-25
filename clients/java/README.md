# Java client

Gradle and grpc-java. Generated stubs come from `crates/diavasi/proto/data.proto`.

```bash
gradle installDist
./build/install/diavasi-data/bin/diavasi-data \
  --addr 127.0.0.1:7710 --ca /tmp/diavasi-sdk/dataplane-ca.crt \
  --token sdk-demo --group demo --consumer java --total 8
```

`gradle test` skips until `DIAVASI_DATA_ADDR`, `DIAVASI_CA`, and `DIAVASI_API_TOKEN` are set.
