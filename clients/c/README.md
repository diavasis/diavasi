# C client

One function, `diavasi_consume`, on the gRPC C stack. Frames are the `diavasi.data.v1` protobuf encoding.

```bash
make
./diavasi_consume --addr 127.0.0.1:7710 --ca /tmp/diavasi-sdk/dataplane-ca.crt \
  --token sdk-demo --group demo --consumer c --total 8
```

`make test-proto` checks the frame codec and does not need a server. `make test` also builds the gRPC program when `pkg-config` can see `grpc`, and that program skips until `DIAVASI_DATA_ADDR`, `DIAVASI_CA`, and `DIAVASI_API_TOKEN` are set.
