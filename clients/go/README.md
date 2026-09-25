# Go client

Module `github.com/diavasis/diavasi/clients/go`.

```bash
go run ./cmd/consume --addr 127.0.0.1:7710 --ca /tmp/diavasi-sdk/dataplane-ca.crt \
  --token sdk-demo --group demo --consumer go --total 8
```

`go test ./...` skips the server cases until `DIAVASI_DATA_ADDR`, `DIAVASI_CA`, and `DIAVASI_API_TOKEN` are set. The protobuf frame tests always run.
