# C# client

`Grpc.Net.Client` against `diavasi.data.v1`.

```bash
dotnet run --project Diavasi.Data -- \
  --addr 127.0.0.1:7710 --ca /tmp/diavasi-sdk/dataplane-ca.crt \
  --token sdk-demo --group demo --consumer csharp --total 8
```

`dotnet test Diavasi.Data.Tests` skips until `DIAVASI_DATA_ADDR`, `DIAVASI_CA`, and `DIAVASI_API_TOKEN` are set.
