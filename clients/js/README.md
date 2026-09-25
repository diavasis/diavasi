# JavaScript client

`@grpc/grpc-js` plus TypeScript types in `index.d.ts`.

```bash
npm install
node examples/consume.js --addr 127.0.0.1:7710 --ca /tmp/diavasi-sdk/dataplane-ca.crt \
  --token sdk-demo --group demo --consumer js --total 8
```

`node --test` skips until `DIAVASI_DATA_ADDR`, `DIAVASI_CA`, and `DIAVASI_API_TOKEN` are set.
