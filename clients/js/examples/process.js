"use strict";

const { CallError, ProtocolError, consume } = require("../index");

function arg(name) {
  let value;
  for (let i = 0; i < process.argv.length; i += 1) {
    if (process.argv[i] === name) {
      value = process.argv[i + 1];
    }
  }
  return value;
}

async function main() {
  const report = await consume({
    addr: arg("--addr") || "127.0.0.1:7710",
    ca: arg("--ca") || "/tmp/diavasi-sdk/dataplane-ca.crt",
    token: arg("--token") || "sdk-demo",
    groupId: arg("--group") || "demo",
    consumerId: arg("--consumer") || "js",
    maxInFlight: Number(arg("--max-in-flight") || "1"),
    expectRecords: Number(arg("--total") || "8"),
    onBatch(batch) {
      for (const record of batch.records) {
        console.log(
          `batch ${batch.batchId} record ${record.recordId} (${record.payload.length} bytes)`,
        );
      }
    },
  });
  console.log("record_ids " + report.recordIds.join(" "));
  console.log("batch_ids " + report.batchIds.join(" "));
}

main().catch((err) => {
  if (err instanceof ProtocolError) {
    console.error(`protocol ${err.code}: ${err.message}`);
    process.exit(err.code >= 1 && err.code <= 8 ? err.code : 1);
  }
  if (err instanceof CallError) {
    console.error(`grpc ${err.status}: ${err.message}`);
    process.exit(1);
  }
  console.error(err && err.message ? err.message : err);
  process.exit(1);
});
