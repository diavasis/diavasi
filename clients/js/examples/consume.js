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
  const halt = Number(arg("--halt-after") || "0");
  const report = await consume({
    addr: arg("--addr"),
    ca: arg("--ca"),
    token: arg("--token"),
    groupId: arg("--group"),
    consumerId: arg("--consumer") || "js",
    maxInFlight: Number(arg("--max-in-flight") || "1"),
    haltAfterAcks: halt || undefined,
    expectRecords: Number(arg("--total")),
  });
  console.log("record_ids " + report.recordIds.join(" "));
  console.log("batch_ids " + report.batchIds.join(" "));
  console.log(`js consumed ${report.recordIds.length} records in ${report.batchIds.length} batches`);
}

main().catch((err) => {
  console.error(String(err.message || err));
  if (err instanceof ProtocolError && err.code >= 1 && err.code <= 8) {
    process.exit(err.code);
  }
  if (err instanceof CallError) {
    process.exit(1);
  }
  process.exit(1);
});
