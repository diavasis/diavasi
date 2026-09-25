"use strict";

const fs = require("fs");
const path = require("path");
const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");

class ProtocolError extends Error {
  constructor(code, message) {
    super(`protocol error ${code}: ${message}`);
    this.code = code;
    this.name = "ProtocolError";
  }
}

class CallError extends Error {
  constructor(status, message) {
    super(`grpc ${status}: ${message}`);
    this.status = status;
    this.name = "CallError";
  }
}

function asNumber(value) {
  if (typeof value === "number") {
    return value;
  }
  return Number(value);
}

function loadClient(protoPath) {
  const definition = protoLoader.loadSync(protoPath, {
    keepCase: true,
    longs: String,
    defaults: false,
    oneofs: true,
  });
  const root = grpc.loadPackageDefinition(definition);
  return root.diavasi.data.v1.DataPlane;
}

function consume(options) {
  const protoPath =
    options.protoPath ||
    process.env.DIAVASI_PROTO ||
    path.join(__dirname, "../../crates/diavasi/proto/data.proto");
  const DataPlane = loadClient(protoPath);
  const ca = fs.readFileSync(options.ca);
  const creds = grpc.credentials.createSsl(ca);
  const client = new DataPlane(options.addr, creds, {
    "grpc.ssl_target_name_override": "localhost",
  });
  const meta = new grpc.Metadata();
  meta.add("authorization", `Bearer ${options.token}`);
  const call = client.Consume(meta);
  const maxInFlight = options.maxInFlight || 1;
  const report = { recordIds: [], batchIds: [] };
  let sentFlow = false;
  let dropping = false;

  return new Promise((resolve, reject) => {
    let settled = false;
    const finish = (fn) => {
      if (settled) {
        return;
      }
      settled = true;
      fn();
    };
    const fail = (err) => {
      finish(() => {
        call.cancel();
        reject(err);
      });
    };
    call.on("error", (err) => {
      if (settled) {
        return;
      }
      if (options.expectRecords && report.recordIds.length >= options.expectRecords) {
        finish(() => resolve(report));
        return;
      }
      const status = err.code === grpc.status.UNAUTHENTICATED ? "UNAUTHENTICATED" : String(err.code);
      fail(new CallError(status, err.details || err.message));
    });
    call.on("end", () => {
      if (options.expectRecords && report.recordIds.length < options.expectRecords && !options.haltAfterAcks) {
        fail(new CallError("UNAVAILABLE", "stream ended early"));
        return;
      }
      finish(() => resolve(report));
    });
    call.on("data", (env) => {
      if (dropping) {
        finish(() => resolve(report));
        call.cancel();
        return;
      }
      switch (env.body) {
        case "hello_ack":
          call.write({
            version: 1,
            join_group: { group_id: options.groupId, consumer_id: options.consumerId },
          });
          return;
        case "joined":
          if (!sentFlow) {
            sentFlow = true;
            call.write({ version: 1, flow_control: { max_in_flight: maxInFlight } });
          }
          return;
        case "record_batch": {
          const batch = env.record_batch;
          const records = batch.records || [];
          for (const record of records) {
            report.recordIds.push(asNumber(record.record_id));
          }
          const batchId = asNumber(batch.batch_id);
          call.write({ version: 1, ack: { batch_id: batchId } });
          report.batchIds.push(batchId);
          if (options.haltAfterAcks && report.batchIds.length >= options.haltAfterAcks) {
            dropping = true;
            return;
          }
          if (options.expectRecords && report.recordIds.length >= options.expectRecords) {
            call.write({ version: 1, leave: {} });
            call.end();
          }
          return;
        }
        case "heartbeat":
          call.write({ version: 1, heartbeat: {} });
          return;
        case "error":
          fail(new ProtocolError(asNumber(env.error.code), env.error.message || ""));
          return;
        default:
          return;
      }
    });
    call.write({ version: 1, hello: { protocol_version: 1 } });
  });
}

module.exports = { ProtocolError, CallError, consume };
