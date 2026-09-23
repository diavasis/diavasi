"""Compatibility client for the Stage 5 TLS gRPC data plane."""

from __future__ import annotations

import argparse
import queue
import sys
import threading

import grpc

from diavasi_data import data_pb2, data_pb2_grpc


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--addr", required=True)
    parser.add_argument("--ca", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--group", required=True)
    parser.add_argument("--consumer", default="python")
    parser.add_argument("--total", type=int, required=True)
    parser.add_argument("--max-in-flight", type=int, default=4)
    args = parser.parse_args()

    with open(args.ca, "rb") as handle:
        creds = grpc.ssl_channel_credentials(root_certificates=handle.read())
    channel = grpc.secure_channel(
        args.addr,
        creds,
        options=(("grpc.ssl_target_name_override", "localhost"),),
    )
    stub = data_pb2_grpc.DataPlaneStub(channel)
    outbound: queue.Queue[data_pb2.Envelope | None] = queue.Queue()

    def requests():
        while True:
            item = outbound.get()
            if item is None:
                return
            yield item

    hello = data_pb2.Envelope(version=1)
    hello.hello.protocol_version = 1
    outbound.put(hello)

    seen: set[int] = set()
    acked = 0
    sent_flow = False
    metadata = (("authorization", f"Bearer {args.token}"),)
    try:
        for env in stub.Consume(requests(), metadata=metadata, timeout=30):
            which = env.WhichOneof("body")
            if which == "hello_ack":
                join = data_pb2.Envelope(version=1)
                join.join_group.group_id = args.group
                join.join_group.consumer_id = args.consumer
                outbound.put(join)
            elif which == "joined":
                if not sent_flow:
                    flow = data_pb2.Envelope(version=1)
                    flow.flow_control.max_in_flight = args.max_in_flight
                    outbound.put(flow)
                    sent_flow = True
            elif which == "record_batch":
                for record in env.record_batch.records:
                    seen.add(record.record_id)
                ack = data_pb2.Envelope(version=1)
                ack.ack.batch_id = env.record_batch.batch_id
                outbound.put(ack)
                acked += 1
                if len(seen) >= args.total:
                    leave = data_pb2.Envelope(version=1)
                    leave.leave.SetInParent()
                    outbound.put(leave)
                    outbound.put(None)
                    break
            elif which == "heartbeat":
                continue
            elif which == "error":
                sys.stderr.write(
                    f"protocol error {env.error.code}: {env.error.message}\n"
                )
                return 1
            else:
                sys.stderr.write(f"unexpected frame {which}\n")
                return 1
    finally:
        outbound.put(None)
        channel.close()

    if len(seen) != args.total or acked == 0:
        sys.stderr.write(f"incomplete consume seen={len(seen)} acked={acked}\n")
        return 1
    print(f"python consumed {len(seen)} records in {acked} batches")
    return 0


if __name__ == "__main__":
    threading.current_thread().name = "diavasi-data"
    sys.exit(main())
