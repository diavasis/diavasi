"""Minimal Diavasi Stage 0 TCP bench client (length-prefixed protobuf)."""

from __future__ import annotations

import argparse
import json
import socket
import struct
import sys
import time
from pathlib import Path

# Prefer generated stubs when present; otherwise fail with install hint.
try:
    from diavasi_bench import bench_pb2
except ImportError:
    sys.stderr.write(
        "missing generated protobuf stubs; run: python -m diavasi_bench.gen_proto\n"
    )
    raise


def encode(env: bench_pb2.Envelope) -> bytes:
    payload = env.SerializeToString()
    return struct.pack(">I", len(payload)) + payload


def read_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("socket closed")
        buf.extend(chunk)
    return bytes(buf)


def decode(sock: socket.socket) -> bench_pb2.Envelope:
    (length,) = struct.unpack(">I", read_exact(sock, 4))
    payload = read_exact(sock, length)
    env = bench_pb2.Envelope()
    env.ParseFromString(payload)
    return env


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--connect", default="127.0.0.1:9800")
    p.add_argument("--group-id", default="bench")
    p.add_argument("--total-records", type=int, default=200)
    p.add_argument("--max-in-flight", type=int, default=2)
    p.add_argument("--ack-delay-ms", type=int, default=0)
    p.add_argument("--output", type=Path, default=None)
    args = p.parse_args()

    host, port_s = args.connect.rsplit(":", 1)
    port = int(port_s)

    sock = socket.create_connection((host, port), timeout=30)
    sock.settimeout(30)

    flow = bench_pb2.Envelope(version=1)
    flow.flow_control.max_in_flight = args.max_in_flight
    sock.sendall(encode(flow))

    join = bench_pb2.Envelope(version=1)
    join.join_group.group_id = args.group_id
    join.join_group.consumer_id = f"python-{int(time.time())}"
    sock.sendall(encode(join))

    records = 0
    batches = 0
    bytes_total = 0
    t0 = time.perf_counter()
    joined = False

    while records < args.total_records:
        env = decode(sock)
        which = env.WhichOneof("body")
        if which == "joined":
            joined = True
        elif which == "record_batch":
            batch = env.record_batch
            records += len(batch.records)
            batches += 1
            bytes_total += sum(len(r.payload) for r in batch.records)
            if args.ack_delay_ms:
                time.sleep(args.ack_delay_ms / 1000.0)
            ack = bench_pb2.Envelope(version=1)
            ack.ack.batch_id = batch.batch_id
            sock.sendall(encode(ack))
        elif which == "error":
            raise RuntimeError(env.error.message)

    elapsed = max(time.perf_counter() - t0, 1e-9)
    result = {
        "transport": "tcp",
        "client_lang": "python",
        "consumers": 1,
        "total_records": args.total_records,
        "metrics": {
            "elapsed_secs": elapsed,
            "records": records,
            "bytes": bytes_total,
            "batches": batches,
            "records_per_sec": records / elapsed,
            "mib_per_sec": (bytes_total / (1024 * 1024)) / elapsed,
        },
        "notes": ["transport=tcp", f"joined={joined}"],
    }
    print(json.dumps(result, indent=2))
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("a", encoding="utf-8") as f:
            f.write(json.dumps(result) + "\n")
    sock.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
