"""Thin client for the diavasi.data.v1 Consume stream.

The caller acks by batch_id. record_id is not an identity: it is 0 for some
sources. This module does not store a cursor.
"""

from __future__ import annotations

import queue
import threading
from dataclasses import dataclass, field

import grpc

from diavasi_data import data_pb2, data_pb2_grpc


class ProtocolError(Exception):
    def __init__(self, code: int, message: str) -> None:
        super().__init__(f"protocol error {code}: {message}")
        self.code = code
        self.message = message


class CallError(Exception):
    """gRPC status from the data plane, including a bad token."""

    def __init__(self, status: str, message: str) -> None:
        super().__init__(f"grpc {status}: {message}")
        self.status = status
        self.message = message


@dataclass
class Record:
    record_id: int
    payload: bytes


@dataclass
class Batch:
    batch_id: int
    records: list[Record]


def _envelope() -> data_pb2.Envelope:
    return data_pb2.Envelope(version=1)


@dataclass
class Session:
    addr: str
    ca: str
    token: str
    group_id: str
    consumer_id: str
    max_in_flight: int = 1
    _outbound: queue.Queue = field(default_factory=queue.Queue, init=False, repr=False)
    _inbound: queue.Queue = field(default_factory=queue.Queue, init=False, repr=False)
    _channel: grpc.Channel | None = field(default=None, init=False, repr=False)
    _thread: threading.Thread | None = field(default=None, init=False, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)

    def connect(self) -> None:
        with open(self.ca, "rb") as handle:
            pem = handle.read()
        creds = grpc.ssl_channel_credentials(root_certificates=pem)
        self._channel = grpc.secure_channel(
            self.addr,
            creds,
            options=(("grpc.ssl_target_name_override", "localhost"),),
        )
        stub = data_pb2_grpc.DataPlaneStub(self._channel)
        hello = _envelope()
        hello.hello.protocol_version = 1
        self._outbound.put((hello, None))
        metadata = (("authorization", f"Bearer {self.token}"),)

        def requests():
            while True:
                item = self._outbound.get()
                if item is None:
                    return
                env, done = item
                yield env
                if done is not None:
                    done.set()

        def read() -> None:
            sent_flow = False
            try:
                for env in stub.Consume(requests(), metadata=metadata):
                    which = env.WhichOneof("body")
                    if which == "hello_ack":
                        join = _envelope()
                        join.join_group.group_id = self.group_id
                        join.join_group.consumer_id = self.consumer_id
                        self._outbound.put((join, None))
                    elif which == "joined":
                        if not sent_flow:
                            sent_flow = True
                            flow = _envelope()
                            flow.flow_control.max_in_flight = self.max_in_flight
                            self._outbound.put((flow, None))
                            self._inbound.put(("joined", None))
                    elif which == "record_batch":
                        records = [
                            Record(record_id=record.record_id, payload=bytes(record.payload))
                            for record in env.record_batch.records
                        ]
                        self._inbound.put(
                            ("batch", Batch(batch_id=env.record_batch.batch_id, records=records))
                        )
                    elif which == "heartbeat":
                        beat = _envelope()
                        beat.heartbeat.SetInParent()
                        self._outbound.put((beat, None))
                    elif which == "error":
                        self._inbound.put(("error", (env.error.code, env.error.message)))
                        return
                    else:
                        self._inbound.put(("error", (2, f"unexpected frame {which}")))
                        return
                self._inbound.put(("done", None))
            except grpc.RpcError as exc:
                self._inbound.put(("grpc", (exc.code().name, exc.details() or "")))

        self._thread = threading.Thread(target=read, name="diavasi-data", daemon=True)
        self._thread.start()
        kind, payload = self._inbound.get(timeout=30)
        self._raise(kind, payload)

    def next_batch(self, timeout: float = 60) -> Batch | None:
        kind, payload = self._inbound.get(timeout=timeout)
        if kind == "batch":
            return payload
        if kind == "done":
            return None
        self._raise(kind, payload)
        return None

    def ack(self, batch_id: int) -> None:
        env = _envelope()
        env.ack.batch_id = batch_id
        self._send(env)

    def leave(self) -> None:
        env = _envelope()
        env.leave.SetInParent()
        self._send(env)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._outbound.put(None)
        if self._thread is not None:
            self._thread.join(timeout=5)
        if self._channel is not None:
            self._channel.close()

    def _send(self, env: data_pb2.Envelope) -> None:
        done = threading.Event()
        self._outbound.put((env, done))
        if not done.wait(5):
            raise CallError("UNAVAILABLE", "timed out sending a frame")

    def __enter__(self) -> Session:
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    @staticmethod
    def _raise(kind: str, payload) -> None:
        if kind == "joined":
            return
        if kind == "error":
            code, message = payload
            raise ProtocolError(code, message)
        if kind == "grpc":
            status, message = payload
            raise CallError(status, message)
        raise CallError("UNKNOWN", f"session ended during handshake ({kind})")


def consume(
    addr: str,
    ca: str,
    token: str,
    group_id: str,
    consumer_id: str,
    max_in_flight: int = 1,
    halt_after_acks: int | None = None,
    expect_records: int | None = None,
):
    """Yield batches. Continuing the iterator acks the batch just yielded.

    ``halt_after_acks`` closes the stream after that many acks and does not
    send Leave. ``expect_records`` sends Leave once that many records have
    been acked.
    """

    session = Session(addr, ca, token, group_id, consumer_id, max_in_flight)
    session.connect()
    acked = 0
    seen = 0
    try:
        while True:
            batch = session.next_batch()
            if batch is None:
                break
            try:
                yield batch
            finally:
                session.ack(batch.batch_id)
                acked += 1
                seen += len(batch.records)
            if halt_after_acks is not None and acked >= halt_after_acks:
                return
            if expect_records is not None and seen >= expect_records:
                session.leave()
                return
        session.leave()
    finally:
        session.close()
