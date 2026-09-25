"""Diavasi data-plane client and the Stage 0 bench package sibling."""

from diavasi_data.client import Batch, CallError, ProtocolError, Record, Session, consume

__all__ = [
    "Batch",
    "CallError",
    "ProtocolError",
    "Record",
    "Session",
    "consume",
]
