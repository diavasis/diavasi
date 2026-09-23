# ADR 0006: Data-plane transport

## Status

Accepted. Frozen in Stage 5.

## Context

Stage 0 compared TCP, gRPC, QUIC, and WebTransport on a synthetic smoke workload. TCP was slightly faster on localhost. The production data plane also has to survive middleboxes, carry TLS, and be usable from Rust, Python, and Elixir.

## Decision

The production data plane is **gRPC over HTTP/2 with TLS**.

- One bidirectional RPC: `diavasi.data.v1.DataPlane/Consume`.
- TLS is required. `diavasi serve` generates a local CA and leaf certificate when `--tls-cert` / `--tls-key` are omitted, and writes `dataplane-ca.crt` next to the leaf.
- The Stage 0 length-prefixed TCP harness stays a benchmark only.

QUIC and WebTransport stay out. They keep TLS but use UDP, which is often blocked, and WebTransport is still a draft.

## Consequences

- Clients trust `dataplane-ca.crt` (or an operator-supplied CA).
- Control plane remains loopback HTTP with the same bearer token. The data plane is a separate bind address (`--data-bind`, default `127.0.0.1:7710`).
- Protocol opcodes are specified in [ADR 0007](0007-protocol-v1.md).
