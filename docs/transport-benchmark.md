# Diavasi Stage 0 transport benchmark

## Status

**Stage 0 bake-off is complete.** The production transport was frozen in Stage 5 as gRPC over HTTP/2 with TLS ([ADR 0006](adr/0006-data-plane-transport.md)). Python and Elixir TCP clients ran in the bake-off; Stage 5 adds gRPC compatibility clients.

## Methodology

Shared logical protocol (`JoinGroup`, `Joined`, `RecordBatch`, `Ack`, `FlowControl`, `Heartbeat`, `Error`) encoded as protobuf (`crates/diavasi/proto/bench.proto`).

| Transport | Framing | Notes |
| --- | --- | --- |
| TCP | length-prefixed protobuf over Tokio TCP | cleartext for Stage 0 |
| gRPC | same messages over Tonic bidirectional stream | HTTP/2 |
| QUIC | length-prefixed protobuf over Quinn bi-stream | self-signed TLS, insecure verifier in bench client |
| WebTransport | length-prefixed protobuf over `wtransport` bi-stream | HTTP/3 WebTransport; cert hash pin in both-mode; draft spec |

Synthetic producer generates records faster than transports. Metrics: records/sec, MiB/sec, delivery latency percentiles. Results append as JSONL under `docs/bench/`.

### WebTransport caveats

`wtransport` documents that WebTransport is still a draft and the library is not production-ready. Included as a Rust candidate anyway. Multi-language WebTransport clients remain weak; that is scored under client maturity.

## How to run (Rust)

```bash
cargo run -p diavasi --release --bin diavasi-transport-bench --features transport-bench -- \
  --transport tcp --smoke --output docs/bench/results.jsonl

cargo run -p diavasi --release --bin diavasi-transport-bench --features transport-bench -- \
  --transport grpc --smoke --listen 127.0.0.1:9801 --connect 127.0.0.1:9801 \
  --output docs/bench/results.jsonl

cargo run -p diavasi --release --bin diavasi-transport-bench --features transport-bench -- \
  --transport quic --smoke --listen 127.0.0.1:9802 --connect 127.0.0.1:9802 \
  --output docs/bench/results.jsonl

cargo run -p diavasi --release --bin diavasi-transport-bench --features transport-bench -- \
  --transport webtransport --smoke --listen 127.0.0.1:9803 --connect 127.0.0.1:9803 \
  --output docs/bench/results.jsonl
```

## Client language setup (mise)

```bash
mise install
git clone https://github.com/diavasis/diavasi-python.git
mise exec -- python -m venv diavasi-python/.venv
mise exec -- diavasi-python/.venv/bin/pip install -r diavasi-python/requirements.txt
cd diavasi-python && PYTHONPATH=. .venv/bin/python -m diavasi_bench.gen_proto

git clone https://github.com/diavasis/diavasi-elixir.git
cd diavasi-elixir && mise exec -- mix deps.get
```

## Results (release smoke, 200 x 64-byte records, batch 8)

Machine-readable: [docs/bench/results.jsonl](bench/results.jsonl).

| Transport | Client | records/sec (approx) | delivery p50 us | Notes |
| --- | --- | --- | --- | --- |
| TCP | Rust | ~117k | ~75 | release, both-mode |
| gRPC | Rust | ~96k | ~64 | release, both-mode |
| QUIC | Rust | ~98k | ~70 | release, both-mode, TLS |
| WebTransport | Rust | ~99k | ~74 | release, both-mode, draft |
| TCP | Python | ~166k | n/a | release server; client does not record delivery hist |
| TCP | Elixir | ~44k | n/a | release server; Mix client |

Throughput among Rust transports is close in this smoke size. Absolute numbers are localhost-only and not a production ranking by themselves.

## Qualitative notes

| Axis | TCP | gRPC | QUIC | WebTransport |
| --- | --- | --- | --- | --- |
| Implementation complexity | low | medium | medium-high | high |
| Client maturity (multi-lang) | high (Python+Elixir proven) | high | uneven | low outside browsers/Rust |
| TLS story | DIY | mature | built-in | built-in (HTTP/3) |
| Debugging | easy | good tooling | harder | hardest |
| Dependency weight | low | medium | medium | medium-high |
| Spec maturity | stable | stable | stable | draft |

## Recommendation (frozen in Stage 5)

**gRPC over HTTP/2 with TLS is the production data plane.** See [ADR 0006](adr/0006-data-plane-transport.md).

Raw TCP won the localhost smoke on throughput, but Diavasi must be proxy- and firewall-friendly and must use TLS for remote data planes. Those constraints outweigh the smoke rps gap.

| Requirement | TCP | gRPC | QUIC | WebTransport |
| --- | --- | --- | --- | --- |
| TLS | DIY | mature (HTTPS) | built-in | built-in |
| Firewall / middlebox friendliness | poor on custom ports | strong (HTTP/2 on 443) | mixed (UDP often blocked) | mixed (UDP / draft) |
| HTTP proxy friendliness | poor | strong | weak | weak |
| Multi-lang clients | high | high | uneven | low |
| Stage 0 release smoke rps | ~117k | ~96k | ~98k | ~99k |

Reasons:

1. gRPC rides HTTP/2 with a mature TLS story and is routinely allowed through corporate proxies and firewalls as ordinary HTTPS.
2. Throughput is close enough to TCP on the Stage 0 smoke that operational fit dominates.
3. Client ecosystems (Rust, Python, Elixir, and the later SDK set) are strongest for gRPC after TCP.
4. QUIC / WebTransport keep built-in TLS but are UDP-based and often less middlebox-friendly; WebTransport remains draft / `wtransport`-not-production-ready.

TCP remains useful for local/dev cleartext and as a regression harness, not as the production data plane.

**Frozen:** gRPC over HTTP/2 with TLS.

## Acceptance checklist

- [x] TCP / gRPC / QUIC / WebTransport smoke runs succeed (Rust, release)
- [x] Python and Elixir TCP clients run against Rust server
- [x] Results stored as JSONL
- [x] WebTransport included with documented caveats
- [x] CI workflow present (includes WebTransport smoke)
- [x] Provisional recommendation written
- [x] Transport frozen: gRPC over HTTP/2 with TLS (Stage 5, [ADR 0006](adr/0006-data-plane-transport.md))
