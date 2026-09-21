# Stage 0 review

## What changed

- Rust workspace with fixed crates: `diavasi`, `diavasi-cli`, four adapter stubs
- Transport-neutral protobuf protocol under `diavasi::protocol`
- Bake-off: TCP, gRPC, QUIC, WebTransport (`wtransport`)
- Rust / Python / Elixir TCP clients; results in `docs/bench/results.jsonl`
- Docs: architecture, transport-benchmark, ADR 0001
- CI workflow including all four transport smokes
- `deny.toml` updated for cargo-deny 0.20 schema

## Tests / commands

- `cargo fmt --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all --all-features`
- `cargo deny check`
- Release smoke for all four transports
- Python and Elixir TCP clients against release TCP server

## Benchmark summary (release smoke)

Rust: TCP ~117k, gRPC ~96k, QUIC ~98k, WebTransport ~99k rps.  
TCP clients: Python ~166k, Elixir ~44k rps (localhost smoke).

## Provisional recommendation

**gRPC (HTTP/2 + TLS)** for the remote data plane.

TCP was fastest on localhost smoke, but proxy/firewall resilience and mandatory TLS outweigh that gap. QUIC/WebTransport keep TLS but are weaker through middleboxes (UDP). TCP stays for local/dev and harness use.

Proposed freeze (awaiting approval): **gRPC over HTTP/2 with TLS**.

## Remaining risks

- Smoke workload is tiny; larger matrix may reorder results
- ACK RTT histograms not fully instrumented on the client path
- WebTransport multi-lang story remains weak
- Server producer is shared per process (one client exhausts the budget)

## Deviations

- Crate layout uses modules inside `diavasi` (per revised plan), not separate protocol/bench crates
- WebTransport included despite draft status (user request)
- Other `clients/*` languages deferred to Stage 11 (smoke-if-cheap not expanded)

## Acceptance

Stage 0 acceptance criteria met except transport freeze, which requires explicit approval.

**Stop here.** Approve TCP (or another transport) before Stage 5; Stage 1 core domain can proceed independently of the freeze if desired.
