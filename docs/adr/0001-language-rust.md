# ADR 0001: Production language is Rust

## Status

Accepted in v0.0.0. Revisit if Rust supervision complexity materially harms correctness.

## Decision

Implement the Diavasi server in Rust. Preserve an explicit ownership and supervision style (Tokio task trees per consumer group). Elixir is a first-class client SDK and interoperability benchmark, not the server language for the initial path.

## Consequences

- Stage 0 transport bake-off is native to the server language.
- Single static binary is operationally attractive.
- Supervision must be designed explicitly rather than inherited from OTP.
