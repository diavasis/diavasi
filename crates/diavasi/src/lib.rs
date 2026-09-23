//! Diavasi server library.
//!
//! Stage 0: transport-neutral benchmark protocol and bake-off harness.
//! Stage 1: in-memory consumer-group domain (`core`).
//! Stage 2: durable metadata store (`store`).
//! Stage 3: supervised per-group Tokio runtime (`runtime`).
//! Stage 4: HTTP control plane (`control`).
//! Stage 5: TLS gRPC data plane (`dataplane`).

pub mod control;
pub mod core;
pub mod dataplane;
pub mod protocol;
pub mod runtime;
pub mod store;

#[cfg(feature = "transport-bench")]
pub mod transport_bench;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
