//! Diavasi server library.
//!
//! Stage 0: transport-neutral benchmark protocol and bake-off harness.
//! Stage 1: in-memory consumer-group domain (`core`).

pub mod core;
pub mod protocol;

#[cfg(feature = "transport-bench")]
pub mod transport_bench;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
