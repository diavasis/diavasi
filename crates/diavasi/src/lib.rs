//! Diavasi server library.
//!
//! Stage 0 exposes the transport-neutral benchmark protocol and bake-off harness.
//! Later stages add core domain, durable store, runtime, control plane, and data plane
//! as modules in this crate.

pub mod protocol;

#[cfg(feature = "transport-bench")]
pub mod transport_bench;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
