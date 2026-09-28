//! Diavasi server library: durable consumer groups over existing databases.
//!
//! A group reads a source in a declared total order, hands batches to
//! consumers, and stores the committed position. Delivery is at-least-once:
//! after a crash, records after the committed position are read again.
//!
//! Modules:
//!
//! - [`core`]: the group engine. Buffer, in-flight batches, contiguous
//!   commit, lifecycle. No I/O.
//! - [`store`]: redb persistence for connections, groups, and committed
//!   cursors, and sealing of connection secrets.
//! - [`runtime`]: one task per running group, and the supervisor that starts,
//!   restarts, and stops them.
//! - [`control`]: the HTTP control plane and [`control::serve`], which runs
//!   the whole server.
//! - [`dataplane`]: the TLS gRPC data plane, `diavasi.data.v1`.
//! - [`observe`]: Prometheus metrics and log setup.
//!
//! Adapters live in separate crates and implement [`core::RecordSource`] and
//! [`runtime::SourceFactory`]. The `transport-bench` feature adds the
//! transport benchmark that chose gRPC for the data plane.
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

pub mod control;
pub mod core;
pub mod dataplane;
pub mod observe;
pub mod runtime;
pub mod store;

// The transport bake-off harness is an internal benchmark, not API.
#[cfg(feature = "transport-bench")]
#[allow(missing_docs)]
pub mod bench_protocol;
#[cfg(feature = "transport-bench")]
#[allow(missing_docs)]
pub mod transport_bench;

/// This crate's version, as `/v1/status` reports it.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
