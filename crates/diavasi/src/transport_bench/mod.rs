//! Stage 0 data-plane transport bake-off.
//!
//! Candidate transports share the same logical protocol and synthetic producer.

pub mod codec;
pub mod config;
pub mod grpc;
pub mod metrics;
pub mod producer;
pub mod quic;
pub mod report;
pub mod tcp;
pub mod webtransport;
pub mod workload;

pub use config::{BenchConfig, RecordSize, TransportKind};
pub use report::BenchResult;
