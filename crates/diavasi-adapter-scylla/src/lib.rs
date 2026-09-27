//! ScyllaDB source for Diavasi. A group reads one partition in clustering
//! order, or the whole table in token order; each read resumes after the
//! committed primary-key position. ADR 0011 and `docs/adapters/scylla.md`
//! state the contract.
//!
//! Install the factory in the server:
//!
//! ```no_run
//! use std::sync::Arc;
//! use diavasi::control::{ServeConfig, serve};
//!
//! # async fn run(config: ServeConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! serve(ServeConfig {
//!     source_factory: Some(Arc::new(diavasi_adapter_scylla::ScyllaFactory)),
//!     ..config
//! })
//! .await
//! # }
//! ```
//!
//! A connection and two groups, one per mode:
//!
//! ```json
//! {"id": "sc", "kind": "scylla", "config_json": {"host": "scylla.internal", "port": 9042, "keyspace": "shop"}, "secret": "unused"}
//! ```
//!
//! ```json
//! {"table": "orders_by_customer", "partition": {"customer_id": 42}}
//! {"table": "orders", "scan": "token", "columns": ["id", "total"]}
//! ```
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

mod catalog;
/// Connecting to ScyllaDB, from a stored connection or a URL.
pub mod connect;
mod reader;
mod spec;

use std::sync::Arc;

use diavasi::runtime::{SourceFactory, SourceOpen};
use futures::future::BoxFuture;

use reader::ScyllaSource;

/// The connection `kind` this adapter reads: `scylla`.
pub const NAME: &str = "scylla";

/// Opens one Scylla session per running group.
#[derive(Debug, Default)]
pub struct ScyllaFactory;

impl SourceFactory for ScyllaFactory {
    fn kind(&self) -> &str {
        NAME
    }

    fn open(
        &self,
        request: SourceOpen,
    ) -> BoxFuture<'static, Result<Box<dyn diavasi::core::RecordSource>, String>> {
        Box::pin(async move {
            let source = ScyllaSource::open(request).await?;
            Ok(Box::new(source) as Box<dyn diavasi::core::RecordSource>)
        })
    }

    fn validate(&self, request: SourceOpen) -> BoxFuture<'static, Result<(), String>> {
        Box::pin(async move {
            let _ = ScyllaSource::open(request).await?;
            Ok(())
        })
    }
}

/// Install helper used by `diavasi serve`.
pub fn factory() -> Arc<ScyllaFactory> {
    Arc::new(ScyllaFactory)
}

#[cfg(test)]
mod tests;
