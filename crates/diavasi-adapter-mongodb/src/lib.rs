//! MongoDB source for Diavasi. A group reads one collection with `find` in a
//! declared sort; each read resumes after the committed cursor. ADR 0009 and
//! `docs/adapters/mongodb.md` state the contract.
//!
//! Install the factory in the server:
//!
//! ```no_run
//! use std::sync::Arc;
//! use diavasi::control::{ServeConfig, serve};
//!
//! # async fn run(config: ServeConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! serve(ServeConfig {
//!     source_factory: Some(Arc::new(diavasi_adapter_mongodb::MongoFactory)),
//!     ..config
//! })
//! .await?;
//! # Ok(())
//! # }
//! ```
//!
//! A connection and a group that reads it:
//!
//! ```json
//! {"id": "mg", "kind": "mongodb",
//!  "config_json": {"host": "db.internal", "port": 27017, "database": "app", "user": "diavasi"},
//!  "secret": "s3cret"}
//! ```
//!
//! ```json
//! {"collection": "orders",
//!  "order_by": [{"field": "created", "type": "date", "direction": "asc"},
//!               {"field": "_id", "type": "objectId", "direction": "asc"}],
//!  "fields": ["customer", "total"],
//!  "filter": {"status": {"$ne": "draft"}}}
//! ```
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

mod catalog;
/// Connecting to MongoDB, from a stored connection or a URL.
pub mod connect;
mod reader;
mod spec;

use std::sync::Arc;

use diavasi::runtime::{SourceFactory, SourceOpen};
use futures::future::BoxFuture;

use reader::MongoSource;

/// The connection `kind` this adapter reads: `mongodb`.
pub const NAME: &str = "mongodb";

/// Opens one MongoDB client per running group.
#[derive(Debug, Default)]
pub struct MongoFactory;

impl SourceFactory for MongoFactory {
    fn kind(&self) -> &str {
        NAME
    }

    fn open(
        &self,
        request: SourceOpen,
    ) -> BoxFuture<'static, Result<Box<dyn diavasi::core::RecordSource>, String>> {
        Box::pin(async move {
            let source = MongoSource::open(request).await?;
            Ok(Box::new(source) as Box<dyn diavasi::core::RecordSource>)
        })
    }

    fn validate(&self, request: SourceOpen) -> BoxFuture<'static, Result<(), String>> {
        Box::pin(async move {
            let _ = MongoSource::open(request).await?;
            Ok(())
        })
    }
}

/// Install helper used by `diavasi serve`.
pub fn factory() -> Arc<MongoFactory> {
    Arc::new(MongoFactory)
}

#[cfg(test)]
mod tests;
