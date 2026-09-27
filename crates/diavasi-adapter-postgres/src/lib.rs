//! PostgreSQL source for Diavasi. A group reads one table in a declared total
//! order; each read is a keyset query after the committed cursor. ADR 0008
//! and `docs/adapters/postgresql.md` state the contract.
//!
//! Install the factory in the server:
//!
//! ```no_run
//! use std::sync::Arc;
//! use diavasi::control::{ServeConfig, serve};
//!
//! # async fn run(config: ServeConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! serve(ServeConfig {
//!     source_factory: Some(Arc::new(diavasi_adapter_postgres::PostgresFactory)),
//!     ..config
//! })
//! .await
//! # }
//! ```
//!
//! A connection and a group that reads it:
//!
//! ```json
//! {"id": "pg-main", "kind": "postgres",
//!  "config_json": {"host": "db.internal", "port": 5432, "dbname": "app", "user": "diavasi", "sslmode": "require"},
//!  "secret": "s3cret"}
//! ```
//!
//! ```json
//! {"table": "app.orders",
//!  "order_by": [{"column": "created_at", "type": "timestamptz"}, {"column": "id", "type": "int8"}],
//!  "payload": ["id", "customer_id", "total_cents"],
//!  "filter": "status <> 'draft'"}
//! ```
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

mod catalog;
/// Connecting to PostgreSQL, from a stored connection or a URL.
pub mod connect;
mod reader;
mod spec;

use std::sync::Arc;

use diavasi::runtime::{SourceFactory, SourceOpen};
use futures::future::BoxFuture;

use reader::PostgresSource;

/// The connection `kind` this adapter reads: `postgres`.
pub const NAME: &str = "postgres";

/// Opens one PostgreSQL connection per running group.
#[derive(Debug, Default)]
pub struct PostgresFactory;

impl SourceFactory for PostgresFactory {
    fn kind(&self) -> &str {
        NAME
    }

    fn open(
        &self,
        request: SourceOpen,
    ) -> BoxFuture<'static, Result<Box<dyn diavasi::core::RecordSource>, String>> {
        Box::pin(async move {
            let source = PostgresSource::open(request).await?;
            Ok(Box::new(source) as Box<dyn diavasi::core::RecordSource>)
        })
    }

    fn validate(&self, request: SourceOpen) -> BoxFuture<'static, Result<(), String>> {
        Box::pin(async move {
            let _ = PostgresSource::open(request).await?;
            Ok(())
        })
    }
}

/// Install helper used by `diavasi serve`.
pub fn factory() -> Arc<PostgresFactory> {
    Arc::new(PostgresFactory)
}

#[cfg(test)]
mod tests;
