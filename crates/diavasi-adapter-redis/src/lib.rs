//! Redis Streams source for Diavasi. A group reads one stream with `XRANGE`
//! after the committed stream id and writes nothing to Redis. ADR 0010 and
//! `docs/adapters/redis.md` state the contract.
//!
//! Install the factory in the server:
//!
//! ```no_run
//! use std::sync::Arc;
//! use diavasi::control::{ServeConfig, serve};
//!
//! # async fn run(config: ServeConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! serve(ServeConfig {
//!     source_factory: Some(Arc::new(diavasi_adapter_redis::RedisFactory)),
//!     ..config
//! })
//! .await
//! # }
//! ```
//!
//! A connection and a group that reads it:
//!
//! ```json
//! {"id": "rd", "kind": "redis", "config_json": {"host": "cache.internal", "port": 6379, "db": 0}, "secret": "unused"}
//! ```
//!
//! ```json
//! {"stream": "orders", "fields": ["id", "total"]}
//! ```
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

/// Connecting to Redis, from a stored connection or a URL.
pub mod connect;
mod reader;
mod spec;

use std::sync::Arc;

use diavasi::runtime::{SourceFactory, SourceOpen};
use futures::future::BoxFuture;

use reader::RedisSource;

/// The connection `kind` this adapter reads: `redis`.
pub const NAME: &str = "redis";

/// Opens one Redis connection per running group.
#[derive(Debug, Default)]
pub struct RedisFactory;

impl SourceFactory for RedisFactory {
    fn kind(&self) -> &str {
        NAME
    }

    fn open(
        &self,
        request: SourceOpen,
    ) -> BoxFuture<'static, Result<Box<dyn diavasi::core::RecordSource>, String>> {
        Box::pin(async move {
            let source = RedisSource::open(request).await?;
            Ok(Box::new(source) as Box<dyn diavasi::core::RecordSource>)
        })
    }

    fn validate(&self, request: SourceOpen) -> BoxFuture<'static, Result<(), String>> {
        Box::pin(async move {
            let _ = RedisSource::open(request).await?;
            Ok(())
        })
    }
}

/// Install helper used by `diavasi serve`.
pub fn factory() -> Arc<RedisFactory> {
    Arc::new(RedisFactory)
}

#[cfg(test)]
mod tests;
