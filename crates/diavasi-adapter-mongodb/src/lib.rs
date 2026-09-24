//! MongoDB find keyset source.

mod catalog;
pub mod connect;
mod reader;
mod spec;

use std::sync::Arc;

use diavasi::runtime::{SourceFactory, SourceOpen};
use futures::future::BoxFuture;

use reader::MongoSource;

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
