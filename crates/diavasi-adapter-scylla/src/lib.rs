//! ScyllaDB source. Resume is a logical primary-key position.

mod catalog;
pub mod connect;
mod reader;
mod spec;

use std::sync::Arc;

use diavasi::runtime::{SourceFactory, SourceOpen};
use futures::future::BoxFuture;

use reader::ScyllaSource;

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
