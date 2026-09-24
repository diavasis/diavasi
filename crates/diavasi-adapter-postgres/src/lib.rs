//! PostgreSQL keyset source.

mod catalog;
pub mod connect;
mod reader;
mod spec;

use std::sync::Arc;

use diavasi::runtime::{SourceFactory, SourceOpen};
use futures::future::BoxFuture;

use reader::PostgresSource;

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
