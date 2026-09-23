use futures::future::BoxFuture;

use crate::core::RecordSource;
use crate::store::ConnectionRecord;

/// Everything an adapter needs to open or validate a group source.
pub struct SourceOpen {
    pub connection: ConnectionRecord,
    pub source_spec: serde_json::Value,
    pub secret: Vec<u8>,
}

/// Installed by the process that links adapter crates. `diavasi` does not depend on them.
pub trait SourceFactory: Send + Sync {
    fn open(
        &self,
        request: SourceOpen,
    ) -> BoxFuture<'static, Result<Box<dyn RecordSource>, String>>;

    fn validate(&self, request: SourceOpen) -> BoxFuture<'static, Result<(), String>>;
}
