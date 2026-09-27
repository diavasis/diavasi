use futures::future::BoxFuture;

use crate::core::RecordSource;
use crate::store::ConnectionRecord;

/// Reject keys of the JSON object `value` that are not in `allowed`. `what`
/// names the object in the error, for example `source_spec`. A misspelled key
/// is an error rather than an option silently left at its default.
pub fn check_keys(value: &serde_json::Value, allowed: &[&str], what: &str) -> Result<(), String> {
    let Some(object) = value.as_object() else {
        return Err(format!("{what} must be an object"));
    };
    let unknown: Vec<&str> = object
        .keys()
        .map(String::as_str)
        .filter(|key| !allowed.contains(key))
        .collect();
    if unknown.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{what} has unknown field(s) {}; expected {}",
            unknown.join(", "),
            allowed.join(", ")
        ))
    }
}

/// Everything an adapter needs to open or validate a group source.
pub struct SourceOpen {
    pub connection: ConnectionRecord,
    pub source_spec: serde_json::Value,
    pub secret: Vec<u8>,
}

/// Installed by the process that links adapter crates. `diavasi` does not depend on them.
pub trait SourceFactory: Send + Sync {
    /// Adapter label this factory opens, matching `ConnectionRecord::kind`.
    fn kind(&self) -> &str;

    /// A router overrides this to accept every leaf factory it holds.
    fn supports(&self, kind: &str) -> bool {
        self.kind() == kind
    }

    fn open(
        &self,
        request: SourceOpen,
    ) -> BoxFuture<'static, Result<Box<dyn RecordSource>, String>>;

    fn validate(&self, request: SourceOpen) -> BoxFuture<'static, Result<(), String>>;
}
