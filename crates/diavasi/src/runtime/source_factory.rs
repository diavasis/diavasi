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
    /// The connection: kind and non-secret `config_json`.
    pub connection: ConnectionRecord,
    /// The group's `source_spec`: what to read and in which order.
    pub source_spec: serde_json::Value,
    /// The connection secret, already opened with the store key.
    pub secret: Vec<u8>,
}

/// Opens [`RecordSource`]s for one connection kind. The binary that links the
/// adapter crates installs a factory; `diavasi` itself depends on none.
///
/// `validate` runs at group create and should fail on a bad spec or an
/// unreachable source. `open` runs at each start and restart.
///
/// A factory for sources over generated rows, configured by
/// `{"rows": <count>}`:
///
/// ```
/// use bytes::Bytes;
/// use diavasi::core::{LogicalCursor, OrderingAtom, OrderingValue, Record, RecordSource, SourceError};
/// use diavasi::runtime::{SourceFactory, SourceOpen, check_keys};
/// use futures::future::BoxFuture;
///
/// struct CountSource {
///     rows: u64,
/// }
///
/// impl RecordSource for CountSource {
///     fn fetch_after<'a>(
///         &'a mut self,
///         cursor: &'a LogicalCursor,
///         limit: usize,
///     ) -> BoxFuture<'a, Result<Vec<Record>, SourceError>> {
///         let start = match cursor.as_ref().map(|c| c.atoms()) {
///             None => 1,
///             Some([OrderingAtom::U64(last)]) => last + 1,
///             Some(_) => {
///                 let err = SourceError::Contract("cursor is not a row number".into());
///                 return Box::pin(async move { Err(err) });
///             }
///         };
///         let records = (start..=self.rows)
///             .take(limit)
///             .map(|id| Record {
///                 ordering: OrderingValue::single_u64(id),
///                 payload: Bytes::from(format!(r#"{{"id":{id}}}"#)),
///             })
///             .collect();
///         Box::pin(async move { Ok(records) })
///     }
/// }
///
/// struct CountFactory;
///
/// fn rows(spec: &serde_json::Value) -> Result<u64, String> {
///     check_keys(spec, &["rows"], "source_spec")?;
///     spec.get("rows")
///         .and_then(|v| v.as_u64())
///         .filter(|n| *n > 0)
///         .ok_or_else(|| "source_spec.rows must be a positive integer".to_string())
/// }
///
/// impl SourceFactory for CountFactory {
///     fn kind(&self) -> &str {
///         "count"
///     }
///
///     fn open(&self, request: SourceOpen) -> BoxFuture<'static, Result<Box<dyn RecordSource>, String>> {
///         Box::pin(async move {
///             let rows = rows(&request.source_spec)?;
///             Ok(Box::new(CountSource { rows }) as Box<dyn RecordSource>)
///         })
///     }
///
///     fn validate(&self, request: SourceOpen) -> BoxFuture<'static, Result<(), String>> {
///         Box::pin(async move { rows(&request.source_spec).map(|_| ()) })
///     }
/// }
/// # let _ = CountFactory;
/// ```
pub trait SourceFactory: Send + Sync {
    /// Adapter label this factory opens, matching `ConnectionRecord::kind`.
    fn kind(&self) -> &str;

    /// A router overrides this to accept every leaf factory it holds.
    fn supports(&self, kind: &str) -> bool {
        self.kind() == kind
    }

    /// Open a source for a group. Errors are reported as transient and retried with backoff.
    fn open(
        &self,
        request: SourceOpen,
    ) -> BoxFuture<'static, Result<Box<dyn RecordSource>, String>>;

    /// Check the spec and the connection at group create. An error fails the create with 400.
    fn validate(&self, request: SourceOpen) -> BoxFuture<'static, Result<(), String>>;
}
