//! Request and response bodies of the HTTP API.

use serde::{Deserialize, Serialize};

use crate::core::{GroupConfig, GroupLifecycle, LogicalCursor};

/// `POST /v1/connections`.
///
/// ```json
/// {"id": "pg-main", "kind": "postgres",
///  "config_json": {"host": "db.internal", "port": 5432, "dbname": "app", "user": "diavasi", "sslmode": "require"},
///  "secret": "s3cret"}
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionCreateRequest {
    /// Connection id: 1 to 128 characters from `A-Z a-z 0-9 . _ - :`.
    pub id: String,
    /// Adapter kind: `postgres`, `mongodb`, `redis`, or `scylla`.
    pub kind: String,
    /// Non-secret settings. The keys depend on `kind`; see `docs/adapters`.
    pub config_json: serde_json::Value,
    /// Plaintext secret; sealed at rest and never returned again.
    pub secret: String,
}

/// A connection as the API returns it. The secret is never included.
///
/// ```json
/// {"id": "pg-main", "kind": "postgres", "config_json": {"host": "db.internal"}, "secret_sealed": true}
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionView {
    /// Connection id.
    pub id: String,
    /// Adapter kind.
    pub kind: String,
    /// Non-secret settings.
    pub config_json: serde_json::Value,
    /// Always true: the secret is stored sealed.
    pub secret_sealed: bool,
}

/// `POST /v1/groups`.
///
/// ```json
/// {"group_id": "orders",
///  "max_buffer_records": 4096, "max_buffer_bytes": 8388608, "batch_max_records": 200,
///  "batch_timeout_ms": 30000,
///  "connection_id": "pg-main",
///  "source_spec": {"table": "app.orders", "order_by": [{"column": "id", "type": "int8"}], "payload": ["id", "total"]}}
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupCreateRequest {
    /// Group id: 1 to 128 characters from `A-Z a-z 0-9 . _ - :`.
    pub group_id: String,
    /// Synthetic groups only: number of records. Omit, or 0, for an adapter
    /// group.
    #[serde(default)]
    pub total_records: u64,
    /// Synthetic groups only: payload bytes per record. Omit, or 0, for an
    /// adapter group.
    #[serde(default)]
    pub payload_size: usize,
    /// Most records the buffer holds. At least 1.
    pub max_buffer_records: usize,
    /// Most payload bytes the buffer holds. At least 1.
    pub max_buffer_bytes: usize,
    /// Most records per batch. 1 to `max_buffer_records`.
    pub batch_max_records: usize,
    /// A batch not acked in this many milliseconds is delivered again. At
    /// least 100.
    pub batch_timeout_ms: u64,
    /// A free-text label for the ordering, such as `postgres-keyset`. At most
    /// 1024 bytes. Stored and shown; not interpreted. Empty or omitted means
    /// `synthetic-u64` for a synthetic group and the connection kind for an
    /// adapter group.
    #[serde(default)]
    pub ordering_contract: String,
    /// Optional connection this group reads. Omit for a synthetic group.
    #[serde(default)]
    pub connection_id: Option<String>,
    /// Adapter source contract. Required when `connection_id` is set.
    #[serde(default)]
    pub source_spec: Option<serde_json::Value>,
}

/// A group as the API returns it. `lifecycle` agrees with `running`.
///
/// ```json
/// {"group_id": "orders", "total_records": 0, "payload_size": 0,
///  "max_buffer_records": 4096, "max_buffer_bytes": 8388608, "batch_max_records": 200,
///  "batch_timeout_ms": 30000, "ordering_contract": "postgres-keyset",
///  "connection_id": "pg-main", "lifecycle": "Running", "next_batch_id": 1532, "running": true}
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupView {
    /// Group id.
    pub group_id: String,
    /// Synthetic groups only: number of records.
    pub total_records: u64,
    /// Synthetic groups only: payload bytes per record.
    pub payload_size: usize,
    /// Most records the buffer holds.
    pub max_buffer_records: usize,
    /// Most payload bytes the buffer holds.
    pub max_buffer_bytes: usize,
    /// Most records per batch.
    pub batch_max_records: usize,
    /// Batch timeout in milliseconds.
    pub batch_timeout_ms: u64,
    /// The ordering label given at create.
    pub ordering_contract: String,
    /// The connection read, or `null` for a synthetic group.
    pub connection_id: Option<String>,
    /// `Running` or `Draining` while running, `Recovering` while waiting to
    /// restart after a failure, otherwise `Stopped` or `Failed`.
    pub lifecycle: GroupLifecycle,
    /// The id the next batch gets, as last stored.
    pub next_batch_id: u64,
    /// True while the group's task runs.
    pub running: bool,
}

/// `GET /v1/groups/{id}/checkpoint`.
///
/// ```json
/// {"group_id": "orders", "durable_cursor": [{"I64": 9001}], "live_cursor": [{"I64": 9001}]}
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointView {
    /// Group id.
    pub group_id: String,
    /// The committed cursor in the store. `null` at the start of the stream.
    pub durable_cursor: LogicalCursor,
    /// The committed cursor in the running group, which can lead the stored
    /// one by up to one checkpoint interval. `null` when not running.
    pub live_cursor: Option<LogicalCursor>,
}

/// `GET /v1/status`.
///
/// ```json
/// {"version": "0.13.0", "schema_version": 1, "running_groups": ["orders"], "bind": "127.0.0.1:7700"}
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusView {
    /// Server version.
    pub version: String,
    /// Store schema version.
    pub schema_version: u32,
    /// Ids of running groups.
    pub running_groups: Vec<String>,
    /// Control-plane address.
    pub bind: String,
}

/// `GET /v1/groups/{id}/consumers`.
///
/// ```json
/// {"group_id": "orders", "consumers": ["worker-1", "worker-2"]}
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumersView {
    /// Group id.
    pub group_id: String,
    /// Joined consumers, sorted. Empty when the group is not running.
    pub consumers: Vec<String>,
}

/// Operator view of one group: position, lag, counters, and the last stop.
///
/// `fetched_cursor` is the runtime's read position while `running` is true.
/// A null cursor then means the group has not fetched yet. While `running` is
/// false there is no live fetched position, so `fetched_cursor` is null.
/// `last_stop_reason` and `recovered` are process-local.
///
/// ```json
/// {"group_id": "orders", "running": true, "lifecycle": "Running",
///  "committed_cursor": [{"I64": 9001}], "fetched_cursor": [{"I64": 9420}],
///  "buffer_records": 300, "buffer_bytes": 61440, "inflight_records": 119,
///  "consumers": ["worker-1", "worker-2"],
///  "records_fetched": 9420, "records_delivered": 9120, "records_acked": 9001,
///  "records_replayed": 12, "bytes": 1929216, "checkpoint_lag": 419,
///  "restarts": 0, "consumer_disconnects": 1, "adapter_errors": 0,
///  "last_stop_reason": null, "recovered": false}
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticsView {
    /// Group id.
    pub group_id: String,
    /// True while the group's task runs.
    pub running: bool,
    /// Lifecycle; agrees with `running` as in [`GroupView::lifecycle`].
    pub lifecycle: GroupLifecycle,
    /// Last position with every earlier record acked: live while running,
    /// otherwise from the store.
    pub committed_cursor: LogicalCursor,
    /// Last position read from the source. `null` when not running.
    pub fetched_cursor: LogicalCursor,
    /// Records in the buffer.
    pub buffer_records: u64,
    /// Payload bytes in the buffer.
    pub buffer_bytes: u64,
    /// Records assigned and not yet acked.
    pub inflight_records: u64,
    /// Joined consumers.
    pub consumers: Vec<String>,
    /// Records read from the source in this process.
    pub records_fetched: u64,
    /// Records assigned in this process.
    pub records_delivered: u64,
    /// Records acked in this process.
    pub records_acked: u64,
    /// Records delivered again in this process.
    pub records_replayed: u64,
    /// Payload bytes read in this process.
    pub bytes: u64,
    /// Buffer plus in-flight records: fetched and not yet committed.
    pub checkpoint_lag: u64,
    /// Restarts after a failure in this process.
    pub restarts: u64,
    /// Consumer streams that left or dropped in this process.
    pub consumer_disconnects: u64,
    /// Failed source reads in this process.
    pub adapter_errors: u64,
    /// Why the group last stopped in this process: `paused`, `drained`,
    /// `shutdown`, `task aborted`, `task panicked`, or a source error.
    pub last_stop_reason: Option<String>,
    /// True after the supervisor restarted the group in this process.
    pub recovered: bool,
}

/// `POST /v1/store/backup`.
///
/// ```json
/// {"path": "/var/backups/diavasi/meta-2026-09-27.redb"}
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupRequest {
    /// Absolute path on the server for the copy. It must not exist.
    pub path: String,
}

/// What a backup wrote.
///
/// ```json
/// {"path": "/var/backups/diavasi/meta-2026-09-27.redb", "connections": 2, "groups": 5}
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupView {
    /// Where the copy was written.
    pub path: String,
    /// Connections copied.
    pub connections: usize,
    /// Groups copied, each with its checkpoint.
    pub groups: usize,
}

/// The body of every error response.
///
/// ```json
/// {"error": "group is running; pause it before delete: orders"}
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorBody {
    /// What went wrong.
    pub error: String,
}

/// Longest group, connection, or consumer id.
pub const MAX_ID_LEN: usize = 128;
/// Shortest batch timeout. A shorter one requeues batches faster than a
/// consumer can ack them.
pub const MIN_BATCH_TIMEOUT_MS: u64 = 100;

/// Ids appear in URL paths and metric labels: 1 to [`MAX_ID_LEN`] characters
/// from `A-Z a-z 0-9 . _ - :`.
pub fn check_id(what: &str, id: &str) -> Result<(), String> {
    let valid = !id.is_empty()
        && id.len() <= MAX_ID_LEN
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | ':'));
    if valid {
        Ok(())
    } else {
        Err(format!(
            "{what} must be 1 to {MAX_ID_LEN} characters from A-Z a-z 0-9 . _ - :"
        ))
    }
}

/// Validate a create request and build the group configuration. The error
/// text is returned to the caller with HTTP 400.
pub fn group_config_from_create(req: &GroupCreateRequest) -> Result<GroupConfig, String> {
    check_id("group id", &req.group_id)?;
    let group_id = crate::core::GroupId::new(req.group_id.clone()).map_err(|e| e.to_string())?;
    if req.batch_max_records == 0 {
        return Err("batch_max_records must be non-zero".into());
    }
    if req.max_buffer_records == 0 {
        return Err("max_buffer_records must be non-zero".into());
    }
    if req.max_buffer_bytes == 0 {
        return Err("max_buffer_bytes must be non-zero".into());
    }
    if req.batch_max_records > req.max_buffer_records {
        return Err("batch_max_records must not exceed max_buffer_records".into());
    }
    if req.batch_timeout_ms < MIN_BATCH_TIMEOUT_MS {
        return Err(format!(
            "batch_timeout_ms must be at least {MIN_BATCH_TIMEOUT_MS}"
        ));
    }
    Ok(GroupConfig {
        group_id,
        total_records: req.total_records,
        payload_size: req.payload_size,
        max_buffer_records: req.max_buffer_records,
        max_buffer_bytes: req.max_buffer_bytes,
        batch_max_records: req.batch_max_records,
        batch_timeout: std::time::Duration::from_millis(req.batch_timeout_ms),
    })
}
