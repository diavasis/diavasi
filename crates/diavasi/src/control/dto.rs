use serde::{Deserialize, Serialize};

use crate::core::{GroupConfig, GroupLifecycle, LogicalCursor};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionCreateRequest {
    pub id: String,
    pub kind: String,
    pub config_json: serde_json::Value,
    /// Plaintext secret; sealed at rest and never returned again.
    pub secret: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionView {
    pub id: String,
    pub kind: String,
    pub config_json: serde_json::Value,
    pub secret_sealed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupCreateRequest {
    pub group_id: String,
    pub total_records: u64,
    pub payload_size: usize,
    pub max_buffer_records: usize,
    pub max_buffer_bytes: usize,
    pub batch_max_records: usize,
    /// Timeout in milliseconds.
    pub batch_timeout_ms: u64,
    pub ordering_contract: String,
    /// Optional connection this group reads.
    #[serde(default)]
    pub connection_id: Option<String>,
    /// Adapter source contract. Required when `connection_id` is set.
    #[serde(default)]
    pub source_spec: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupView {
    pub group_id: String,
    pub total_records: u64,
    pub payload_size: usize,
    pub max_buffer_records: usize,
    pub max_buffer_bytes: usize,
    pub batch_max_records: usize,
    pub batch_timeout_ms: u64,
    pub ordering_contract: String,
    pub connection_id: Option<String>,
    pub lifecycle: GroupLifecycle,
    pub next_batch_id: u64,
    pub running: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointView {
    pub group_id: String,
    pub durable_cursor: LogicalCursor,
    pub live_cursor: Option<LogicalCursor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusView {
    pub version: String,
    pub schema_version: u32,
    pub running_groups: Vec<String>,
    pub bind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumersView {
    pub group_id: String,
    pub consumers: Vec<String>,
}

/// Operator view of one group: position, lag, counters, and the last stop.
///
/// `fetched_cursor` is the runtime's read position while `running` is true.
/// A null cursor then means the group has not fetched yet. While `running` is
/// false there is no live fetched position, so `fetched_cursor` is null.
/// `last_stop_reason` and `recovered` are process-local.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticsView {
    pub group_id: String,
    pub running: bool,
    pub lifecycle: GroupLifecycle,
    pub committed_cursor: LogicalCursor,
    pub fetched_cursor: LogicalCursor,
    pub buffer_records: u64,
    pub buffer_bytes: u64,
    pub inflight_records: u64,
    pub consumers: Vec<String>,
    pub records_fetched: u64,
    pub records_delivered: u64,
    pub records_acked: u64,
    pub records_replayed: u64,
    pub bytes: u64,
    pub checkpoint_lag: u64,
    pub restarts: u64,
    pub consumer_disconnects: u64,
    pub adapter_errors: u64,
    pub last_stop_reason: Option<String>,
    pub recovered: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorBody {
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
