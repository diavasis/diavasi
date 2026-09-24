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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorBody {
    pub error: String,
}

pub fn group_config_from_create(req: &GroupCreateRequest) -> Result<GroupConfig, String> {
    let group_id = crate::core::GroupId::new(req.group_id.clone()).map_err(|e| e.to_string())?;
    if req.batch_max_records == 0 {
        return Err("batch_max_records must be non-zero".into());
    }
    if req.max_buffer_records == 0 {
        return Err("max_buffer_records must be non-zero".into());
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
