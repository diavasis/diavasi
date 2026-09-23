use serde::{Deserialize, Serialize};

use crate::core::{GroupConfig, GroupId, GroupLifecycle, LogicalCursor};

/// Ciphertext plus nonce for a sealed connection secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedSecret {
    pub nonce: Vec<u8>,
    pub ciphertext: Vec<u8>,
}

/// Persisted connection definition. Secrets are sealed at rest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionRecord {
    pub id: String,
    /// Adapter kind label (`"synthetic"`, `"postgres"`, ...).
    pub kind: String,
    /// Non-secret configuration JSON.
    pub config_json: serde_json::Value,
    pub sealed_secret: SealedSecret,
}

/// Durable group definition fields (no buffer / inflight / fetched cursor).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupRecord {
    pub config: GroupConfig,
    pub lifecycle: GroupLifecycle,
    pub next_batch_id: u64,
    /// Placeholder for future adapter ordering contracts.
    pub ordering_contract: String,
    /// When set, the group reads this connection instead of the synthetic source.
    #[serde(default)]
    pub connection_id: Option<String>,
    /// Adapter query contract. Absent for synthetic groups.
    #[serde(default)]
    pub source_spec: Option<serde_json::Value>,
}

impl GroupRecord {
    pub fn group_id(&self) -> &GroupId {
        &self.config.group_id
    }

    pub fn from_engine_snapshot(
        snapshot: crate::core::GroupSnapshot,
        ordering_contract: impl Into<String>,
    ) -> Self {
        Self {
            config: snapshot.config,
            lifecycle: snapshot.lifecycle,
            next_batch_id: snapshot.next_batch_id,
            ordering_contract: ordering_contract.into(),
            connection_id: None,
            source_spec: None,
        }
    }
}

/// Checkpoint payload stored under a group id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CheckpointRecord {
    pub cursor: LogicalCursor,
}
