use serde::{Deserialize, Serialize};

use crate::core::{GroupConfig, GroupId, GroupLifecycle, LogicalCursor};

/// Ciphertext plus nonce for a sealed connection secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedSecret {
    /// The 12-byte ChaCha20-Poly1305 nonce, random per secret.
    pub nonce: Vec<u8>,
    /// The encrypted secret with its authentication tag.
    pub ciphertext: Vec<u8>,
}

/// Persisted connection definition. Secrets are sealed at rest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionRecord {
    /// Connection id, unique within the store.
    pub id: String,
    /// Adapter kind label (`"synthetic"`, `"postgres"`, ...).
    pub kind: String,
    /// Non-secret configuration JSON.
    pub config_json: serde_json::Value,
    /// The password or token, sealed with the store key.
    pub sealed_secret: SealedSecret,
}

/// Durable group definition fields (no buffer / inflight / fetched cursor).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupRecord {
    /// The group configuration.
    pub config: GroupConfig,
    /// The lifecycle the running group last recorded. `Running` or `Draining` groups resume when the server starts.
    pub lifecycle: GroupLifecycle,
    /// The id the next assigned batch gets.
    pub next_batch_id: u64,
    /// A free-text label for the ordering the source promises, such as
    /// `postgres-keyset`. Stored and shown; not interpreted.
    pub ordering_contract: String,
    /// When set, the group reads this connection instead of the synthetic source.
    #[serde(default)]
    pub connection_id: Option<String>,
    /// Adapter query contract. Absent for synthetic groups.
    #[serde(default)]
    pub source_spec: Option<serde_json::Value>,
}

impl GroupRecord {
    /// The group id.
    pub fn group_id(&self) -> &GroupId {
        &self.config.group_id
    }
}

/// Checkpoint payload stored under a group id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CheckpointRecord {
    pub cursor: LogicalCursor,
}
