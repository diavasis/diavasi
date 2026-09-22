use serde::{Deserialize, Serialize};
use std::fmt;

use super::error::{CoreError, CoreResult};

/// Opaque consumer-group identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GroupId(String);

impl GroupId {
    pub fn new(id: impl Into<String>) -> CoreResult<Self> {
        let id = id.into();
        if id.is_empty() {
            return Err(CoreError::InvalidArgument("group id must be non-empty"));
        }
        Ok(Self(id))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for GroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Opaque consumer identity within a group.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConsumerId(String);

impl ConsumerId {
    pub fn new(id: impl Into<String>) -> CoreResult<Self> {
        let id = id.into();
        if id.is_empty() {
            return Err(CoreError::InvalidArgument("consumer id must be non-empty"));
        }
        Ok(Self(id))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ConsumerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Monotonic batch identity within a group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BatchId(u64);

impl BatchId {
    pub const fn from_u64(id: u64) -> Self {
        Self(id)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for BatchId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
