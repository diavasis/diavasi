use serde::{Deserialize, Serialize};
use std::fmt;

use super::error::{CoreError, CoreResult};

/// Opaque consumer-group identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GroupId(String);

impl GroupId {
    /// A group id. Fails when `id` is empty. The control plane also limits ids
    /// to 1 to 128 characters from `A-Z a-z 0-9 . _ - :`.
    ///
    /// ```
    /// use diavasi::core::GroupId;
    /// let id = GroupId::new("orders")?;
    /// assert_eq!(id.as_str(), "orders");
    /// assert!(GroupId::new("").is_err());
    /// # Ok::<(), diavasi::core::CoreError>(())
    /// ```
    pub fn new(id: impl Into<String>) -> CoreResult<Self> {
        let id = id.into();
        if id.is_empty() {
            return Err(CoreError::InvalidArgument(
                "group id must be non-empty".into(),
            ));
        }
        Ok(Self(id))
    }

    /// The id as text.
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
    /// A consumer id. Fails when `id` is empty.
    ///
    /// ```
    /// use diavasi::core::ConsumerId;
    /// let id = ConsumerId::new("worker-1")?;
    /// assert_eq!(id.to_string(), "worker-1");
    /// # Ok::<(), diavasi::core::CoreError>(())
    /// ```
    pub fn new(id: impl Into<String>) -> CoreResult<Self> {
        let id = id.into();
        if id.is_empty() {
            return Err(CoreError::InvalidArgument(
                "consumer id must be non-empty".into(),
            ));
        }
        Ok(Self(id))
    }

    /// The id as text.
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
    /// Wrap a batch id received on the wire.
    ///
    /// ```
    /// use diavasi::core::BatchId;
    /// assert_eq!(BatchId::from_u64(17).as_u64(), 17);
    /// ```
    pub const fn from_u64(id: u64) -> Self {
        Self(id)
    }

    /// The id as sent on the wire.
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for BatchId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
