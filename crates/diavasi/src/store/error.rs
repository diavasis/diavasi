use thiserror::Error;

/// Result of a store operation.
pub type StoreResult<T> = Result<T, StoreError>;

/// Why a store operation failed.
#[derive(Debug, Error)]
pub enum StoreError {
    /// Reading or writing the store file failed.
    #[error("store I/O: {0}")]
    Io(#[from] std::io::Error),

    /// redb could not open or create the database.
    #[error("redb database: {0}")]
    RedbDatabase(Box<::redb::DatabaseError>),

    /// redb could not start a transaction.
    #[error("redb transaction: {0}")]
    RedbTransaction(Box<::redb::TransactionError>),

    /// redb could not open a table.
    #[error("redb table: {0}")]
    RedbTable(Box<::redb::TableError>),

    /// redb could not read or write storage.
    #[error("redb storage: {0}")]
    RedbStorage(Box<::redb::StorageError>),

    /// redb could not commit a transaction.
    #[error("redb commit: {0}")]
    RedbCommit(Box<::redb::CommitError>),

    /// A stored record could not be encoded or decoded.
    #[error("serialization: {0}")]
    Serde(#[from] serde_json::Error),

    /// Sealing or opening a secret failed, or a key is malformed.
    #[error("crypto: {0}")]
    Crypto(String),

    /// The store was written by a version with a different schema.
    #[error("schema version mismatch: found {found}, supported {supported}")]
    SchemaVersion {
        /// The version recorded in the store.
        found: u32,
        /// The version this build reads and writes.
        supported: u32,
    },

    /// No group with this id is stored.
    #[error("group not found: {0}")]
    GroupNotFound(String),

    /// A group with this id is already stored.
    #[error("group already exists: {0}")]
    GroupExists(String),

    /// No connection with this id is stored.
    #[error("connection not found: {0}")]
    ConnectionNotFound(String),

    /// A [`CrashHook`](crate::store::CrashHook) aborted at this point.
    #[error("simulated crash at {0:?}")]
    SimulatedCrash(crate::store::CrashPoint),

    /// The group engine refused the operation.
    #[error(transparent)]
    Core(#[from] crate::core::CoreError),
}

impl From<::redb::DatabaseError> for StoreError {
    fn from(e: ::redb::DatabaseError) -> Self {
        Self::RedbDatabase(Box::new(e))
    }
}

impl From<::redb::TransactionError> for StoreError {
    fn from(e: ::redb::TransactionError) -> Self {
        Self::RedbTransaction(Box::new(e))
    }
}

impl From<::redb::TableError> for StoreError {
    fn from(e: ::redb::TableError) -> Self {
        Self::RedbTable(Box::new(e))
    }
}

impl From<::redb::StorageError> for StoreError {
    fn from(e: ::redb::StorageError) -> Self {
        Self::RedbStorage(Box::new(e))
    }
}

impl From<::redb::CommitError> for StoreError {
    fn from(e: ::redb::CommitError) -> Self {
        Self::RedbCommit(Box::new(e))
    }
}
