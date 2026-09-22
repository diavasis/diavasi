use thiserror::Error;

pub type StoreResult<T> = Result<T, StoreError>;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("store I/O: {0}")]
    Io(#[from] std::io::Error),

    #[error("redb: {0}")]
    Redb(String),

    #[error("redb database: {0}")]
    RedbDatabase(Box<::redb::DatabaseError>),

    #[error("redb transaction: {0}")]
    RedbTransaction(Box<::redb::TransactionError>),

    #[error("redb table: {0}")]
    RedbTable(Box<::redb::TableError>),

    #[error("redb storage: {0}")]
    RedbStorage(Box<::redb::StorageError>),

    #[error("redb commit: {0}")]
    RedbCommit(Box<::redb::CommitError>),

    #[error("serialization: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("crypto: {0}")]
    Crypto(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(&'static str),

    #[error("schema version mismatch: found {found}, supported {supported}")]
    SchemaVersion { found: u32, supported: u32 },

    #[error("group not found: {0}")]
    GroupNotFound(String),

    #[error("connection not found: {0}")]
    ConnectionNotFound(String),

    #[error("simulated crash at {0:?}")]
    SimulatedCrash(crate::store::CrashPoint),

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
