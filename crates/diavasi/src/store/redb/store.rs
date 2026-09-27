use std::path::Path;
use std::sync::Arc;

use ::redb::{Database, ReadableTable};

use crate::core::{GroupId, LogicalCursor};
use crate::store::error::{StoreError, StoreResult};
use crate::store::trait_::StateStore;
use crate::store::types::{CheckpointRecord, ConnectionRecord, GroupRecord};

use super::keys::{CHECKPOINTS, CONNECTIONS, GROUPS, META, SCHEMA_VERSION, SCHEMA_VERSION_KEY};

/// redb-backed [`StateStore`].
#[derive(Clone)]
pub struct RedbStore {
    db: Arc<Database>,
}

impl RedbStore {
    /// Create a store file at `path`, replacing none: fails when it cannot be created.
    pub fn create(path: impl AsRef<Path>) -> StoreResult<Self> {
        let db = Database::create(path.as_ref())?;
        let store = Self { db: Arc::new(db) };
        store.init_schema()?;
        Ok(store)
    }

    /// Open an existing store file. Fails on a schema version this build does not support.
    pub fn open(path: impl AsRef<Path>) -> StoreResult<Self> {
        let db = Database::open(path.as_ref())?;
        let store = Self { db: Arc::new(db) };
        store.check_or_init_schema()?;
        Ok(store)
    }

    fn init_schema(&self) -> StoreResult<()> {
        let txn = self.db.begin_write()?;
        {
            let mut meta = txn.open_table(META)?;
            meta.insert(SCHEMA_VERSION_KEY, SCHEMA_VERSION)?;
            let _ = txn.open_table(CONNECTIONS)?;
            let _ = txn.open_table(GROUPS)?;
            let _ = txn.open_table(CHECKPOINTS)?;
        }
        txn.commit()?;
        Ok(())
    }

    fn check_or_init_schema(&self) -> StoreResult<()> {
        let txn = self.db.begin_read()?;
        let meta = match txn.open_table(META) {
            Ok(t) => t,
            Err(::redb::TableError::TableDoesNotExist(_)) => {
                drop(txn);
                return self.init_schema();
            }
            Err(e) => return Err(e.into()),
        };
        match meta.get(SCHEMA_VERSION_KEY)? {
            Some(v) => {
                let found = v.value();
                if found > SCHEMA_VERSION {
                    return Err(StoreError::SchemaVersion {
                        found,
                        supported: SCHEMA_VERSION,
                    });
                }
                if found < SCHEMA_VERSION {
                    // No migration exists yet: version 1 is the first schema.
                    return Err(StoreError::SchemaVersion {
                        found,
                        supported: SCHEMA_VERSION,
                    });
                }
            }
            None => {
                drop(meta);
                drop(txn);
                return self.init_schema();
            }
        }
        Ok(())
    }

    fn encode<T: serde::Serialize>(value: &T) -> StoreResult<Vec<u8>> {
        Ok(serde_json::to_vec(value)?)
    }

    fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> StoreResult<T> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

impl StateStore for RedbStore {
    fn schema_version(&self) -> u32 {
        SCHEMA_VERSION
    }

    fn put_connection(&self, conn: &ConnectionRecord) -> StoreResult<()> {
        let bytes = Self::encode(conn)?;
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(CONNECTIONS)?;
            table.insert(conn.id.as_str(), bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    fn get_connection(&self, id: &str) -> StoreResult<Option<ConnectionRecord>> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(CONNECTIONS)?;
        match table.get(id)? {
            Some(v) => Ok(Some(Self::decode(v.value())?)),
            None => Ok(None),
        }
    }

    fn list_connections(&self) -> StoreResult<Vec<ConnectionRecord>> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(CONNECTIONS)?;
        let mut out: Vec<ConnectionRecord> = Vec::new();
        let iter = table.iter()?;
        for item in iter {
            let (_, v) = item?;
            out.push(Self::decode(v.value())?);
        }
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }

    fn delete_connection(&self, id: &str) -> StoreResult<()> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(CONNECTIONS)?;
            table.remove(id)?;
        }
        txn.commit()?;
        Ok(())
    }

    fn put_group(&self, group: &GroupRecord) -> StoreResult<()> {
        let bytes = Self::encode(group)?;
        let key = group.group_id().as_str();
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(GROUPS)?;
            table.insert(key, bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    fn insert_group(&self, group: &GroupRecord, cursor: &LogicalCursor) -> StoreResult<()> {
        let group_bytes = Self::encode(group)?;
        let checkpoint_bytes = Self::encode(&CheckpointRecord {
            cursor: cursor.clone(),
        })?;
        let key = group.group_id().as_str();
        let txn = self.db.begin_write()?;
        {
            let mut groups = txn.open_table(GROUPS)?;
            if groups.get(key)?.is_some() {
                return Err(StoreError::GroupExists(key.to_string()));
            }
            groups.insert(key, group_bytes.as_slice())?;
            let mut checkpoints = txn.open_table(CHECKPOINTS)?;
            checkpoints.insert(key, checkpoint_bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    fn get_group(&self, id: &GroupId) -> StoreResult<Option<GroupRecord>> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(GROUPS)?;
        match table.get(id.as_str())? {
            Some(v) => Ok(Some(Self::decode(v.value())?)),
            None => Ok(None),
        }
    }

    fn list_groups(&self) -> StoreResult<Vec<GroupRecord>> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(GROUPS)?;
        let mut out: Vec<GroupRecord> = Vec::new();
        for item in table.iter()? {
            let (_, v) = item?;
            out.push(Self::decode(v.value())?);
        }
        out.sort_by(|a, b| a.group_id().as_str().cmp(b.group_id().as_str()));
        Ok(out)
    }

    fn delete_group(&self, id: &GroupId) -> StoreResult<()> {
        let txn = self.db.begin_write()?;
        {
            let mut groups = txn.open_table(GROUPS)?;
            groups.remove(id.as_str())?;
            let mut checkpoints = txn.open_table(CHECKPOINTS)?;
            checkpoints.remove(id.as_str())?;
        }
        txn.commit()?;
        Ok(())
    }

    fn load_checkpoint(&self, id: &GroupId) -> StoreResult<LogicalCursor> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(CHECKPOINTS)?;
        match table.get(id.as_str())? {
            Some(v) => {
                let rec: CheckpointRecord = Self::decode(v.value())?;
                Ok(rec.cursor)
            }
            None => Ok(None),
        }
    }

    fn commit_checkpoint(&self, id: &GroupId, cursor: &LogicalCursor) -> StoreResult<()> {
        let bytes = Self::encode(&CheckpointRecord {
            cursor: cursor.clone(),
        })?;
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(CHECKPOINTS)?;
            table.insert(id.as_str(), bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    fn commit_progress(&self, group: &GroupRecord, cursor: &LogicalCursor) -> StoreResult<()> {
        self.commit_progress_with_hook(group, cursor, &|| Ok(()))
    }

    fn commit_progress_with_hook(
        &self,
        group: &GroupRecord,
        cursor: &LogicalCursor,
        before_commit: &dyn Fn() -> StoreResult<()>,
    ) -> StoreResult<()> {
        let group_bytes = Self::encode(group)?;
        let checkpoint_bytes = Self::encode(&CheckpointRecord {
            cursor: cursor.clone(),
        })?;
        let key = group.group_id().as_str();
        let txn = self.db.begin_write()?;
        {
            let mut groups = txn.open_table(GROUPS)?;
            if groups.get(key)?.is_none() {
                return Err(StoreError::GroupNotFound(key.to_string()));
            }
            groups.insert(key, group_bytes.as_slice())?;
            let mut checkpoints = txn.open_table(CHECKPOINTS)?;
            checkpoints.insert(key, checkpoint_bytes.as_slice())?;
        }
        before_commit()?;
        txn.commit()?;
        Ok(())
    }
}
