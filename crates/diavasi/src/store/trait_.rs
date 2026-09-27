use crate::core::{GroupId, LogicalCursor};

use super::error::StoreResult;
use super::types::{ConnectionRecord, GroupRecord};

/// Narrow persistence API for Diavasi metadata.
///
/// Implementations must treat successful return from [`commit_progress`] /
/// [`commit_checkpoint`] as the durability point for committed cursors.
pub trait StateStore: Send + Sync {
    fn schema_version(&self) -> u32;

    fn put_connection(&self, conn: &ConnectionRecord) -> StoreResult<()>;
    fn get_connection(&self, id: &str) -> StoreResult<Option<ConnectionRecord>>;
    fn list_connections(&self) -> StoreResult<Vec<ConnectionRecord>>;
    fn delete_connection(&self, id: &str) -> StoreResult<()>;

    /// Insert or replace a group record.
    fn put_group(&self, group: &GroupRecord) -> StoreResult<()>;

    /// Create a group and its checkpoint in one transaction. Fails with
    /// [`StoreError::GroupExists`](super::StoreError::GroupExists) when the id
    /// is taken, and then changes nothing.
    fn insert_group(&self, group: &GroupRecord, cursor: &LogicalCursor) -> StoreResult<()>;
    fn get_group(&self, id: &GroupId) -> StoreResult<Option<GroupRecord>>;
    fn list_groups(&self) -> StoreResult<Vec<GroupRecord>>;
    fn delete_group(&self, id: &GroupId) -> StoreResult<()>;

    fn load_checkpoint(&self, id: &GroupId) -> StoreResult<LogicalCursor>;

    /// Persist only the committed cursor (single write transaction).
    fn commit_checkpoint(&self, id: &GroupId, cursor: &LogicalCursor) -> StoreResult<()>;

    /// Atomically persist group metadata and committed cursor. Fails with
    /// [`StoreError::GroupNotFound`](super::StoreError::GroupNotFound) when
    /// the group was deleted, so a running group cannot recreate it.
    fn commit_progress(&self, group: &GroupRecord, cursor: &LogicalCursor) -> StoreResult<()>;

    /// Like [`commit_progress`], but invokes `before_commit` after staging writes
    /// and before the durability point (`txn.commit()`).
    fn commit_progress_with_hook(
        &self,
        group: &GroupRecord,
        cursor: &LogicalCursor,
        before_commit: &dyn Fn() -> StoreResult<()>,
    ) -> StoreResult<()> {
        before_commit()?;
        self.commit_progress(group, cursor)
    }
}
