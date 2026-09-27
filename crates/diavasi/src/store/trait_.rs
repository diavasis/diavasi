use crate::core::{GroupId, LogicalCursor};

use super::error::StoreResult;
use super::types::{ConnectionRecord, GroupRecord};

/// Persistence for connections, groups, and committed cursors.
///
/// A successful return from [`Self::commit_progress`] or
/// [`Self::commit_checkpoint`] means the cursor is durable. Keys are group
/// and connection ids. [`RedbStore`](super::RedbStore) is the implementation
/// the server uses; a test can wrap it to count or fail writes.
pub trait StateStore: Send + Sync {
    /// The schema version this store writes.
    fn schema_version(&self) -> u32;

    /// Insert or replace a connection.
    fn put_connection(&self, conn: &ConnectionRecord) -> StoreResult<()>;
    /// The connection `id`, if stored.
    fn get_connection(&self, id: &str) -> StoreResult<Option<ConnectionRecord>>;
    /// Every connection, sorted by id.
    fn list_connections(&self) -> StoreResult<Vec<ConnectionRecord>>;
    /// Remove a connection. Removing a missing one is not an error.
    fn delete_connection(&self, id: &str) -> StoreResult<()>;

    /// Insert or replace a group record.
    fn put_group(&self, group: &GroupRecord) -> StoreResult<()>;

    /// Create a group and its checkpoint in one transaction. Fails with
    /// [`StoreError::GroupExists`](super::StoreError::GroupExists) when the id
    /// is taken, and then changes nothing.
    fn insert_group(&self, group: &GroupRecord, cursor: &LogicalCursor) -> StoreResult<()>;
    /// The group `id`, if stored.
    fn get_group(&self, id: &GroupId) -> StoreResult<Option<GroupRecord>>;
    /// Every group, sorted by id.
    fn list_groups(&self) -> StoreResult<Vec<GroupRecord>>;
    /// Remove a group and its checkpoint in one transaction.
    fn delete_group(&self, id: &GroupId) -> StoreResult<()>;

    /// The committed cursor of `id`. `None` at the start of the stream or for a missing group.
    fn load_checkpoint(&self, id: &GroupId) -> StoreResult<LogicalCursor>;

    /// Persist only the committed cursor (single write transaction).
    fn commit_checkpoint(&self, id: &GroupId, cursor: &LogicalCursor) -> StoreResult<()>;

    /// Atomically persist group metadata and committed cursor. Fails with
    /// [`StoreError::GroupNotFound`](super::StoreError::GroupNotFound) when
    /// the group was deleted, so a running group cannot recreate it.
    fn commit_progress(&self, group: &GroupRecord, cursor: &LogicalCursor) -> StoreResult<()>;

    /// Like [`Self::commit_progress`], but invokes `before_commit` after staging writes
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
