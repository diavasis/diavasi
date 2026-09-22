use std::sync::Arc;
use std::time::Instant;

use crate::core::{
    Batch, BatchId, ConsumerId, CoreResult, GroupConfig, GroupEngine, GroupId, GroupSnapshot,
    LogicalCursor,
};

use super::crash::{CrashHook, CrashPoint, check_crash, no_crash};
use super::error::{StoreError, StoreResult};
use super::trait_::StateStore;
use super::types::GroupRecord;

/// Group engine with durable committed-cursor persistence.
pub struct DurableGroup<S: StateStore> {
    engine: GroupEngine,
    store: Arc<S>,
    ordering_contract: String,
    crash: CrashHook,
}

impl<S: StateStore> DurableGroup<S> {
    /// Create a new group and persist its definition + empty checkpoint.
    pub fn create(
        store: Arc<S>,
        config: GroupConfig,
        ordering_contract: impl Into<String>,
    ) -> StoreResult<Self> {
        let ordering_contract = ordering_contract.into();
        let engine = GroupEngine::new(config)?;
        let group = GroupRecord {
            config: engine.config().clone(),
            lifecycle: engine.lifecycle(),
            next_batch_id: 1,
            ordering_contract: ordering_contract.clone(),
        };
        store.put_group(&group)?;
        store.commit_checkpoint(group.group_id(), &None)?;
        Ok(Self {
            engine,
            store,
            ordering_contract,
            crash: no_crash(),
        })
    }

    /// Open an existing group from the store and recover the engine.
    pub fn open(store: Arc<S>, group_id: &GroupId) -> StoreResult<Self> {
        let group = store
            .get_group(group_id)?
            .ok_or_else(|| StoreError::GroupNotFound(group_id.to_string()))?;
        let cursor = store.load_checkpoint(group_id)?;
        let snapshot = GroupSnapshot {
            config: group.config.clone(),
            lifecycle: group.lifecycle,
            committed_cursor: cursor,
            next_batch_id: group.next_batch_id,
        };
        let engine = GroupEngine::recover_from(snapshot)?;
        Ok(Self {
            engine,
            store,
            ordering_contract: group.ordering_contract,
            crash: no_crash(),
        })
    }

    pub fn with_crash_hook(mut self, hook: CrashHook) -> Self {
        self.crash = hook;
        self
    }

    pub fn set_crash_hook(&mut self, hook: CrashHook) {
        self.crash = hook;
    }

    pub fn engine(&self) -> &GroupEngine {
        &self.engine
    }

    pub fn engine_mut(&mut self) -> &mut GroupEngine {
        &mut self.engine
    }

    pub fn group_id(&self) -> &GroupId {
        &self.engine.config().group_id
    }

    pub fn committed_cursor(&self) -> &LogicalCursor {
        self.engine.committed_cursor()
    }

    pub fn start(&mut self) -> CoreResult<()> {
        self.engine.start()
    }

    pub fn join_consumer(&mut self, id: ConsumerId) -> CoreResult<()> {
        self.engine.join_consumer(id)
    }

    pub fn poll_fetch(&mut self) -> CoreResult<usize> {
        self.engine.poll_fetch()
    }

    pub fn assign_batch(&mut self, consumer_id: &ConsumerId) -> StoreResult<Batch> {
        let batch = self.engine.assign_batch(consumer_id)?;
        check_crash(&self.crash, CrashPoint::AfterDeliver)?;
        Ok(batch)
    }

    /// ACK a batch; persist checkpoint when the committed cursor advances.
    pub fn ack(&mut self, batch_id: BatchId) -> StoreResult<()> {
        let before = self.engine.committed_cursor().clone();
        self.engine.ack(batch_id)?;
        check_crash(&self.crash, CrashPoint::AfterAckApplied)?;

        let after = self.engine.committed_cursor().clone();
        if after != before {
            check_crash(&self.crash, CrashPoint::BeforeCheckpointCompute)?;
            let group = self.current_group_record();
            check_crash(&self.crash, CrashPoint::BeforeTxnBegin)?;
            let crash = Arc::clone(&self.crash);
            self.store.commit_progress_with_hook(&group, &after, &|| {
                check_crash(&crash, CrashPoint::BeforeTxnCommit)
            })?;
            check_crash(&self.crash, CrashPoint::AfterTxnCommit)?;
        }

        check_crash(&self.crash, CrashPoint::AfterAckResponse)?;
        Ok(())
    }

    /// Persist current group record + committed cursor (controlled shutdown).
    pub fn snapshot_to_store(&self) -> StoreResult<()> {
        let group = self.current_group_record();
        let cursor = self.engine.committed_cursor().clone();
        self.store.commit_progress(&group, &cursor)
    }

    fn current_group_record(&self) -> GroupRecord {
        let snap = self.engine.snapshot();
        GroupRecord {
            config: snap.config,
            lifecycle: snap.lifecycle,
            next_batch_id: snap.next_batch_id,
            ordering_contract: self.ordering_contract.clone(),
        }
    }

    pub fn tick(&mut self, now: Instant) -> CoreResult<usize> {
        self.engine.tick(now)
    }
}
