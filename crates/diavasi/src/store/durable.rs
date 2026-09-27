use std::sync::Arc;
use std::time::Instant;

use crate::core::{
    AckOutcome, Batch, BatchId, ConsumerId, CoreResult, GroupConfig, GroupEngine, GroupId,
    GroupSnapshot, LogicalCursor,
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
    connection_id: Option<String>,
    source_spec: Option<serde_json::Value>,
    crash: CrashHook,
    /// The committed cursor as last written to the store.
    persisted: LogicalCursor,
}

impl<S: StateStore> DurableGroup<S> {
    /// Create a new group and persist its definition + empty checkpoint.
    pub fn create(
        store: Arc<S>,
        config: GroupConfig,
        ordering_contract: impl Into<String>,
    ) -> StoreResult<Self> {
        Self::create_with_source(store, config, ordering_contract, None, None)
    }

    /// Create a group that may be bound to a connection and adapter source spec.
    pub fn create_with_source(
        store: Arc<S>,
        config: GroupConfig,
        ordering_contract: impl Into<String>,
        connection_id: Option<String>,
        source_spec: Option<serde_json::Value>,
    ) -> StoreResult<Self> {
        let ordering_contract = ordering_contract.into();
        let engine = GroupEngine::new(config)?;
        let group = GroupRecord {
            config: engine.config().clone(),
            lifecycle: engine.lifecycle(),
            next_batch_id: 1,
            ordering_contract: ordering_contract.clone(),
            connection_id: connection_id.clone(),
            source_spec: source_spec.clone(),
        };
        store.insert_group(&group, &None)?;
        Ok(Self {
            engine,
            store,
            ordering_contract,
            connection_id,
            source_spec,
            crash: no_crash(),
            persisted: None,
        })
    }

    /// Open an existing group from the store and recover the engine.
    pub fn open(store: Arc<S>, group_id: &GroupId) -> StoreResult<Self> {
        let group = store
            .get_group(group_id)?
            .ok_or_else(|| StoreError::GroupNotFound(group_id.to_string()))?;
        let cursor = store.load_checkpoint(group_id)?;
        let persisted = cursor.clone();
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
            connection_id: group.connection_id,
            source_spec: group.source_spec,
            crash: no_crash(),
            persisted,
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

    pub fn leave_consumer(&mut self, id: &ConsumerId) -> CoreResult<()> {
        self.engine.leave_consumer(id)
    }

    pub fn drain(&mut self) -> CoreResult<()> {
        self.engine.drain()
    }

    pub fn list_consumers(&self) -> Vec<ConsumerId> {
        self.engine.list_consumers()
    }

    pub fn poll_fetch(&mut self) -> CoreResult<usize> {
        self.engine.poll_fetch()
    }

    pub fn assign_batch(&mut self, consumer_id: &ConsumerId) -> StoreResult<Batch> {
        let batch = self.engine.assign_batch(consumer_id)?;
        check_crash(&self.crash, CrashPoint::AfterDeliver)?;
        Ok(batch)
    }

    /// Ack a batch and persist the checkpoint when the committed cursor
    /// advances. Durable when this returns.
    pub fn ack(&mut self, batch_id: BatchId) -> StoreResult<AckOutcome> {
        let outcome = self.ack_in_memory(batch_id)?;
        self.persist_progress()?;
        check_crash(&self.crash, CrashPoint::AfterAckResponse)?;
        Ok(outcome)
    }

    /// Apply an ack to the engine without writing the store. Follow with
    /// [`Self::persist_progress`]; several acks can share one write.
    pub fn ack_in_memory(&mut self, batch_id: BatchId) -> StoreResult<AckOutcome> {
        let outcome = self.engine.ack(batch_id)?;
        check_crash(&self.crash, CrashPoint::AfterAckApplied)?;
        Ok(outcome)
    }

    /// True when the committed cursor moved since the last write.
    pub fn has_unpersisted_progress(&self) -> bool {
        self.engine.committed_cursor() != &self.persisted
    }

    /// Write the group record and committed cursor in one transaction when
    /// the cursor moved since the last write. Returns whether it wrote.
    pub fn persist_progress(&mut self) -> StoreResult<bool> {
        if !self.has_unpersisted_progress() {
            return Ok(false);
        }
        let cursor = self.engine.committed_cursor().clone();
        check_crash(&self.crash, CrashPoint::BeforeCheckpointCompute)?;
        let group = self.current_group_record();
        check_crash(&self.crash, CrashPoint::BeforeTxnBegin)?;
        let crash = Arc::clone(&self.crash);
        self.store.commit_progress_with_hook(&group, &cursor, &|| {
            check_crash(&crash, CrashPoint::BeforeTxnCommit)
        })?;
        self.persisted = cursor;
        check_crash(&self.crash, CrashPoint::AfterTxnCommit)?;
        Ok(true)
    }

    /// Persist current group record + committed cursor (lifecycle changes,
    /// pause, shutdown).
    pub fn snapshot_to_store(&mut self) -> StoreResult<()> {
        let group = self.current_group_record();
        let cursor = self.engine.committed_cursor().clone();
        self.store.commit_progress(&group, &cursor)?;
        self.persisted = cursor;
        Ok(())
    }

    fn current_group_record(&self) -> GroupRecord {
        let snap = self.engine.snapshot();
        GroupRecord {
            config: snap.config,
            lifecycle: snap.lifecycle,
            next_batch_id: snap.next_batch_id,
            ordering_contract: self.ordering_contract.clone(),
            connection_id: self.connection_id.clone(),
            source_spec: self.source_spec.clone(),
        }
    }

    pub fn tick(&mut self, now: Instant) -> CoreResult<usize> {
        self.engine.tick(now)
    }
}
