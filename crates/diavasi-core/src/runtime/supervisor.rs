use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::FutureExt;
use tokio::task::{AbortHandle, JoinHandle};

use crate::core::{GroupId, GroupLifecycle, RecordSource, SourceError};
use crate::observe::Observe;
use crate::store::{DurableGroup, StateStore, StoreError, StoreKey, open_secret};

use super::error::{RuntimeError, RuntimeResult};
use super::group_runtime::{
    GroupExit, GroupRuntimeConfig, SpawnedGroup, map_join_result, spawn_group_runtime,
};
use super::handle::GroupHandle;
use super::source_factory::{SourceFactory, SourceOpen};

/// Delay before the first restart after a failure. Each further consecutive
/// failure doubles it, up to [`RETRY_MAX`].
pub const RETRY_FIRST: Duration = Duration::from_millis(250);
/// Longest delay between restarts.
pub const RETRY_MAX: Duration = Duration::from_secs(30);
/// A group that ran at least this long before it failed starts its backoff
/// over at [`RETRY_FIRST`].
pub const RETRY_RESET_AFTER: Duration = Duration::from_secs(60);

struct RunningGroup {
    handle: GroupHandle,
    abort: AbortHandle,
    join: JoinHandle<RuntimeResult<GroupExit>>,
    started_at: Instant,
    /// Consecutive failures before this start. 0 for an operator start.
    failures: u32,
}

/// A failed group waiting for its next restart.
struct Retry {
    failures: u32,
    next_attempt: Instant,
}

/// Why a group task last stopped. `Display` gives the text the API reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopReason {
    /// An operator pause (`paused`).
    Paused,
    /// A drain finished (`drained`).
    Drained,
    /// The server shut down (`shutdown`).
    Shutdown,
    /// The task was aborted (`task aborted`).
    Aborted,
    /// The task panicked (`task panicked`).
    Panicked,
    /// The source could not be reached; the group is retried.
    SourceUnavailable(String),
    /// The data broke the source contract; the group is `Failed` until an
    /// operator starts it.
    BadData(String),
    /// Another runtime error; the group is retried.
    Error(String),
    /// A restart could not open the source; it is retried.
    RestartFailed(String),
}

impl std::fmt::Display for StopReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Paused => f.write_str("paused"),
            Self::Drained => f.write_str("drained"),
            Self::Shutdown => f.write_str("shutdown"),
            Self::Aborted => f.write_str("task aborted"),
            Self::Panicked => f.write_str("task panicked"),
            Self::SourceUnavailable(text)
            | Self::BadData(text)
            | Self::Error(text)
            | Self::RestartFailed(text) => f.write_str(text),
        }
    }
}

/// Why the last group task exited, and whether the supervisor respawned it.
/// Process-local. A process restart clears it. The durable lifecycle and
/// checkpoint remain in the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupOutcome {
    /// Why the task last stopped. `None` until a restart without a recorded
    /// stop (a group recovered before this process saw it stop).
    pub last_stop_reason: Option<StopReason>,
    /// True after the supervisor restarted the group in this process.
    pub recovered: bool,
}

/// Everything needed to open a group, detached from the supervisor so the
/// source can be opened without holding a lock on it. Built by
/// [`GroupSupervisor::plan_start`] or during [`GroupSupervisor::reap`], and
/// handed back to [`GroupSupervisor::finish_start`] after
/// [`StartPlan::open`].
pub struct StartPlan<S: StateStore> {
    id: GroupId,
    store: Arc<S>,
    factory: Option<Arc<dyn SourceFactory>>,
    key: Option<StoreKey>,
    failures: u32,
    recovery: bool,
}

/// A group whose store record and source are open and ready to spawn.
pub struct OpenedGroup<S: StateStore> {
    id: GroupId,
    durable: DurableGroup<S>,
    source: Option<Box<dyn RecordSource>>,
    adapter: String,
    failures: u32,
    recovery: bool,
}

/// A start that failed while opening.
pub struct FailedStart {
    id: GroupId,
    failures: u32,
    recovery: bool,
    error: RuntimeError,
}

impl<S: StateStore + 'static> StartPlan<S> {
    /// The group to open.
    pub fn group_id(&self) -> &GroupId {
        &self.id
    }

    /// Read the group from the store and open its source. May wait on the
    /// network; hold no lock across it.
    pub async fn open(self) -> Result<OpenedGroup<S>, FailedStart> {
        let opened = async {
            let durable = DurableGroup::open(Arc::clone(&self.store), &self.id)?;
            let (adapter, source) = open_source(
                self.store.as_ref(),
                self.factory.as_ref(),
                self.key.as_ref(),
                &self.id,
            )
            .await?;
            Ok::<_, RuntimeError>((durable, adapter, source))
        }
        .await;
        match opened {
            Ok((durable, adapter, source)) => Ok(OpenedGroup {
                id: self.id,
                durable,
                source,
                adapter,
                failures: self.failures,
                recovery: self.recovery,
            }),
            Err(error) => Err(FailedStart {
                id: self.id,
                failures: self.failures,
                recovery: self.recovery,
                error,
            }),
        }
    }
}

/// Supervises many independent group runtimes over one shared [`StateStore`].
///
/// A group task that fails is restarted from the store after a delay that
/// doubles with each consecutive failure, from [`RETRY_FIRST`] to
/// [`RETRY_MAX`]. A [`SourceError::Contract`] failure is not restarted: the
/// same data would fail again, so the group waits for an operator start.
pub struct GroupSupervisor<S: StateStore> {
    store: Arc<S>,
    runtime_config: GroupRuntimeConfig,
    groups: HashMap<String, RunningGroup>,
    /// Groups whose source is being opened outside the supervisor.
    starting: HashSet<String>,
    retries: HashMap<String, Retry>,
    source_factory: Option<Arc<dyn SourceFactory>>,
    store_key: Option<StoreKey>,
    observe: Observe,
    outcomes: HashMap<String, GroupOutcome>,
}

impl<S: StateStore + 'static> GroupSupervisor<S> {
    /// A supervisor with no running groups and no source factory, so only
    /// synthetic groups can start until [`Self::set_source_factory`].
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::time::Duration;
    /// use diavasi::core::{ConsumerId, GroupConfig, GroupId};
    /// use diavasi::runtime::GroupSupervisor;
    /// use diavasi::store::{DurableGroup, RedbStore};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let dir = tempfile::tempdir()?;
    /// let store = Arc::new(RedbStore::create(dir.path().join("meta.redb"))?);
    /// let config = GroupConfig {
    ///     group_id: GroupId::new("orders")?,
    ///     total_records: 4,
    ///     payload_size: 8,
    ///     max_buffer_records: 4,
    ///     max_buffer_bytes: 1024,
    ///     batch_max_records: 2,
    ///     batch_timeout: Duration::from_secs(30),
    /// };
    /// DurableGroup::create(Arc::clone(&store), config, "synthetic-u64")?;
    ///
    /// let mut supervisor = GroupSupervisor::new(store);
    /// let id = GroupId::new("orders")?;
    /// let handle = supervisor.start_group(&id).await?;
    /// let worker = ConsumerId::new("w")?;
    /// handle.join(worker.clone()).await?;
    /// let batch = handle.assign_wait(&worker, Duration::from_secs(1)).await?;
    /// handle.ack(batch.id).await?;
    /// supervisor.stop_group(&id).await?;
    /// # Ok(()) }
    /// ```
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            runtime_config: GroupRuntimeConfig::default(),
            groups: HashMap::new(),
            starting: HashSet::new(),
            retries: HashMap::new(),
            source_factory: None,
            store_key: None,
            observe: Observe::new(),
            outcomes: HashMap::new(),
        }
    }

    /// The metrics registry shared by the supervised groups.
    pub fn observe(&self) -> Observe {
        self.observe.clone()
    }

    /// Why `id` last stopped in this process, if it did.
    pub fn outcome(&self, id: &GroupId) -> Option<GroupOutcome> {
        self.outcomes.get(id.as_str()).cloned()
    }

    /// Handles of groups whose task is still running.
    pub fn running_handles(&self) -> Vec<GroupHandle> {
        self.live().map(|g| g.handle.clone()).collect()
    }

    fn live(&self) -> impl Iterator<Item = &RunningGroup> {
        self.groups.values().filter(|g| !g.join.is_finished())
    }

    /// The same supervisor with `config` for groups it starts from now on.
    pub fn with_runtime_config(mut self, config: GroupRuntimeConfig) -> Self {
        self.runtime_config = config;
        self
    }

    /// Timing for group runtimes started from now on.
    pub fn set_runtime_config(&mut self, config: GroupRuntimeConfig) {
        self.runtime_config = config;
    }

    /// Install the factory for adapter groups and the key that opens their secrets.
    pub fn set_source_factory(&mut self, factory: Arc<dyn SourceFactory>, key: StoreKey) {
        self.source_factory = Some(factory);
        self.store_key = Some(key);
    }

    /// The installed factory, if any.
    pub fn source_factory(&self) -> Option<Arc<dyn SourceFactory>> {
        self.source_factory.clone()
    }

    /// Groups whose task is running.
    pub fn list_running(&self) -> Vec<GroupId> {
        self.live().map(|g| g.handle.group_id().clone()).collect()
    }

    /// The handle of a group whose task is still running.
    pub fn get_handle(&self, id: &GroupId) -> Option<GroupHandle> {
        self.groups
            .get(id.as_str())
            .filter(|g| !g.join.is_finished())
            .map(|g| g.handle.clone())
    }

    /// True while a failed group waits for its next restart.
    pub fn is_retrying(&self, id: &GroupId) -> bool {
        self.retries.contains_key(id.as_str())
    }

    /// True while the group runs or its source is being opened.
    pub fn is_active(&self, id: &GroupId) -> bool {
        self.get_handle(id).is_some() || self.starting.contains(id.as_str())
    }

    /// Schedule a first restart attempt for a group that could not start,
    /// for example at boot while its database is down.
    pub fn retry_later(&mut self, id: &GroupId, reason: String) {
        if !self.is_active(id) {
            self.schedule_retry(id, 1, StopReason::RestartFailed(reason));
        }
    }

    /// Forget a pending restart. Returns true when one was pending.
    pub fn cancel_retry(&mut self, id: &GroupId) -> bool {
        self.retries.remove(id.as_str()).is_some()
    }

    /// Reserve `id` for an operator start and return the plan to open it.
    /// A pending restart is replaced by this start.
    pub fn plan_start(&mut self, id: &GroupId) -> RuntimeResult<StartPlan<S>> {
        self.collect_finished();
        if self.is_active(id) {
            return Err(RuntimeError::GroupAlreadyRunning(id.to_string()));
        }
        self.retries.remove(id.as_str());
        if let Some(outcome) = self.outcomes.get_mut(id.as_str()) {
            outcome.recovered = false;
        }
        self.starting.insert(id.as_str().to_string());
        Ok(self.plan(id.clone(), 0, false))
    }

    /// Spawn an opened group, or record why it could not open. A failed
    /// restart is scheduled again with a longer delay. A group deleted from
    /// the store is forgotten.
    pub fn finish_start(
        &mut self,
        opened: Result<OpenedGroup<S>, FailedStart>,
    ) -> RuntimeResult<GroupHandle> {
        match opened {
            Ok(opened) => {
                self.starting.remove(opened.id.as_str());
                if opened.recovery {
                    self.observe.record_restart(opened.id.as_str());
                    tracing::warn!(group_id = %opened.id, "group restarted");
                    let outcome = self
                        .outcomes
                        .entry(opened.id.as_str().to_string())
                        .or_insert_with(|| GroupOutcome {
                            last_stop_reason: None,
                            recovered: false,
                        });
                    outcome.recovered = true;
                }
                let spawned = spawn_group_runtime(
                    opened.durable,
                    self.runtime_config.clone(),
                    opened.source,
                    self.observe.clone(),
                    opened.adapter,
                );
                let handle = spawned.handle.clone();
                self.insert_spawned(spawned, opened.failures);
                Ok(handle)
            }
            Err(failed) => {
                self.starting.remove(failed.id.as_str());
                if failed.recovery {
                    let gone = matches!(
                        failed.error,
                        RuntimeError::Store(StoreError::GroupNotFound(_))
                    );
                    if gone {
                        self.outcomes.remove(failed.id.as_str());
                    } else {
                        let failures = failed.failures.saturating_add(1);
                        self.observe.record_recovery_failure(failed.id.as_str());
                        tracing::warn!(
                            group_id = %failed.id,
                            error = %failed.error,
                            failures,
                            "group restart failed"
                        );
                        self.schedule_retry(
                            &failed.id,
                            failures,
                            StopReason::RestartFailed(failed.error.to_string()),
                        );
                    }
                }
                Err(failed.error)
            }
        }
    }

    /// Open the group from the store and spawn its runtime.
    pub async fn start_group(&mut self, id: &GroupId) -> RuntimeResult<GroupHandle> {
        let plan = self.plan_start(id)?;
        let opened = plan.open().await;
        self.finish_start(opened)
    }

    /// First half of a pause. Cancels a pending restart (returns `None`), or
    /// returns the handle to send `Stop` to. Nothing changes until the owner
    /// has answered and [`Self::finish_stop`] runs, so a failed stop leaves
    /// the group supervised.
    pub fn begin_stop(&mut self, id: &GroupId) -> RuntimeResult<Option<GroupHandle>> {
        if self.retries.remove(id.as_str()).is_some() {
            self.note_paused(id);
            return Ok(None);
        }
        self.get_handle(id)
            .map(Some)
            .ok_or_else(|| RuntimeError::GroupNotRunning(id.to_string()))
    }

    /// Second half of a pause, after the owner answered `Stop`. Returns the
    /// task to await, if the supervisor had not already collected it.
    pub fn finish_stop(&mut self, id: &GroupId) -> Option<JoinHandle<RuntimeResult<GroupExit>>> {
        self.note_paused(id);
        self.groups.remove(id.as_str()).map(|running| running.join)
    }

    /// Graceful stop: ask the owner to snapshot, then await exit (no respawn).
    pub async fn stop_group(&mut self, id: &GroupId) -> RuntimeResult<()> {
        let Some(handle) = self.begin_stop(id)? else {
            return Ok(());
        };
        handle.stop().await?;
        match self.finish_stop(id) {
            Some(join) => map_join_result(join.await),
            None => Ok(()),
        }
    }

    /// Hard-kill the group task tree (test / fault injection). The next
    /// [`Self::supervise_once`] treats it as a failure and schedules a restart.
    pub fn abort_group(&mut self, id: &GroupId) -> RuntimeResult<()> {
        let Some(running) = self.groups.get(id.as_str()) else {
            return Err(RuntimeError::GroupNotRunning(id.to_string()));
        };
        running.abort.abort();
        Ok(())
    }

    /// Collect finished tasks, then return the restarts that are due. Open
    /// each plan outside any lock, then pass the result to
    /// [`Self::finish_start`].
    pub fn reap(&mut self) -> Vec<StartPlan<S>> {
        self.collect_finished();
        self.due_restarts()
    }

    /// Record why finished group tasks stopped and schedule restarts for
    /// failures. Cheap and without I/O beyond the store, so read paths call
    /// it to report a group that just stopped.
    pub fn collect_finished(&mut self) {
        let finished: Vec<String> = self
            .groups
            .iter()
            .filter(|(_, running)| running.join.is_finished())
            .map(|(key, _)| key.clone())
            .collect();
        for key in finished {
            let Some(mut running) = self.groups.remove(&key) else {
                continue;
            };
            let id = running.handle.group_id().clone();
            let result = (&mut running.join)
                .now_or_never()
                .expect("a finished task has its result");
            let reason = exit_reason(&result);
            match result {
                Ok(Ok(_)) => {
                    self.set_outcome(&id, reason, false);
                }
                Ok(Err(RuntimeError::Source(SourceError::Contract(_)))) => {
                    tracing::error!(
                        group_id = %id,
                        reason = %reason,
                        "group stopped: the source broke its contract; start it again after fixing the data or the spec"
                    );
                    self.record_failed(&id);
                    self.observe.record_contract_failure(id.as_str());
                    self.set_outcome(&id, reason, false);
                }
                _ => {
                    let failures = if running.started_at.elapsed() >= RETRY_RESET_AFTER {
                        1
                    } else {
                        running.failures.saturating_add(1)
                    };
                    tracing::warn!(group_id = %id, reason = %reason, failures, "group failed");
                    self.schedule_retry(&id, failures, reason);
                }
            }
        }
    }

    /// Restarts whose delay has passed, reserved as starting.
    fn due_restarts(&mut self) -> Vec<StartPlan<S>> {
        let now = Instant::now();
        let due: Vec<(String, u32)> = self
            .retries
            .iter()
            .filter(|(_, retry)| retry.next_attempt <= now)
            .map(|(key, retry)| (key.clone(), retry.failures))
            .collect();
        let mut plans = Vec::with_capacity(due.len());
        for (key, failures) in due {
            self.retries.remove(&key);
            let Ok(id) = GroupId::new(key.clone()) else {
                continue;
            };
            self.starting.insert(key);
            plans.push(self.plan(id, failures, true));
        }
        plans
    }

    /// Collect finished tasks and run the restarts that are due. Never fails:
    /// a restart that cannot open is scheduled again with a longer delay.
    /// Returns the groups that were restarted.
    pub async fn supervise_once(&mut self) -> RuntimeResult<Vec<GroupId>> {
        let mut recovered = Vec::new();
        for plan in self.reap() {
            let opened = plan.open().await;
            if let Ok(handle) = self.finish_start(opened) {
                recovered.push(handle.group_id().clone());
            }
        }
        Ok(recovered)
    }

    /// Spawn with an explicit source. Failure-injection tests use this to
    /// install a source that errors. A later recovery opens the stored source.
    #[cfg(test)]
    pub async fn start_group_with_source(
        &mut self,
        id: &GroupId,
        source: Option<Box<dyn RecordSource>>,
    ) -> RuntimeResult<GroupHandle> {
        if self.is_active(id) {
            return Err(RuntimeError::GroupAlreadyRunning(id.to_string()));
        }
        let durable = DurableGroup::open(Arc::clone(&self.store), id)?;
        let adapter = adapter_label(self.store.as_ref(), id)?;
        let spawned = spawn_group_runtime(
            durable,
            self.runtime_config.clone(),
            source,
            self.observe.clone(),
            adapter,
        );
        let handle = spawned.handle.clone();
        self.insert_spawned(spawned, 0);
        Ok(handle)
    }

    /// Save progress for every running group without changing its lifecycle,
    /// and stop supervising. Groups that were running resume when the server
    /// starts again. Pending restarts are dropped.
    pub async fn shutdown_all(&mut self) {
        self.retries.clear();
        let groups: Vec<RunningGroup> = self.groups.drain().map(|(_, g)| g).collect();
        for running in groups {
            if !running.join.is_finished() {
                if let Err(err) = running.handle.shutdown().await {
                    tracing::warn!(group_id = %running.handle.group_id(), error = %err, "group did not save progress at shutdown");
                    running.abort.abort();
                }
            }
            let _ = running.join.await;
        }
    }

    /// Mark a group `Failed` in the store after an error a restart would repeat.
    fn record_failed(&self, id: &GroupId) {
        let result = self.store.get_group(id).and_then(|group| match group {
            Some(mut group) => {
                group.lifecycle = GroupLifecycle::Failed;
                self.store.put_group(&group)
            }
            None => Ok(()),
        });
        if let Err(err) = result {
            tracing::warn!(group_id = %id, error = %err, "could not record the failed lifecycle");
        }
    }

    fn plan(&self, id: GroupId, failures: u32, recovery: bool) -> StartPlan<S> {
        StartPlan {
            id,
            store: Arc::clone(&self.store),
            factory: self.source_factory.clone(),
            key: self.store_key.clone(),
            failures,
            recovery,
        }
    }

    fn schedule_retry(&mut self, id: &GroupId, failures: u32, reason: StopReason) {
        self.retries.insert(
            id.as_str().to_string(),
            Retry {
                failures,
                next_attempt: Instant::now() + retry_delay(failures),
            },
        );
        self.set_outcome(id, reason, false);
    }

    fn set_outcome(&mut self, id: &GroupId, reason: StopReason, recovered: bool) {
        self.outcomes.insert(
            id.as_str().to_string(),
            GroupOutcome {
                last_stop_reason: Some(reason),
                recovered,
            },
        );
    }

    fn note_paused(&mut self, id: &GroupId) {
        let recovered = self
            .outcomes
            .get(id.as_str())
            .is_some_and(|outcome| outcome.recovered);
        self.set_outcome(id, StopReason::Paused, recovered);
    }

    fn insert_spawned(&mut self, spawned: SpawnedGroup, failures: u32) {
        let key = spawned.handle.group_id().as_str().to_string();
        self.groups.insert(
            key,
            RunningGroup {
                handle: spawned.handle,
                abort: spawned.abort,
                join: spawned.join,
                started_at: Instant::now(),
                failures,
            },
        );
    }
}

/// Delay before restart number `failures` (1 for the first).
fn retry_delay(failures: u32) -> Duration {
    let doublings = failures.saturating_sub(1).min(16);
    RETRY_FIRST.saturating_mul(1 << doublings).min(RETRY_MAX)
}

async fn open_source<S: StateStore>(
    store: &S,
    factory: Option<&Arc<dyn SourceFactory>>,
    key: Option<&StoreKey>,
    id: &GroupId,
) -> RuntimeResult<(String, Option<Box<dyn RecordSource>>)> {
    let group = store
        .get_group(id)?
        .ok_or_else(|| RuntimeError::Store(StoreError::GroupNotFound(id.to_string())))?;
    let Some(connection_id) = group.connection_id else {
        return Ok(("synthetic".into(), None));
    };
    let connection = store.get_connection(&connection_id)?.ok_or_else(|| {
        RuntimeError::Store(StoreError::ConnectionNotFound(connection_id.clone()))
    })?;
    let transient = |message: String| RuntimeError::Source(SourceError::Transient(message));
    let Some(source_spec) = group.source_spec else {
        return Err(transient(format!(
            "{} connection requires source_spec",
            connection.kind
        )));
    };
    let factory = factory.ok_or_else(|| transient("source factory is not installed".into()))?;
    if !factory.supports(&connection.kind) {
        return Err(transient(format!(
            "unsupported connection kind {}",
            connection.kind
        )));
    }
    let key = key.ok_or_else(|| transient("store key is not installed".into()))?;
    let secret = open_secret(key, &connection.sealed_secret)?;
    let adapter = connection.kind.clone();
    let source = factory
        .open(SourceOpen {
            connection,
            source_spec,
            secret,
        })
        .await
        .map_err(transient)?;
    Ok((adapter, Some(source)))
}

#[cfg(test)]
fn adapter_label<S: StateStore>(store: &S, id: &GroupId) -> RuntimeResult<String> {
    let group = store
        .get_group(id)?
        .ok_or_else(|| RuntimeError::Store(StoreError::GroupNotFound(id.to_string())))?;
    let Some(connection_id) = group.connection_id else {
        return Ok("synthetic".into());
    };
    let connection = store
        .get_connection(&connection_id)?
        .ok_or_else(|| RuntimeError::Store(StoreError::ConnectionNotFound(connection_id)))?;
    Ok(connection.kind)
}

fn exit_reason(result: &Result<RuntimeResult<GroupExit>, tokio::task::JoinError>) -> StopReason {
    match result {
        Ok(Ok(GroupExit::Paused)) => StopReason::Paused,
        Ok(Ok(GroupExit::Drained)) => StopReason::Drained,
        Ok(Ok(GroupExit::Shutdown)) => StopReason::Shutdown,
        Ok(Err(RuntimeError::Source(SourceError::Transient(text)))) => {
            StopReason::SourceUnavailable(text.clone())
        }
        Ok(Err(RuntimeError::Source(SourceError::Contract(text)))) => {
            StopReason::BadData(text.clone())
        }
        Ok(Err(err)) => StopReason::Error(err.to_string()),
        Err(err) if err.is_panic() => StopReason::Panicked,
        Err(_) => StopReason::Aborted,
    }
}
