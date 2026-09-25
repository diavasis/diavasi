use std::collections::HashMap;
use std::sync::Arc;

use tokio::task::{AbortHandle, JoinHandle};

use crate::core::{GroupId, RecordSource};
use crate::observe::Observe;
use crate::store::{DurableGroup, StateStore, StoreKey, open_secret};

use super::error::{RuntimeError, RuntimeResult};
use super::group_runtime::{
    GroupRuntimeConfig, SpawnedGroup, map_join_result, spawn_group_runtime,
};
use super::handle::GroupHandle;
use super::source_factory::{SourceFactory, SourceOpen};

struct RunningGroup {
    handle: GroupHandle,
    abort: AbortHandle,
    join: JoinHandle<RuntimeResult<()>>,
    /// Set when [`GroupSupervisor::stop_group`] requested a clean shutdown.
    clean_stop: bool,
}

/// Why the last group task exited, and whether the supervisor respawned it.
/// Process-local. A process restart clears it. The durable lifecycle and
/// checkpoint remain in the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupOutcome {
    pub last_stop_reason: String,
    pub recovered: bool,
}

/// Supervises many independent group runtimes over one shared [`StateStore`].
pub struct GroupSupervisor<S: StateStore> {
    store: Arc<S>,
    runtime_config: GroupRuntimeConfig,
    groups: HashMap<String, RunningGroup>,
    source_factory: Option<Arc<dyn SourceFactory>>,
    store_key: Option<StoreKey>,
    observe: Observe,
    outcomes: HashMap<String, GroupOutcome>,
}

impl<S: StateStore + 'static> GroupSupervisor<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            runtime_config: GroupRuntimeConfig::default(),
            groups: HashMap::new(),
            source_factory: None,
            store_key: None,
            observe: Observe::new(),
            outcomes: HashMap::new(),
        }
    }

    pub fn observe(&self) -> Observe {
        self.observe.clone()
    }

    pub fn outcome(&self, id: &GroupId) -> Option<GroupOutcome> {
        self.outcomes.get(id.as_str()).cloned()
    }

    pub fn running_handles(&self) -> Vec<GroupHandle> {
        self.groups.values().map(|g| g.handle.clone()).collect()
    }

    pub fn with_runtime_config(mut self, config: GroupRuntimeConfig) -> Self {
        self.runtime_config = config;
        self
    }

    pub fn set_source_factory(&mut self, factory: Arc<dyn SourceFactory>, key: StoreKey) {
        self.source_factory = Some(factory);
        self.store_key = Some(key);
    }

    pub fn source_factory(&self) -> Option<Arc<dyn SourceFactory>> {
        self.source_factory.clone()
    }

    pub fn list_running(&self) -> Vec<GroupId> {
        self.groups
            .values()
            .map(|g| g.handle.group_id.clone())
            .collect()
    }

    pub fn get_handle(&self, id: &GroupId) -> Option<GroupHandle> {
        self.groups.get(id.as_str()).map(|g| g.handle.clone())
    }

    /// Open the group from the store (recover path) and spawn its runtime.
    pub async fn start_group(&mut self, id: &GroupId) -> RuntimeResult<GroupHandle> {
        if self.groups.contains_key(id.as_str()) {
            return Err(RuntimeError::GroupAlreadyRunning(id.to_string()));
        }
        if let Some(outcome) = self.outcomes.get_mut(id.as_str()) {
            outcome.recovered = false;
        }
        let durable = DurableGroup::open(Arc::clone(&self.store), id)?;
        let adapter = self.adapter_label(id)?;
        let source = self.open_source(id).await?;
        let spawned = spawn_group_runtime(
            durable,
            self.runtime_config.clone(),
            source,
            self.observe.clone(),
            adapter,
        );
        let handle = spawned.handle.clone();
        self.insert_spawned(spawned);
        Ok(handle)
    }

    /// Graceful stop: ask the owner to snapshot, then await exit (no respawn).
    pub async fn stop_group(&mut self, id: &GroupId) -> RuntimeResult<()> {
        let Some(running) = self.groups.get_mut(id.as_str()) else {
            return Err(RuntimeError::GroupNotRunning(id.to_string()));
        };
        running.clean_stop = true;
        let handle = running.handle.clone();
        handle.stop().await?;
        let running = self.groups.remove(id.as_str()).expect("just checked");
        let result = map_join_result(running.join.await);
        if result.is_ok() {
            let recovered = self
                .outcomes
                .get(id.as_str())
                .map(|outcome| outcome.recovered)
                .unwrap_or(false);
            self.outcomes.insert(
                id.as_str().to_string(),
                GroupOutcome {
                    last_stop_reason: "paused".into(),
                    recovered,
                },
            );
        }
        result
    }

    /// Hard-kill the group task tree (test / fault injection). Leaves a pending
    /// recovery opportunity for [`supervise_once`].
    pub fn abort_group(&mut self, id: &GroupId) -> RuntimeResult<()> {
        let Some(running) = self.groups.get_mut(id.as_str()) else {
            return Err(RuntimeError::GroupNotRunning(id.to_string()));
        };
        running.clean_stop = false;
        running.abort.abort();
        Ok(())
    }

    /// Poll finished group tasks. Unexpected exits are recovered from the store
    /// and respawned. Clean stops are removed without respawn.
    pub async fn supervise_once(&mut self) -> RuntimeResult<Vec<GroupId>> {
        let finished: Vec<(String, bool)> = {
            let mut out = Vec::new();
            for (key, running) in &self.groups {
                if running.join.is_finished() {
                    out.push((key.clone(), running.clean_stop));
                }
            }
            out
        };

        let mut recovered = Vec::new();
        for (key, clean_stop) in finished {
            let Some(running) = self.groups.remove(&key) else {
                continue;
            };
            let id = running.handle.group_id.clone();
            let join_res = running.join.await;
            if clean_stop {
                // Clean stop should already have been awaited in stop_group; if
                // we observe it here, just drop.
                let _ = join_res;
                continue;
            }
            // Unexpected exit (abort / panic / error): recover from store.
            let reason = exit_reason(join_res);
            self.observe.record_restart(id.as_str());
            tracing::warn!(group_id = %id, reason = %reason, "group restarted");
            self.outcomes.insert(
                id.as_str().to_string(),
                GroupOutcome {
                    last_stop_reason: reason,
                    recovered: true,
                },
            );
            let durable = DurableGroup::open(Arc::clone(&self.store), &id)?;
            let adapter = self.adapter_label(&id)?;
            let source = self.open_source(&id).await?;
            let spawned = spawn_group_runtime(
                durable,
                self.runtime_config.clone(),
                source,
                self.observe.clone(),
                adapter,
            );
            recovered.push(spawned.handle.group_id.clone());
            self.insert_spawned(spawned);
        }
        Ok(recovered)
    }

    async fn open_source(&self, id: &GroupId) -> RuntimeResult<Option<Box<dyn RecordSource>>> {
        let group = self.store.get_group(id)?.ok_or_else(|| {
            RuntimeError::Store(crate::store::StoreError::GroupNotFound(id.to_string()))
        })?;
        let Some(connection_id) = group.connection_id else {
            return Ok(None);
        };
        let connection = self.store.get_connection(&connection_id)?.ok_or_else(|| {
            RuntimeError::Store(crate::store::StoreError::ConnectionNotFound(
                connection_id.clone(),
            ))
        })?;
        let Some(source_spec) = group.source_spec else {
            return Err(RuntimeError::Source(format!(
                "{} connection requires source_spec",
                connection.kind
            )));
        };
        let factory = self
            .source_factory
            .as_ref()
            .ok_or_else(|| RuntimeError::Source("source factory is not installed".into()))?;
        if !factory.supports(&connection.kind) {
            return Err(RuntimeError::Source(format!(
                "unsupported connection kind {}",
                connection.kind
            )));
        }
        let key = self
            .store_key
            .as_ref()
            .ok_or_else(|| RuntimeError::Source("store key is not installed".into()))?;
        let secret = open_secret(key, &connection.sealed_secret)?;
        let source = factory
            .open(SourceOpen {
                connection,
                source_spec,
                secret,
            })
            .await
            .map_err(RuntimeError::Source)?;
        Ok(Some(source))
    }

    fn adapter_label(&self, id: &GroupId) -> RuntimeResult<String> {
        let group = self.store.get_group(id)?.ok_or_else(|| {
            RuntimeError::Store(crate::store::StoreError::GroupNotFound(id.to_string()))
        })?;
        let Some(connection_id) = group.connection_id else {
            return Ok("synthetic".into());
        };
        let connection = self.store.get_connection(&connection_id)?.ok_or_else(|| {
            RuntimeError::Store(crate::store::StoreError::ConnectionNotFound(
                connection_id.clone(),
            ))
        })?;
        Ok(connection.kind)
    }

    /// Spawn with an explicit source. Failure-injection tests use this to
    /// install a source that errors. A later recovery opens the stored source.
    #[cfg(test)]
    pub async fn start_group_with_source(
        &mut self,
        id: &GroupId,
        source: Option<Box<dyn RecordSource>>,
    ) -> RuntimeResult<GroupHandle> {
        if self.groups.contains_key(id.as_str()) {
            return Err(RuntimeError::GroupAlreadyRunning(id.to_string()));
        }
        let durable = DurableGroup::open(Arc::clone(&self.store), id)?;
        let adapter = self.adapter_label(id)?;
        let spawned = spawn_group_runtime(
            durable,
            self.runtime_config.clone(),
            source,
            self.observe.clone(),
            adapter,
        );
        let handle = spawned.handle.clone();
        self.insert_spawned(spawned);
        Ok(handle)
    }

    fn insert_spawned(&mut self, spawned: SpawnedGroup) {
        let key = spawned.handle.group_id.as_str().to_string();
        self.groups.insert(
            key,
            RunningGroup {
                handle: spawned.handle,
                abort: spawned.abort,
                join: spawned.join,
                clean_stop: false,
            },
        );
    }
}

fn exit_reason(result: Result<RuntimeResult<()>, tokio::task::JoinError>) -> String {
    match result {
        Ok(Ok(())) => "task exited".into(),
        Ok(Err(RuntimeError::Source(message))) => message,
        Ok(Err(err)) => err.to_string(),
        Err(err) if err.is_panic() => "task panicked".into(),
        Err(_) => "task aborted".into(),
    }
}
