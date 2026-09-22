use std::collections::HashMap;
use std::sync::Arc;

use tokio::task::{AbortHandle, JoinHandle};

use crate::core::GroupId;
use crate::store::{DurableGroup, StateStore};

use super::error::{RuntimeError, RuntimeResult};
use super::group_runtime::{
    GroupRuntimeConfig, SpawnedGroup, map_join_result, spawn_group_runtime,
};
use super::handle::GroupHandle;

struct RunningGroup {
    handle: GroupHandle,
    abort: AbortHandle,
    join: JoinHandle<RuntimeResult<()>>,
    /// Set when [`GroupSupervisor::stop_group`] requested a clean shutdown.
    clean_stop: bool,
}

/// Supervises many independent group runtimes over one shared [`StateStore`].
pub struct GroupSupervisor<S: StateStore> {
    store: Arc<S>,
    runtime_config: GroupRuntimeConfig,
    groups: HashMap<String, RunningGroup>,
}

impl<S: StateStore + 'static> GroupSupervisor<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            runtime_config: GroupRuntimeConfig::default(),
            groups: HashMap::new(),
        }
    }

    pub fn with_runtime_config(mut self, config: GroupRuntimeConfig) -> Self {
        self.runtime_config = config;
        self
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
        let durable = DurableGroup::open(Arc::clone(&self.store), id)?;
        // DurableGroup::open uses recover_from -> Running already.
        let spawned = spawn_group_runtime(durable, self.runtime_config.clone());
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
        map_join_result(running.join.await)
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
            let join_res = map_join_result(running.join.await);
            if clean_stop {
                // Clean stop should already have been awaited in stop_group; if
                // we observe it here, just drop.
                let _ = join_res;
                continue;
            }
            // Unexpected exit (abort / panic / error): recover from store.
            let _ = join_res;
            let durable = DurableGroup::open(Arc::clone(&self.store), &id)?;
            let spawned = spawn_group_runtime(durable, self.runtime_config.clone());
            recovered.push(spawned.handle.group_id.clone());
            self.insert_spawned(spawned);
        }
        Ok(recovered)
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
