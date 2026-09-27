use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use crate::core::{GroupId, GroupLifecycle};
use crate::observe::GaugeSample;
use crate::runtime::{GroupSupervisor, RuntimeError, SourceFactory, SourceOpen};
use crate::store::{
    ConnectionRecord, DurableGroup, RedbStore, StateStore, StoreError, StoreKey, open_secret,
    seal_secret,
};

use super::dto::{
    CheckpointView, ConnectionCreateRequest, ConnectionView, ConsumersView, DiagnosticsView,
    GroupCreateRequest, GroupView, StatusView, group_config_from_create,
};
use super::error::{ControlError, ControlResult};

pub const MAX_SECRET_BYTES: usize = 16 * 1024;
/// How long a metrics scrape waits for one group's live numbers.
const SCRAPE_WAIT: Duration = Duration::from_millis(250);
pub const MAX_CONFIG_JSON_BYTES: usize = 64 * 1024;

/// Control-plane facade over store + supervisor.
pub struct ControlService {
    store: Arc<RedbStore>,
    key: StoreKey,
    supervisor: Arc<Mutex<GroupSupervisor<RedbStore>>>,
    bind: String,
    /// The key lives only in this process. Secrets sealed with it could not
    /// be opened after a restart, so connection create is refused.
    ephemeral_key: bool,
}

impl ControlService {
    pub fn new(store: Arc<RedbStore>, key: StoreKey, bind: impl Into<String>) -> Self {
        let supervisor = GroupSupervisor::new(Arc::clone(&store));
        Self {
            store,
            key,
            supervisor: Arc::new(Mutex::new(supervisor)),
            bind: bind.into(),
            ephemeral_key: false,
        }
    }

    /// Mark the store key as temporary. Connection create is then refused.
    pub fn with_ephemeral_key(mut self) -> Self {
        self.ephemeral_key = true;
        self
    }

    pub fn supervisor(&self) -> Arc<Mutex<GroupSupervisor<RedbStore>>> {
        Arc::clone(&self.supervisor)
    }

    pub fn store(&self) -> &Arc<RedbStore> {
        &self.store
    }

    /// Timing for group runtimes started from now on.
    pub async fn set_runtime_config(&self, config: crate::runtime::GroupRuntimeConfig) {
        self.supervisor.lock().await.set_runtime_config(config);
    }

    pub async fn install_source_factory(&self, factory: Arc<dyn SourceFactory>) {
        self.supervisor
            .lock()
            .await
            .set_source_factory(factory, self.key.clone());
    }

    /// Collect finished groups and run the restarts that are due. Sources are
    /// opened without holding the supervisor lock.
    pub async fn supervise_once(&self) -> ControlResult<Vec<GroupId>> {
        let plans = self.supervisor.lock().await.reap();
        let mut recovered = Vec::new();
        for plan in plans {
            let opened = plan.open().await;
            if let Ok(handle) = self.supervisor.lock().await.finish_start(opened) {
                recovered.push(handle.group_id.clone());
            }
        }
        Ok(recovered)
    }

    /// Start the groups that were running or draining when the process last
    /// stopped. A group whose source cannot open yet is retried with backoff.
    pub async fn resume_groups(&self) {
        let groups = match self.store.list_groups() {
            Ok(groups) => groups,
            Err(err) => {
                tracing::warn!(error = %err, "could not list groups to resume");
                return;
            }
        };
        for group in groups {
            if !matches!(
                group.lifecycle,
                GroupLifecycle::Running | GroupLifecycle::Draining
            ) {
                continue;
            }
            let id = group.group_id().clone();
            match self.start_group(id.as_str()).await {
                Ok(_) => tracing::info!(group_id = %id, "group resumed"),
                Err(err) => {
                    tracing::warn!(group_id = %id, error = %err, "group did not resume; retrying");
                    self.supervisor
                        .lock()
                        .await
                        .retry_later(&id, err.to_string());
                }
            }
        }
    }

    /// Save every running group's progress and stop supervising. Lifecycles
    /// are kept, so the groups resume at the next start.
    pub async fn shutdown_groups(&self) {
        self.supervisor.lock().await.shutdown_all().await;
    }

    pub fn ready(&self) -> ControlResult<()> {
        self.store.list_groups()?;
        Ok(())
    }

    pub async fn encode_metrics(&self) -> String {
        let (observe, handles) = {
            let sup = self.supervisor.lock().await;
            (sup.observe(), sup.running_handles())
        };
        // Ask every group at once, and skip a group that does not answer in
        // time rather than stall the scrape.
        let snapshots = futures::future::join_all(
            handles
                .iter()
                .map(|handle| tokio::time::timeout(SCRAPE_WAIT, handle.live_snapshot())),
        )
        .await;
        let mut samples = Vec::with_capacity(handles.len());
        for (handle, snapshot) in handles.iter().zip(snapshots) {
            if let Ok(Ok(snap)) = snapshot {
                samples.push(GaugeSample {
                    group_id: handle.group_id.as_str().to_string(),
                    buffer_records: snap.buffer_records as u64,
                    buffer_bytes: snap.buffer_bytes as u64,
                    inflight_records: snap.inflight_records as u64,
                    checkpoint_lag: (snap.buffer_records + snap.inflight_records) as u64,
                    consumer_count: snap.consumers.len() as u64,
                });
            }
        }
        observe.render(handles.len(), &samples)
    }

    pub async fn status(&self) -> StatusView {
        let running = {
            let mut sup = self.supervisor.lock().await;
            sup.collect_finished();
            sup.list_running()
        }
        .into_iter()
        .map(|g| g.to_string())
        .collect();
        StatusView {
            version: crate::VERSION.to_string(),
            schema_version: self.store.schema_version(),
            running_groups: running,
            bind: self.bind.clone(),
        }
    }

    pub fn create_connection(&self, req: ConnectionCreateRequest) -> ControlResult<ConnectionView> {
        validate_connection_create(&req)?;
        if self.ephemeral_key {
            return Err(ControlError::Conflict(
                "the server runs with a temporary store key; set DIAVASI_STORE_KEY so connection secrets survive a restart".into(),
            ));
        }
        if self.store.get_connection(&req.id)?.is_some() {
            return Err(ControlError::Conflict(format!(
                "connection already exists: {}",
                req.id
            )));
        }
        let sealed = seal_secret(&self.key, req.secret.as_bytes())?;
        let record = ConnectionRecord {
            id: req.id.clone(),
            kind: req.kind,
            config_json: req.config_json,
            sealed_secret: sealed,
        };
        self.store.put_connection(&record)?;
        Ok(connection_view(&record))
    }

    pub fn list_connections(&self) -> ControlResult<Vec<ConnectionView>> {
        Ok(self
            .store
            .list_connections()?
            .iter()
            .map(connection_view)
            .collect())
    }

    pub fn get_connection(&self, id: &str) -> ControlResult<ConnectionView> {
        let rec = self
            .store
            .get_connection(id)?
            .ok_or_else(|| ControlError::NotFound(format!("connection not found: {id}")))?;
        Ok(connection_view(&rec))
    }

    /// Delete a connection that no group uses. A group bound to a missing
    /// connection could not start, so a connection in use is a conflict.
    pub fn delete_connection(&self, id: &str) -> ControlResult<()> {
        if self.store.get_connection(id)?.is_none() {
            return Err(ControlError::NotFound(format!(
                "connection not found: {id}"
            )));
        }
        let users: Vec<String> = self
            .store
            .list_groups()?
            .into_iter()
            .filter(|group| group.connection_id.as_deref() == Some(id))
            .map(|group| group.group_id().to_string())
            .collect();
        if !users.is_empty() {
            return Err(ControlError::Conflict(format!(
                "connection {id} is used by groups: {}",
                users.join(", ")
            )));
        }
        self.store.delete_connection(id)?;
        Ok(())
    }

    pub async fn create_group(&self, req: GroupCreateRequest) -> ControlResult<GroupView> {
        if req.ordering_contract.len() > 1024 {
            return Err(ControlError::BadRequest(
                "ordering_contract too long".into(),
            ));
        }
        let config = group_config_from_create(&req).map_err(ControlError::BadRequest)?;
        // Fast answer before validating the source. `insert_group` below is
        // what makes the id unique under concurrent creates.
        if self.store.get_group(&config.group_id)?.is_some() {
            return Err(StoreError::GroupExists(req.group_id).into());
        }
        if let Some(cid) = &req.connection_id {
            let connection = self
                .store
                .get_connection(cid)?
                .ok_or_else(|| ControlError::NotFound(format!("connection not found: {cid}")))?;
            let source_spec = req.source_spec.clone().ok_or_else(|| {
                ControlError::BadRequest(format!(
                    "{} connection requires source_spec",
                    connection.kind
                ))
            })?;
            let factory = self
                .supervisor
                .lock()
                .await
                .source_factory()
                .ok_or_else(|| {
                    ControlError::BadRequest("source factory is not installed".into())
                })?;
            if !factory.supports(&connection.kind) {
                return Err(ControlError::BadRequest(format!(
                    "unsupported connection kind {}",
                    connection.kind
                )));
            }
            let secret = open_secret(&self.key, &connection.sealed_secret)?;
            factory
                .validate(SourceOpen {
                    connection,
                    source_spec,
                    secret,
                })
                .await
                .map_err(ControlError::BadRequest)?;
        } else if req.source_spec.is_some() {
            return Err(ControlError::BadRequest(
                "source_spec requires connection_id".into(),
            ));
        }
        let g = DurableGroup::create_with_source(
            Arc::clone(&self.store),
            config,
            req.ordering_contract.clone(),
            req.connection_id.clone(),
            req.source_spec.clone(),
        )?;
        drop(g);
        self.group_view_from_store(&req.group_id, false)
    }

    pub async fn list_groups(&self) -> ControlResult<Vec<GroupView>> {
        let records = self.store.list_groups()?;
        let mut sup = self.supervisor.lock().await;
        sup.collect_finished();
        Ok(records
            .iter()
            .map(|rec| group_view(rec, presence(&sup, rec.group_id())))
            .collect())
    }

    pub async fn get_group(&self, id: &str) -> ControlResult<GroupView> {
        let gid = GroupId::new(id).map_err(|e| ControlError::BadRequest(e.to_string()))?;
        let rec = self
            .store
            .get_group(&gid)?
            .ok_or_else(|| ControlError::NotFound(format!("group not found: {id}")))?;
        let mut sup = self.supervisor.lock().await;
        sup.collect_finished();
        Ok(group_view(&rec, presence(&sup, &gid)))
    }

    fn group_view_from_store(&self, id: &str, running: bool) -> ControlResult<GroupView> {
        let gid = GroupId::new(id).map_err(|e| ControlError::BadRequest(e.to_string()))?;
        let rec = self
            .store
            .get_group(&gid)?
            .ok_or_else(|| ControlError::NotFound(format!("group not found: {id}")))?;
        let presence = if running {
            Presence::Running
        } else {
            Presence::Idle
        };
        Ok(group_view(&rec, presence))
    }

    pub async fn delete_group(&self, id: &str) -> ControlResult<()> {
        let gid = GroupId::new(id).map_err(|e| ControlError::BadRequest(e.to_string()))?;
        // Hold the supervisor across the check and the delete so a start
        // cannot slip in between and run a group whose record is gone.
        let mut supervisor = self.supervisor.lock().await;
        if supervisor.is_active(&gid) {
            return Err(ControlError::Conflict(format!(
                "group is running; pause it before delete: {id}"
            )));
        }
        supervisor.cancel_retry(&gid);
        if self.store.get_group(&gid)?.is_none() {
            return Err(ControlError::NotFound(format!("group not found: {id}")));
        }
        let adapter = self.adapter_label(&gid)?;
        self.store.delete_group(&gid)?;
        supervisor.observe().forget_group(gid.as_str(), &adapter);
        drop(supervisor);
        Ok(())
    }

    pub async fn start_group(&self, id: &str) -> ControlResult<GroupView> {
        let gid = GroupId::new(id).map_err(|e| ControlError::BadRequest(e.to_string()))?;
        let plan = match self.supervisor.lock().await.plan_start(&gid) {
            Ok(plan) => Some(plan),
            Err(RuntimeError::GroupAlreadyRunning(_)) => None,
            Err(e) => return Err(e.into()),
        };
        if let Some(plan) = plan {
            // Opening the source may take seconds; other requests proceed.
            let opened = plan.open().await;
            self.supervisor.lock().await.finish_start(opened)?;
        }
        self.get_group(id).await
    }

    pub async fn pause_group(&self, id: &str) -> ControlResult<GroupView> {
        let gid = GroupId::new(id).map_err(|e| ControlError::BadRequest(e.to_string()))?;
        let handle = self.supervisor.lock().await.begin_stop(&gid)?;
        if let Some(handle) = handle {
            handle.stop().await?;
            let join = self.supervisor.lock().await.finish_stop(&gid);
            if let Some(join) = join {
                let _ = join.await;
            }
        }
        self.get_group(id).await
    }

    pub async fn resume_group(&self, id: &str) -> ControlResult<GroupView> {
        self.start_group(id).await
    }

    pub async fn drain_group(&self, id: &str) -> ControlResult<GroupView> {
        let gid = GroupId::new(id).map_err(|e| ControlError::BadRequest(e.to_string()))?;
        let handle = self
            .supervisor
            .lock()
            .await
            .get_handle(&gid)
            .ok_or_else(|| ControlError::Conflict(format!("group is not running: {id}")))?;
        handle.drain().await?;
        self.get_group(id).await
    }

    pub async fn list_consumers(&self, id: &str) -> ControlResult<ConsumersView> {
        let gid = GroupId::new(id).map_err(|e| ControlError::BadRequest(e.to_string()))?;
        let handle = self.supervisor.lock().await.get_handle(&gid);
        let consumers = match handle {
            Some(h) => h
                .list_consumers()
                .await?
                .into_iter()
                .map(|c| c.to_string())
                .collect(),
            None => Vec::new(),
        };
        Ok(ConsumersView {
            group_id: id.to_string(),
            consumers,
        })
    }

    pub async fn diagnostics(&self, id: &str) -> ControlResult<DiagnosticsView> {
        let gid = GroupId::new(id).map_err(|e| ControlError::BadRequest(e.to_string()))?;
        let rec = self
            .store
            .get_group(&gid)?
            .ok_or_else(|| ControlError::NotFound(format!("group not found: {id}")))?;
        let adapter = self.adapter_label(&gid)?;
        let (handle, outcome, observe, presence) = {
            let mut sup = self.supervisor.lock().await;
            sup.collect_finished();
            (
                sup.get_handle(&gid),
                sup.outcome(&gid),
                sup.observe(),
                presence(&sup, &gid),
            )
        };
        let counters = observe.counters(gid.as_str(), &adapter);
        let live = match handle {
            Some(handle) => handle.live_snapshot().await.ok(),
            None => None,
        };
        let running = live.is_some();
        let checkpoint_lag = live
            .as_ref()
            .map(|snap| (snap.buffer_records + snap.inflight_records) as u64)
            .unwrap_or(0);
        let committed_cursor = match &live {
            Some(snap) => snap.committed.clone(),
            None => self.store.load_checkpoint(&gid)?,
        };
        Ok(DiagnosticsView {
            group_id: id.to_string(),
            running,
            lifecycle: live
                .as_ref()
                .map(|snap| snap.lifecycle)
                .unwrap_or_else(|| view_lifecycle(rec.lifecycle, presence)),
            committed_cursor,
            fetched_cursor: live
                .as_ref()
                .map(|snap| snap.fetched.clone())
                .unwrap_or(None),
            buffer_records: live
                .as_ref()
                .map(|snap| snap.buffer_records as u64)
                .unwrap_or(0),
            buffer_bytes: live
                .as_ref()
                .map(|snap| snap.buffer_bytes as u64)
                .unwrap_or(0),
            inflight_records: live
                .as_ref()
                .map(|snap| snap.inflight_records as u64)
                .unwrap_or(0),
            consumers: live
                .map(|snap| {
                    snap.consumers
                        .into_iter()
                        .map(|consumer| consumer.to_string())
                        .collect()
                })
                .unwrap_or_default(),
            records_fetched: counters.records_fetched,
            records_delivered: counters.records_delivered,
            records_acked: counters.records_acked,
            records_replayed: counters.records_replayed,
            bytes: counters.bytes,
            checkpoint_lag,
            restarts: counters.restarts,
            consumer_disconnects: counters.consumer_disconnects,
            adapter_errors: counters.adapter_errors,
            last_stop_reason: outcome
                .as_ref()
                .map(|outcome| outcome.last_stop_reason.clone()),
            recovered: outcome.map(|outcome| outcome.recovered).unwrap_or(false),
        })
    }

    /// The `adapter` metric label: the connection kind, or `synthetic`.
    fn adapter_label(&self, id: &GroupId) -> ControlResult<String> {
        let Some(rec) = self.store.get_group(id)? else {
            return Ok("unknown".into());
        };
        Ok(match &rec.connection_id {
            None => "synthetic".to_string(),
            Some(connection_id) => self
                .store
                .get_connection(connection_id)?
                .map(|connection| connection.kind)
                .unwrap_or_else(|| "unknown".to_string()),
        })
    }

    pub async fn checkpoint(&self, id: &str) -> ControlResult<CheckpointView> {
        let gid = GroupId::new(id).map_err(|e| ControlError::BadRequest(e.to_string()))?;
        if self.store.get_group(&gid)?.is_none() {
            return Err(ControlError::NotFound(format!("group not found: {id}")));
        }
        let durable_cursor = self.store.load_checkpoint(&gid)?;
        let handle = self.supervisor.lock().await.get_handle(&gid);
        let live_cursor = match handle {
            Some(h) => Some(h.snapshot_cursor().await?),
            None => None,
        };
        Ok(CheckpointView {
            group_id: id.to_string(),
            durable_cursor,
            live_cursor,
        })
    }

    /// Join a consumer and ack up to `acks` batches.
    #[cfg(test)]
    pub async fn advance_for_test(
        &self,
        id: &str,
        consumer: &str,
        acks: usize,
    ) -> ControlResult<()> {
        let gid = GroupId::new(id).map_err(|e| ControlError::BadRequest(e.to_string()))?;
        let cid = crate::core::ConsumerId::new(consumer)
            .map_err(|e| ControlError::BadRequest(e.to_string()))?;
        let handle = {
            let sup = self.supervisor.lock().await;
            sup.get_handle(&gid)
                .ok_or_else(|| ControlError::Conflict(format!("group is not running: {id}")))?
        };
        handle.join(cid.clone()).await?;
        let mut done = 0;
        for _ in 0..500 {
            if done >= acks {
                break;
            }
            match handle.assign(&cid).await {
                Ok(batch) => {
                    handle.ack(batch.id).await?;
                    done += 1;
                }
                Err(RuntimeError::Core(crate::core::CoreError::NoWork)) => {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}

fn connection_view(rec: &ConnectionRecord) -> ConnectionView {
    ConnectionView {
        id: rec.id.clone(),
        kind: rec.kind.clone(),
        config_json: rec.config_json.clone(),
        secret_sealed: true,
    }
}

/// Where a group is, as the supervisor sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Presence {
    Running,
    /// Failed and waiting for its next restart.
    Retrying,
    Idle,
}

fn presence(sup: &GroupSupervisor<RedbStore>, id: &GroupId) -> Presence {
    if sup.is_active(id) {
        Presence::Running
    } else if sup.is_retrying(id) {
        Presence::Retrying
    } else {
        Presence::Idle
    }
}

/// The lifecycle to report, so it agrees with `running`. The owner records
/// `Running` or `Draining` when it starts or drains; until then, and after a
/// process restart, the stored value can lag the supervisor.
fn view_lifecycle(stored: GroupLifecycle, presence: Presence) -> GroupLifecycle {
    match presence {
        Presence::Running if stored == GroupLifecycle::Draining => GroupLifecycle::Draining,
        Presence::Running => GroupLifecycle::Running,
        Presence::Retrying => GroupLifecycle::Recovering,
        Presence::Idle => match stored {
            GroupLifecycle::Running
            | GroupLifecycle::Draining
            | GroupLifecycle::Starting
            | GroupLifecycle::Recovering => GroupLifecycle::Stopped,
            other => other,
        },
    }
}

fn group_view(rec: &crate::store::GroupRecord, presence: Presence) -> GroupView {
    GroupView {
        group_id: rec.group_id().as_str().to_string(),
        total_records: rec.config.total_records,
        payload_size: rec.config.payload_size,
        max_buffer_records: rec.config.max_buffer_records,
        max_buffer_bytes: rec.config.max_buffer_bytes,
        batch_max_records: rec.config.batch_max_records,
        batch_timeout_ms: rec.config.batch_timeout.as_millis() as u64,
        ordering_contract: rec.ordering_contract.clone(),
        connection_id: rec.connection_id.clone(),
        lifecycle: view_lifecycle(rec.lifecycle, presence),
        next_batch_id: rec.next_batch_id,
        running: presence == Presence::Running,
    }
}

fn validate_connection_create(req: &ConnectionCreateRequest) -> ControlResult<()> {
    super::dto::check_id("connection id", &req.id).map_err(ControlError::BadRequest)?;
    if req.kind.is_empty() || req.kind.len() > 64 {
        return Err(ControlError::BadRequest("invalid connection kind".into()));
    }
    if req.secret.is_empty() || req.secret.len() > MAX_SECRET_BYTES {
        return Err(ControlError::BadRequest("invalid secret length".into()));
    }
    let cfg_bytes = serde_json::to_vec(&req.config_json)
        .map_err(|e| ControlError::BadRequest(e.to_string()))?;
    if cfg_bytes.len() > MAX_CONFIG_JSON_BYTES {
        return Err(ControlError::BadRequest("config_json too large".into()));
    }
    Ok(())
}
