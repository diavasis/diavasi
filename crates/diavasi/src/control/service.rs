use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use crate::core::{ConsumerId, GroupId};
use crate::runtime::{GroupSupervisor, RuntimeError, SourceFactory, SourceOpen};
use crate::store::{
    ConnectionRecord, DurableGroup, RedbStore, StateStore, StoreKey, open_secret, seal_secret,
};

use super::dto::{
    CheckpointView, ConnectionCreateRequest, ConnectionView, ConsumersView, GroupCreateRequest,
    GroupView, StatusView, group_config_from_create,
};
use super::error::{ControlError, ControlResult};

pub const MAX_SECRET_BYTES: usize = 16 * 1024;
pub const MAX_CONFIG_JSON_BYTES: usize = 64 * 1024;

/// Control-plane facade over store + supervisor.
pub struct ControlService {
    store: Arc<RedbStore>,
    key: StoreKey,
    supervisor: Arc<Mutex<GroupSupervisor<RedbStore>>>,
    bind: String,
}

impl ControlService {
    pub fn new(store: Arc<RedbStore>, key: StoreKey, bind: impl Into<String>) -> Self {
        let supervisor = GroupSupervisor::new(Arc::clone(&store));
        Self {
            store,
            key,
            supervisor: Arc::new(Mutex::new(supervisor)),
            bind: bind.into(),
        }
    }

    pub fn supervisor(&self) -> Arc<Mutex<GroupSupervisor<RedbStore>>> {
        Arc::clone(&self.supervisor)
    }

    pub fn store(&self) -> &Arc<RedbStore> {
        &self.store
    }

    pub async fn install_source_factory(&self, factory: Arc<dyn SourceFactory>) {
        self.supervisor
            .lock()
            .await
            .set_source_factory(factory, self.key.clone());
    }

    pub async fn supervise_once(&self) -> ControlResult<Vec<GroupId>> {
        let mut sup = self.supervisor.lock().await;
        Ok(sup.supervise_once().await?)
    }

    pub async fn status(&self) -> StatusView {
        let running = self
            .supervisor
            .lock()
            .await
            .list_running()
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

    pub fn delete_connection(&self, id: &str) -> ControlResult<()> {
        if self.store.get_connection(id)?.is_none() {
            return Err(ControlError::NotFound(format!(
                "connection not found: {id}"
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
        if self.store.get_group(&config.group_id)?.is_some() {
            return Err(ControlError::Conflict(format!(
                "group already exists: {}",
                req.group_id
            )));
        }
        if let Some(cid) = &req.connection_id {
            let connection = self
                .store
                .get_connection(cid)?
                .ok_or_else(|| ControlError::NotFound(format!("connection not found: {cid}")))?;
            if connection.kind == "postgres" {
                let source_spec = req.source_spec.clone().ok_or_else(|| {
                    ControlError::BadRequest("postgres connection requires source_spec".into())
                })?;
                let factory = self
                    .supervisor
                    .lock()
                    .await
                    .source_factory()
                    .ok_or_else(|| {
                        ControlError::BadRequest("postgres source factory is not installed".into())
                    })?;
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
                    "source_spec requires a postgres connection".into(),
                ));
            }
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
        let running: std::collections::HashSet<_> = self
            .supervisor
            .lock()
            .await
            .list_running()
            .into_iter()
            .map(|g| g.to_string())
            .collect();
        let mut out = Vec::new();
        for rec in self.store.list_groups()? {
            let id = rec.group_id().as_str().to_string();
            out.push(group_view(&rec, running.contains(&id)));
        }
        Ok(out)
    }

    pub async fn get_group(&self, id: &str) -> ControlResult<GroupView> {
        let gid = GroupId::new(id).map_err(|e| ControlError::BadRequest(e.to_string()))?;
        let rec = self
            .store
            .get_group(&gid)?
            .ok_or_else(|| ControlError::NotFound(format!("group not found: {id}")))?;
        let running = self.supervisor.lock().await.get_handle(&gid).is_some();
        Ok(group_view(&rec, running))
    }

    fn group_view_from_store(&self, id: &str, running: bool) -> ControlResult<GroupView> {
        let gid = GroupId::new(id).map_err(|e| ControlError::BadRequest(e.to_string()))?;
        let rec = self
            .store
            .get_group(&gid)?
            .ok_or_else(|| ControlError::NotFound(format!("group not found: {id}")))?;
        Ok(group_view(&rec, running))
    }

    pub async fn delete_group(&self, id: &str) -> ControlResult<()> {
        let gid = GroupId::new(id).map_err(|e| ControlError::BadRequest(e.to_string()))?;
        if self.supervisor.lock().await.get_handle(&gid).is_some() {
            return Err(ControlError::Conflict(format!(
                "group is running; pause it before delete: {id}"
            )));
        }
        if self.store.get_group(&gid)?.is_none() {
            return Err(ControlError::NotFound(format!("group not found: {id}")));
        }
        self.store.delete_group(&gid)?;
        Ok(())
    }

    pub async fn start_group(&self, id: &str) -> ControlResult<GroupView> {
        let gid = GroupId::new(id).map_err(|e| ControlError::BadRequest(e.to_string()))?;
        let mut sup = self.supervisor.lock().await;
        match sup.start_group(&gid).await {
            Ok(_) => {}
            Err(RuntimeError::GroupAlreadyRunning(_)) => {}
            Err(e) => return Err(e.into()),
        }
        drop(sup);
        self.get_group(id).await
    }

    pub async fn pause_group(&self, id: &str) -> ControlResult<GroupView> {
        let gid = GroupId::new(id).map_err(|e| ControlError::BadRequest(e.to_string()))?;
        let mut sup = self.supervisor.lock().await;
        sup.stop_group(&gid).await?;
        drop(sup);
        self.get_group(id).await
    }

    pub async fn resume_group(&self, id: &str) -> ControlResult<GroupView> {
        self.start_group(id).await
    }

    pub async fn drain_group(&self, id: &str) -> ControlResult<GroupView> {
        let gid = GroupId::new(id).map_err(|e| ControlError::BadRequest(e.to_string()))?;
        let sup = self.supervisor.lock().await;
        let handle = sup
            .get_handle(&gid)
            .ok_or_else(|| ControlError::Conflict(format!("group is not running: {id}")))?;
        handle.drain().await?;
        drop(sup);
        self.get_group(id).await
    }

    pub async fn list_consumers(&self, id: &str) -> ControlResult<ConsumersView> {
        let gid = GroupId::new(id).map_err(|e| ControlError::BadRequest(e.to_string()))?;
        let sup = self.supervisor.lock().await;
        let consumers = match sup.get_handle(&gid) {
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

    pub async fn checkpoint(&self, id: &str) -> ControlResult<CheckpointView> {
        let gid = GroupId::new(id).map_err(|e| ControlError::BadRequest(e.to_string()))?;
        if self.store.get_group(&gid)?.is_none() {
            return Err(ControlError::NotFound(format!("group not found: {id}")));
        }
        let durable_cursor = self.store.load_checkpoint(&gid)?;
        let live_cursor = {
            let sup = self.supervisor.lock().await;
            match sup.get_handle(&gid) {
                Some(h) => Some(h.snapshot_cursor().await?),
                None => None,
            }
        };
        Ok(CheckpointView {
            group_id: id.to_string(),
            durable_cursor,
            live_cursor,
        })
    }

    /// Test/helper: join a consumer and ack batches until cursor advances or idle.
    pub async fn advance_for_test(
        &self,
        id: &str,
        consumer: &str,
        acks: usize,
    ) -> ControlResult<()> {
        let gid = GroupId::new(id).map_err(|e| ControlError::BadRequest(e.to_string()))?;
        let cid = ConsumerId::new(consumer).map_err(|e| ControlError::BadRequest(e.to_string()))?;
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

fn group_view(rec: &crate::store::GroupRecord, running: bool) -> GroupView {
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
        lifecycle: rec.lifecycle,
        next_batch_id: rec.next_batch_id,
        running,
    }
}

fn validate_connection_create(req: &ConnectionCreateRequest) -> ControlResult<()> {
    if req.id.is_empty() || req.id.len() > 256 {
        return Err(ControlError::BadRequest("invalid connection id".into()));
    }
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
