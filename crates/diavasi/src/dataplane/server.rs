use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{Stream, StreamExt};
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};

use crate::core::{Batch, BatchId, ConsumerId, CoreError, GroupId, OrderingAtom};
use crate::runtime::{GroupHandle, GroupSupervisor, RuntimeError};
use crate::store::RedbStore;

use super::error_codes::{HEARTBEAT_TIMEOUT, INTERNAL, NOT_RUNNING};
use super::pb::data_plane_server::{DataPlane, DataPlaneServer};
use super::pb::{Record, RecordBatch};
use super::session::{Effect, Session};
use super::{Envelope, error_envelope, record_batch};

pub struct DataPlaneConfig {
    pub bind: SocketAddr,
    pub tls_cert_pem: Vec<u8>,
    pub tls_key_pem: Vec<u8>,
    pub api_token: String,
    pub supervisor: Arc<Mutex<GroupSupervisor<RedbStore>>>,
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
}

pub async fn serve_dataplane(
    config: DataPlaneConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let identity = Identity::from_pem(&config.tls_cert_pem, &config.tls_key_pem);
    let svc = DataSvc {
        auth_token: config.api_token,
        supervisor: config.supervisor,
        heartbeat_interval: config.heartbeat_interval,
        heartbeat_timeout: config.heartbeat_timeout,
    };
    let addr = config.bind;
    tracing::info!(%addr, "diavasi data plane listening (gRPC TLS)");
    Server::builder()
        .tls_config(ServerTlsConfig::new().identity(identity))?
        .add_service(DataPlaneServer::new(svc))
        .serve(addr)
        .await?;
    Ok(())
}

struct DataSvc {
    auth_token: String,
    supervisor: Arc<Mutex<GroupSupervisor<RedbStore>>>,
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
}

#[tonic::async_trait]
impl DataPlane for DataSvc {
    type ConsumeStream = Pin<Box<dyn Stream<Item = Result<Envelope, Status>> + Send>>;

    async fn consume(
        &self,
        request: Request<Streaming<Envelope>>,
    ) -> Result<Response<Self::ConsumeStream>, Status> {
        if !bearer_ok(request.metadata(), &self.auth_token) {
            return Err(Status::unauthenticated("unauthorized"));
        }
        let inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(32);
        let supervisor = Arc::clone(&self.supervisor);
        let heartbeat_interval = self.heartbeat_interval;
        let heartbeat_timeout = self.heartbeat_timeout;
        tokio::spawn(async move {
            drive_session(
                supervisor,
                inbound,
                tx,
                heartbeat_interval,
                heartbeat_timeout,
            )
            .await;
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

fn bearer_ok(meta: &tonic::metadata::MetadataMap, expected: &str) -> bool {
    let Some(value) = meta.get("authorization").and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let Some(token) = value.strip_prefix("Bearer ") else {
        return false;
    };
    subtle_eq(token.trim().as_bytes(), expected.as_bytes())
}

fn subtle_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

struct JoinedConsumer {
    handle: GroupHandle,
    consumer: ConsumerId,
    left: bool,
}

impl JoinedConsumer {
    async fn leave(&mut self) {
        if self.left {
            return;
        }
        self.left = true;
        let _ = self.handle.leave(&self.consumer).await;
    }
}

impl Drop for JoinedConsumer {
    fn drop(&mut self) {
        if self.left {
            return;
        }
        self.left = true;
        let handle = self.handle.clone();
        let consumer = self.consumer.clone();
        tokio::spawn(async move {
            let _ = handle.leave(&consumer).await;
        });
    }
}

async fn drive_session(
    supervisor: Arc<Mutex<GroupSupervisor<RedbStore>>>,
    mut inbound: Streaming<Envelope>,
    tx: mpsc::Sender<Result<Envelope, Status>>,
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
) {
    let mut session = Session::new();
    let mut joined: Option<JoinedConsumer> = None;
    let mut last_rx = Instant::now();
    let mut heartbeat = tokio::time::interval(heartbeat_interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        let assign_target = joined
            .as_ref()
            .map(|consumer| (consumer.handle.clone(), consumer.consumer.clone()));
        tokio::select! {
            _ = heartbeat.tick() => {
                if last_rx.elapsed() > heartbeat_timeout {
                    if let Some(consumer) = joined.as_mut() {
                        consumer.leave().await;
                    }
                    let _ = tx
                        .send(Ok(error_envelope(
                            HEARTBEAT_TIMEOUT,
                            "heartbeat timeout",
                        )))
                        .await;
                    return;
                }
                if session.phase() == super::session::Phase::Active {
                    let _ = tx.send(Ok(super::heartbeat())).await;
                }
            }
            incoming = inbound.next() => {
                match incoming {
                    None => {
                        if let Some(consumer) = joined.as_mut() {
                            consumer.leave().await;
                        }
                        return;
                    }
                    Some(Err(e)) => {
                        if let Some(consumer) = joined.as_mut() {
                            consumer.leave().await;
                        }
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                    Some(Ok(env)) => {
                        last_rx = Instant::now();
                        let step = session.on_frame(&env);
                        let mut failed = false;
                        for effect in step.effects {
                            match effect {
                                Effect::Join { group_id, consumer_id } => {
                                    match join_group(&supervisor, &group_id, &consumer_id).await {
                                        Ok(consumer) => {
                                            let frame = super::joined(
                                                consumer.handle.group_id.as_str(),
                                                consumer.consumer.as_str(),
                                            );
                                            let _ = tx.send(Ok(frame)).await;
                                            joined = Some(consumer);
                                        }
                                        Err(frame) => {
                                            session.close();
                                            let _ = tx.send(Ok(frame)).await;
                                            failed = true;
                                        }
                                    }
                                }
                                Effect::Ack { batch_id } => {
                                    if let Some(consumer) = joined.as_ref() {
                                        if let Err(e) = consumer
                                            .handle
                                            .ack(BatchId::from_u64(batch_id))
                                            .await
                                        {
                                            session.close();
                                            let _ = tx
                                                .send(Ok(error_envelope(INTERNAL, e.to_string())))
                                                .await;
                                            failed = true;
                                        }
                                    }
                                }
                                Effect::Leave => {
                                    if let Some(consumer) = joined.as_mut() {
                                        consumer.leave().await;
                                    }
                                }
                            }
                        }
                        for frame in step.frames {
                            if tx.send(Ok(frame)).await.is_err() {
                                if let Some(consumer) = joined.as_mut() {
                                    consumer.leave().await;
                                }
                                return;
                            }
                        }
                        if step.close || failed {
                            if let Some(consumer) = joined.as_mut() {
                                consumer.leave().await;
                            }
                            return;
                        }
                    }
                }
            }
            pulled = pull_owned(assign_target.clone()), if session.can_assign() && assign_target.is_some() => {
                match pulled {
                    Pull::Batch(batch) => {
                        let id = batch.id.as_u64();
                        session.note_assigned(id);
                        let frame = record_batch(batch_to_proto(&batch));
                        if tx.send(Ok(frame)).await.is_err() {
                            if let Some(consumer) = joined.as_mut() {
                                consumer.leave().await;
                            }
                            return;
                        }
                    }
                    Pull::Idle => {}
                    Pull::Failed(message) => {
                        session.close();
                        let _ = tx.send(Ok(error_envelope(INTERNAL, message))).await;
                        if let Some(consumer) = joined.as_mut() {
                            consumer.leave().await;
                        }
                        return;
                    }
                }
            }
        }
    }
}

enum Pull {
    Batch(Batch),
    Idle,
    Failed(String),
}

async fn pull_owned(target: Option<(GroupHandle, ConsumerId)>) -> Pull {
    let Some((handle, consumer)) = target else {
        return Pull::Idle;
    };
    match handle.assign(&consumer).await {
        Ok(batch) => Pull::Batch(batch),
        Err(RuntimeError::Core(CoreError::NoWork)) => {
            tokio::time::sleep(Duration::from_millis(5)).await;
            Pull::Idle
        }
        Err(e) => Pull::Failed(e.to_string()),
    }
}

async fn join_group(
    supervisor: &Arc<Mutex<GroupSupervisor<RedbStore>>>,
    group_id: &str,
    consumer_id: &str,
) -> Result<JoinedConsumer, Envelope> {
    let gid = match GroupId::new(group_id) {
        Ok(id) => id,
        Err(e) => return Err(error_envelope(NOT_RUNNING, e.to_string())),
    };
    let cid = match ConsumerId::new(consumer_id) {
        Ok(id) => id,
        Err(e) => return Err(error_envelope(NOT_RUNNING, e.to_string())),
    };
    let handle = {
        let sup = supervisor.lock().await;
        match sup.get_handle(&gid) {
            Some(handle) => handle,
            None => {
                return Err(error_envelope(
                    NOT_RUNNING,
                    format!("group is not running: {group_id}"),
                ));
            }
        }
    };
    if let Err(e) = handle.join(cid.clone()).await {
        return Err(error_envelope(NOT_RUNNING, e.to_string()));
    }
    Ok(JoinedConsumer {
        handle,
        consumer: cid,
        left: false,
    })
}

fn batch_to_proto(batch: &Batch) -> RecordBatch {
    RecordBatch {
        batch_id: batch.id.as_u64(),
        records: batch
            .records
            .iter()
            .map(|record| Record {
                record_id: match record.ordering.atoms() {
                    [OrderingAtom::U64(id)] => *id,
                    _ => 0,
                },
                payload: record.payload.to_vec(),
            })
            .collect(),
    }
}
