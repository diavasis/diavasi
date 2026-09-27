use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::Poll;
use std::time::{Duration, Instant};

use futures::{Stream, StreamExt};
use tokio::sync::{Mutex, Notify, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};

use crate::control::{AuthValidator, BearerTokenAuth};
use crate::core::{Batch, BatchId, ConsumerId, CoreError, GroupId, OrderingAtom};
use crate::observe::Observe;
use crate::runtime::{GroupHandle, GroupSupervisor, RuntimeError};
use crate::store::RedbStore;

use super::error_codes::{BAD_STATE, HEARTBEAT_TIMEOUT, INTERNAL, NOT_RUNNING};
use super::pb::data_plane_server::{DataPlane, DataPlaneServer};
use super::pb::{Record, RecordBatch};
use super::session::{Effect, Session};
use super::{Envelope, error_envelope, record_batch};

/// Settings for [`serve_dataplane`].
pub struct DataPlaneConfig {
    /// Address to listen on.
    pub bind: SocketAddr,
    /// PEM certificate chain the server presents.
    pub tls_cert_pem: Vec<u8>,
    /// PEM private key of the certificate.
    pub tls_key_pem: Vec<u8>,
    /// Bearer token clients must send.
    pub api_token: String,
    /// The supervisor that owns the groups consumers join.
    pub supervisor: Arc<Mutex<GroupSupervisor<RedbStore>>>,
    /// How often the server sends a heartbeat on an active stream.
    pub heartbeat_interval: Duration,
    /// A stream that sends nothing for this long is closed with error 8.
    pub heartbeat_timeout: Duration,
}

/// Bind `config.bind` and serve `DataPlane.Consume` over TLS until the task
/// is aborted or the listener fails. See [`serve_dataplane_on`].
pub async fn serve_dataplane(config: DataPlaneConfig) -> Result<(), DataPlaneError> {
    let listener = tokio::net::TcpListener::bind(config.bind)
        .await
        .map_err(DataPlaneError::Bind)?;
    serve_dataplane_on(config, listener).await
}

/// Why the data plane stopped.
#[derive(Debug, thiserror::Error)]
pub enum DataPlaneError {
    /// The address could not be bound.
    #[error("bind: {0}")]
    Bind(std::io::Error),
    /// The gRPC server failed.
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
}

/// Serve `DataPlane.Consume` over TLS on a bound `listener`.
/// `config.bind` is only reported.
pub async fn serve_dataplane_on(
    config: DataPlaneConfig,
    listener: tokio::net::TcpListener,
) -> Result<(), DataPlaneError> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let identity = Identity::from_pem(&config.tls_cert_pem, &config.tls_key_pem);
    let addr = listener.local_addr().unwrap_or(config.bind);
    let svc = data_service(config);
    tracing::info!(%addr, "diavasi data plane listening (gRPC TLS)");
    let incoming = futures::stream::unfold(listener, |listener| async move {
        let accepted = listener.accept().await.map(|(stream, _)| stream);
        Some((accepted, listener))
    });
    Server::builder()
        .tls_config(ServerTlsConfig::new().identity(identity))?
        .add_service(DataPlaneServer::new(svc))
        .serve_with_incoming(incoming)
        .await?;
    Ok(())
}

fn data_service(config: DataPlaneConfig) -> DataSvc {
    DataSvc {
        auth: BearerTokenAuth::new(config.api_token),
        supervisor: config.supervisor,
        sessions: Arc::new(SessionRegistry::default()),
        heartbeat_interval: config.heartbeat_interval,
        heartbeat_timeout: config.heartbeat_timeout,
    }
}

/// How long a stream's request for records waits in the group before the
/// group answers that it has none.
const ASSIGN_WAIT: Duration = Duration::from_secs(1);

/// How long a new stream waits for the stream it replaces to close.
const TAKEOVER_WAIT: Duration = Duration::from_secs(1);

struct DataSvc {
    auth: BearerTokenAuth,
    supervisor: Arc<Mutex<GroupSupervisor<RedbStore>>>,
    sessions: Arc<SessionRegistry>,
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
        if !bearer_ok(request.metadata(), &self.auth) {
            return Err(Status::unauthenticated("unauthorized"));
        }
        let inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(32);
        let supervisor = Arc::clone(&self.supervisor);
        let sessions = Arc::clone(&self.sessions);
        let heartbeat_interval = self.heartbeat_interval;
        let heartbeat_timeout = self.heartbeat_timeout;
        tokio::spawn(async move {
            drive_session(
                supervisor,
                sessions,
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

/// Check `authorization: Bearer <token>` with the control plane's validator.
fn bearer_ok(meta: &tonic::metadata::MetadataMap, auth: &BearerTokenAuth) -> bool {
    let token = meta
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim);
    auth.validate_bearer(token)
}

/// One joined stream, as the takeover logic sees it.
struct SessionEntry {
    id: u64,
    /// Set before `evict` fires. The evicted stream then exits without
    /// calling `leave`, which would remove the consumer that replaced it.
    evicted: Arc<AtomicBool>,
    evict: Arc<Notify>,
    /// Fires when the stream has exited.
    closed: Arc<Notify>,
}

/// Joined streams by group and consumer id. A join with a consumer id that
/// another stream holds takes over: the old stream closes, its unacked
/// batches return to the buffer, and the new stream joins. This is the
/// reconnect path for a client whose old connection is not yet known dead.
#[derive(Default)]
struct SessionRegistry {
    next_id: AtomicU64,
    sessions: std::sync::Mutex<HashMap<(String, String), SessionEntry>>,
}

impl SessionRegistry {
    fn key(group: &GroupId, consumer: &ConsumerId) -> (String, String) {
        (group.as_str().to_string(), consumer.as_str().to_string())
    }

    fn register(&self, group: &GroupId, consumer: &ConsumerId) -> SessionTicket {
        let entry = SessionEntry {
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
            evicted: Arc::new(AtomicBool::new(false)),
            evict: Arc::new(Notify::new()),
            closed: Arc::new(Notify::new()),
        };
        let ticket = SessionTicket {
            id: entry.id,
            evicted: Arc::clone(&entry.evicted),
            evict: Arc::clone(&entry.evict),
            closed: Arc::clone(&entry.closed),
        };
        self.lock().insert(Self::key(group, consumer), entry);
        ticket
    }

    /// Tell the stream holding `consumer` to close. Returns the signal that
    /// fires when it has, or `None` when no stream holds it.
    fn evict(&self, group: &GroupId, consumer: &ConsumerId) -> Option<Arc<Notify>> {
        let sessions = self.lock();
        let entry = sessions.get(&Self::key(group, consumer))?;
        entry.evicted.store(true, Ordering::SeqCst);
        entry.evict.notify_one();
        Some(Arc::clone(&entry.closed))
    }

    fn remove(&self, group: &GroupId, consumer: &ConsumerId, id: u64) {
        let mut sessions = self.lock();
        let key = Self::key(group, consumer);
        if sessions.get(&key).is_some_and(|entry| entry.id == id) {
            sessions.remove(&key);
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<(String, String), SessionEntry>> {
        self.sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// A stream's own view of its registry entry.
struct SessionTicket {
    id: u64,
    evicted: Arc<AtomicBool>,
    evict: Arc<Notify>,
    closed: Arc<Notify>,
}

struct JoinedConsumer {
    handle: GroupHandle,
    consumer: ConsumerId,
    left: bool,
    observe: Observe,
    sessions: Arc<SessionRegistry>,
    ticket: SessionTicket,
}

impl JoinedConsumer {
    /// Leave the group so this stream's unacked batches return to the
    /// buffer. Skipped when a newer stream took the consumer id over.
    async fn leave(&mut self) {
        if self.left {
            return;
        }
        self.left = true;
        if !self.ticket.evicted.load(Ordering::SeqCst) {
            let _ = self.handle.leave(&self.consumer).await;
        }
        note_disconnect(&self.observe, &self.handle, &self.consumer);
    }
}

impl Drop for JoinedConsumer {
    fn drop(&mut self) {
        self.sessions
            .remove(self.handle.group_id(), &self.consumer, self.ticket.id);
        self.ticket.closed.notify_one();
        if self.left {
            return;
        }
        self.left = true;
        if self.ticket.evicted.load(Ordering::SeqCst) {
            return;
        }
        let handle = self.handle.clone();
        let consumer = self.consumer.clone();
        let observe = self.observe.clone();
        tokio::spawn(async move {
            let _ = handle.leave(&consumer).await;
            note_disconnect(&observe, &handle, &consumer);
        });
    }
}

fn note_disconnect(observe: &Observe, handle: &GroupHandle, consumer: &ConsumerId) {
    observe.record_disconnect(handle.group_id().as_str());
    tracing::info!(
        group_id = %handle.group_id(),
        consumer_id = %consumer.as_str(),
        "consumer disconnected"
    );
}

async fn drive_session(
    supervisor: Arc<Mutex<GroupSupervisor<RedbStore>>>,
    sessions: Arc<SessionRegistry>,
    mut inbound: Streaming<Envelope>,
    tx: mpsc::Sender<Result<Envelope, Status>>,
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
) {
    let observe = supervisor.lock().await.observe();
    let mut session = Session::new();
    let mut joined: Option<JoinedConsumer> = None;
    let mut last_rx = Instant::now();
    let mut heartbeat = tokio::time::interval(heartbeat_interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Held across iterations so a heartbeat or inbound frame cannot drop an
    // assign that the group owner has already moved into inflight.
    let mut inflight_pull: Option<Pin<Box<dyn Future<Output = Pull> + Send>>> = None;
    // Fires when a newer stream takes this consumer id over.
    let mut evict: Option<Arc<Notify>> = None;

    loop {
        if inflight_pull.is_none() && session.can_assign() {
            if let Some(consumer) = joined.as_ref() {
                let target = (consumer.handle.clone(), consumer.consumer.clone());
                inflight_pull = Some(Box::pin(pull_owned(Some(target))));
            }
        }
        tokio::select! {
            _ = wait_evicted(evict.as_deref()) => {
                if let Some(consumer) = joined.as_mut() {
                    consumer.leave().await;
                }
                let _ = tx
                    .send(Ok(error_envelope(
                        BAD_STATE,
                        "consumer id taken over by a newer stream",
                    )))
                    .await;
                return;
            }
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
                                    match join_group(&supervisor, &sessions, &group_id, &consumer_id, observe.clone()).await {
                                        Ok(consumer) => {
                                            evict = Some(Arc::clone(&consumer.ticket.evict));
                                            let frame = super::joined(
                                                consumer.handle.group_id().as_str(),
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
                                            tracing::warn!(
                                                group_id = %consumer.handle.group_id(),
                                                consumer_id = %consumer.consumer.as_str(),
                                                error = %e,
                                                "ack failed"
                                            );
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
            pulled = poll_pull(&mut inflight_pull), if inflight_pull.is_some() => {
                inflight_pull = None;
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

fn poll_pull(
    slot: &mut Option<Pin<Box<dyn Future<Output = Pull> + Send>>>,
) -> impl Future<Output = Pull> + '_ {
    std::future::poll_fn(move |cx| match slot.as_mut() {
        Some(fut) => fut.as_mut().poll(cx),
        None => Poll::Pending,
    })
}

async fn pull_owned(target: Option<(GroupHandle, ConsumerId)>) -> Pull {
    let Some((handle, consumer)) = target else {
        return Pull::Idle;
    };
    match handle.assign_wait(&consumer, ASSIGN_WAIT).await {
        Ok(batch) => Pull::Batch(batch),
        Err(RuntimeError::Core(CoreError::NoWork)) => Pull::Idle,
        Err(e) => Pull::Failed(e.to_string()),
    }
}

async fn wait_evicted(evict: Option<&Notify>) {
    match evict {
        Some(evict) => evict.notified().await,
        None => std::future::pending().await,
    }
}

/// Protocol error code for a failed join.
fn join_error(err: &RuntimeError) -> Envelope {
    let code = match err {
        RuntimeError::GroupNotRunning(_)
        | RuntimeError::ChannelClosed
        | RuntimeError::Core(CoreError::NotRunning(_)) => NOT_RUNNING,
        RuntimeError::Core(_) => BAD_STATE,
        _ => INTERNAL,
    };
    error_envelope(code, err.to_string())
}

async fn join_group(
    supervisor: &Arc<Mutex<GroupSupervisor<RedbStore>>>,
    sessions: &Arc<SessionRegistry>,
    group_id: &str,
    consumer_id: &str,
    observe: Observe,
) -> Result<JoinedConsumer, Envelope> {
    let gid = GroupId::new(group_id).map_err(|e| error_envelope(BAD_STATE, e.to_string()))?;
    let cid = ConsumerId::new(consumer_id).map_err(|e| error_envelope(BAD_STATE, e.to_string()))?;
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
    match handle.join(cid.clone()).await {
        Ok(()) => {}
        Err(RuntimeError::Core(CoreError::DuplicateConsumer(_))) => {
            let Some(closed) = sessions.evict(&gid, &cid) else {
                return Err(error_envelope(BAD_STATE, "consumer id is already joined"));
            };
            let _ = tokio::time::timeout(TAKEOVER_WAIT, closed.notified()).await;
            // Return the old stream's unacked batches, then join as its successor.
            let _ = handle.leave(&cid).await;
            handle.join(cid.clone()).await.map_err(|e| join_error(&e))?;
            tracing::info!(
                group_id = %gid,
                consumer_id = %cid.as_str(),
                "stream took over a joined consumer id"
            );
        }
        Err(e) => return Err(join_error(&e)),
    }
    let ticket = sessions.register(&gid, &cid);
    Ok(JoinedConsumer {
        handle,
        consumer: cid,
        left: false,
        observe,
        sessions: Arc::clone(sessions),
        ticket,
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
                    [OrderingAtom::I64(id)] if *id >= 0 => *id as u64,
                    _ => 0,
                },
                payload: record.payload.clone(),
            })
            .collect(),
    }
}
