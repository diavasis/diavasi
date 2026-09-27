use std::collections::VecDeque;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};
use tokio::task::{AbortHandle, JoinHandle};
use tracing::Instrument;

use crate::core::{
    AckOutcome, Batch, BatchId, ConsumerId, CoreError, LogicalCursor, RecordSource, SourceError,
};
use crate::observe::Observe;
use crate::store::{DurableGroup, StateStore, StoreError};

use super::command::{BufferStats, LiveSnapshot, RuntimeCommand};
use super::error::{RuntimeError, RuntimeResult};
use super::handle::GroupHandle;

pub const DEFAULT_COMMAND_CAPACITY: usize = 64;
pub const DEFAULT_FETCH_INTERVAL: Duration = Duration::from_millis(5);
pub const DEFAULT_IDLE_FETCH_MAX: Duration = Duration::from_secs(1);
pub const DEFAULT_TICK_INTERVAL: Duration = Duration::from_millis(25);

/// Timing and capacity of one group runtime.
#[derive(Debug, Clone)]
pub struct GroupRuntimeConfig {
    /// Commands the owner mailbox holds before senders wait.
    pub command_capacity: usize,
    /// How often the owner considers a fetch while the source is producing.
    pub fetch_interval: Duration,
    /// Longest wait between fetches while the source has nothing new. After
    /// each empty fetch the wait doubles, starting at `fetch_interval`. A
    /// fetch that returns records resets it.
    pub idle_fetch_max: Duration,
    /// How often timed-out batches are returned to the buffer.
    pub tick_interval: Duration,
    /// Zero writes the checkpoint before each ack is answered; acks already
    /// waiting in the mailbox share that write. A positive interval answers
    /// acks at once and writes at most once per interval, on the timeout
    /// tick; a crash can then replay up to one interval of acked records.
    pub checkpoint_interval: Duration,
}

impl Default for GroupRuntimeConfig {
    fn default() -> Self {
        Self {
            command_capacity: DEFAULT_COMMAND_CAPACITY,
            fetch_interval: DEFAULT_FETCH_INTERVAL,
            idle_fetch_max: DEFAULT_IDLE_FETCH_MAX,
            tick_interval: DEFAULT_TICK_INTERVAL,
            checkpoint_interval: Duration::ZERO,
        }
    }
}

/// Why a group task ended without an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupExit {
    /// An operator pause. The store records `Stopped`.
    Paused,
    /// A drain finished: every fetched record was acked. The store records
    /// `Stopped`.
    Drained,
    /// The process is shutting down. The store keeps the lifecycle, so the
    /// group resumes when the server starts again.
    Shutdown,
}

pub struct SpawnedGroup {
    pub handle: GroupHandle,
    pub abort: AbortHandle,
    pub join: JoinHandle<RuntimeResult<GroupExit>>,
}

/// One read request from the owner to the fetch task.
struct FetchRequest {
    cursor: LogicalCursor,
    limit: usize,
}

/// When the next fetch may start. An empty read doubles the wait up to
/// `idle_max`. A read that returns records resets it.
struct FetchPacing {
    interval: Duration,
    idle_max: Duration,
    delay: Duration,
    not_before: Instant,
}

impl FetchPacing {
    fn new(config: &GroupRuntimeConfig) -> Self {
        Self {
            interval: config.fetch_interval,
            idle_max: config.idle_fetch_max.max(config.fetch_interval),
            delay: Duration::ZERO,
            not_before: Instant::now(),
        }
    }

    fn ready(&self, now: Instant) -> bool {
        now >= self.not_before
    }

    fn record(&mut self, empty: bool, now: Instant) {
        self.delay = if !empty {
            Duration::ZERO
        } else if self.delay.is_zero() {
            self.interval
        } else {
            self.delay.saturating_mul(2).min(self.idle_max)
        };
        self.not_before = now + self.delay;
    }
}

/// Spawn the owner task, the tickers, and, for an adapter source, the fetch
/// task. The owner is the only task that touches the engine and never waits
/// on source I/O: reads run in the fetch task and return as
/// [`RuntimeCommand::Fetched`].
pub fn spawn_group_runtime<S>(
    mut durable: DurableGroup<S>,
    config: GroupRuntimeConfig,
    source: Option<Box<dyn RecordSource>>,
    observe: Observe,
    adapter: String,
) -> SpawnedGroup
where
    S: StateStore + 'static,
{
    let group_id = durable.group_id().clone();
    let group_label = group_id.as_str().to_string();
    let (tx, mut rx) = mpsc::channel::<RuntimeCommand>(config.command_capacity);
    let handle = GroupHandle::new(group_id.clone(), tx.clone());

    let fetch_tick = spawn_ticker(tx.clone(), config.fetch_interval, || RuntimeCommand::Fetch);
    let timeout_tick = spawn_ticker(tx.clone(), config.tick_interval, || RuntimeCommand::Tick);
    let (fetch_requests, fetch_task) = match source {
        Some(source) => {
            let (requests, task) = spawn_fetcher(source, tx, group_label.clone());
            (Some(requests), Some(task))
        }
        None => (None, None),
    };

    let mut pacing = FetchPacing::new(&config);
    let checkpoint_interval = config.checkpoint_interval;
    let join = tokio::spawn(async move {
        let mut fetch_in_flight = false;
        // A command taken from the mailbox while collecting acks, handled next.
        let mut stashed: Option<RuntimeCommand> = None;
        let mut last_checkpoint = Instant::now();
        // Consumers waiting for records, oldest first.
        let mut waiters: VecDeque<Waiter> = VecDeque::new();
        let run = async {
            // Record the running lifecycle, so the store agrees with the
            // supervisor and a restarted process knows to resume the group.
            durable.snapshot_to_store()?;
            loop {
                let cmd = match stashed.take() {
                    Some(cmd) => cmd,
                    None => match rx.recv().await {
                        Some(cmd) => cmd,
                        None => break,
                    },
                };
                match cmd {
                    RuntimeCommand::Join { consumer, reply } => {
                        let _ = reply.send(durable.join_consumer(consumer).map_err(Into::into));
                    }
                    RuntimeCommand::Leave { consumer, reply } => {
                        let before = durable.engine().inflight_records();
                        let result = durable.leave_consumer(&consumer);
                        if result.is_ok() {
                            note_replay(
                                &observe,
                                &group_label,
                                &adapter,
                                before,
                                durable.engine().inflight_records(),
                            );
                        }
                        let _ = reply.send(result.map_err(Into::into));
                    }
                    RuntimeCommand::Assign { consumer, reply } => {
                        let result = durable.assign_batch(&consumer).map_err(Into::into);
                        deliver(
                            &mut durable,
                            &observe,
                            &group_label,
                            &adapter,
                            result,
                            reply,
                        );
                    }
                    RuntimeCommand::AssignWait {
                        consumer,
                        wait,
                        reply,
                    } => match durable.assign_batch(&consumer) {
                        Err(StoreError::Core(CoreError::NoWork)) => {
                            waiters.push_back(Waiter {
                                consumer,
                                reply,
                                deadline: Instant::now() + wait,
                            });
                        }
                        result => {
                            let result = result.map_err(Into::into);
                            deliver(
                                &mut durable,
                                &observe,
                                &group_label,
                                &adapter,
                                result,
                                reply,
                            );
                        }
                    },
                    RuntimeCommand::Ack { batch_id, reply } => {
                        // Take the acks already waiting, so one checkpoint
                        // write covers all of them.
                        let mut acks = vec![(batch_id, reply)];
                        while let Ok(next) = rx.try_recv() {
                            match next {
                                RuntimeCommand::Ack { batch_id, reply } => {
                                    acks.push((batch_id, reply));
                                }
                                other => {
                                    stashed = Some(other);
                                    break;
                                }
                            }
                        }
                        let _span =
                            tracing::debug_span!("group_ack", group_id = %group_label).entered();
                        let started = Instant::now();
                        let mut results = Vec::with_capacity(acks.len());
                        for (batch_id, _) in &acks {
                            results.push(apply_ack(
                                &mut durable,
                                &observe,
                                &group_label,
                                &adapter,
                                *batch_id,
                            ));
                        }
                        let persisted = if checkpoint_interval.is_zero() {
                            durable.persist_progress().map_err(RuntimeError::from)
                        } else {
                            Ok(false)
                        };
                        match &persisted {
                            Ok(true) => {
                                observe.record_checkpoint(&group_label, started.elapsed());
                                last_checkpoint = Instant::now();
                            }
                            Ok(false) => {}
                            Err(err) => {
                                tracing::warn!(group_id = %group_label, error = %err, "checkpoint write failed");
                            }
                        }
                        for ((_, reply), result) in acks.into_iter().zip(results) {
                            let result = match (&result, &persisted) {
                                (Ok(()), Err(err)) => {
                                    Err(RuntimeError::CheckpointFailed(err.to_string()))
                                }
                                _ => result,
                            };
                            if let Err(ref err) = result {
                                tracing::warn!(group_id = %group_label, error = %err, "ack failed");
                            }
                            let _ = reply.send(result);
                        }
                    }
                    RuntimeCommand::SnapshotCursor { reply } => {
                        let _ = reply.send(Ok(durable.committed_cursor().clone()));
                    }
                    RuntimeCommand::BufferStats { reply } => {
                        let eng = durable.engine();
                        let _ = reply.send(Ok(BufferStats {
                            buffer_len: eng.buffer_len(),
                            buffer_bytes: eng.buffer_bytes(),
                            inflight_len: eng.inflight_len(),
                            max_buffer_records: eng.config().max_buffer_records,
                            max_buffer_bytes: eng.config().max_buffer_bytes,
                        }));
                    }
                    RuntimeCommand::LiveSnapshot { reply } => {
                        let eng = durable.engine();
                        let _ = reply.send(Ok(LiveSnapshot {
                            lifecycle: eng.lifecycle(),
                            committed: eng.committed_cursor().clone(),
                            fetched: eng.fetched_cursor().clone(),
                            buffer_records: eng.buffer_len(),
                            buffer_bytes: eng.buffer_bytes(),
                            inflight_records: eng.inflight_records(),
                            consumers: durable.list_consumers(),
                        }));
                    }
                    RuntimeCommand::Lifecycle { reply } => {
                        let _ = reply.send(Ok(durable.engine().lifecycle()));
                    }
                    RuntimeCommand::ListConsumers { reply } => {
                        let _ = reply.send(Ok(durable.list_consumers()));
                    }
                    RuntimeCommand::Drain { reply } => {
                        let result = durable
                            .drain()
                            .map_err(RuntimeError::from)
                            .and_then(|()| durable.snapshot_to_store().map_err(Into::into));
                        let _ = reply.send(result);
                    }
                    RuntimeCommand::Fetch => {
                        let now = Instant::now();
                        if fetch_in_flight || !pacing.ready(now) {
                            continue;
                        }
                        let slots = durable.engine().buffer_free_records();
                        if slots == 0 {
                            continue;
                        }
                        match &fetch_requests {
                            Some(requests) => {
                                let request = FetchRequest {
                                    cursor: durable.engine().fetched_cursor().clone(),
                                    limit: slots,
                                };
                                fetch_in_flight = requests.try_send(request).is_ok();
                            }
                            None => {
                                let fetched = durable.poll_fetch()?;
                                if fetched > 0 {
                                    let bytes = fetched
                                        .saturating_mul(durable.engine().config().payload_size);
                                    observe.record_fetch(
                                        &group_label,
                                        &adapter,
                                        fetched as u64,
                                        bytes as u64,
                                        now.elapsed(),
                                    );
                                }
                                pacing.record(fetched == 0, Instant::now());
                            }
                        }
                    }
                    RuntimeCommand::Fetched {
                        cursor,
                        result,
                        latency,
                    } => {
                        fetch_in_flight = false;
                        let records = match result {
                            Ok(records) => records,
                            Err(err) => {
                                observe.record_fetch(&group_label, &adapter, 0, 0, latency);
                                observe.record_adapter_error(&group_label, &adapter);
                                tracing::warn!(
                                    group_id = %group_label,
                                    adapter = %adapter,
                                    error = %err,
                                    transient = err.is_transient(),
                                    "adapter fetch failed"
                                );
                                return Err(RuntimeError::Source(err));
                            }
                        };
                        if &cursor != durable.engine().fetched_cursor() {
                            // Started before the fetched position moved; read again.
                            continue;
                        }
                        let empty = records.is_empty();
                        let sizes: Vec<usize> = records.iter().map(|r| r.byte_len()).collect();
                        let accepted = durable.engine_mut().ingest(records).map_err(|err| {
                            RuntimeError::Source(SourceError::Contract(err.to_string()))
                        })?;
                        if accepted > 0 {
                            let bytes: usize = sizes.into_iter().take(accepted).sum();
                            observe.record_fetch(
                                &group_label,
                                &adapter,
                                accepted as u64,
                                bytes as u64,
                                latency,
                            );
                        }
                        pacing.record(empty, Instant::now());
                    }
                    RuntimeCommand::Tick => {
                        if !checkpoint_interval.is_zero()
                            && last_checkpoint.elapsed() >= checkpoint_interval
                            && durable.has_unpersisted_progress()
                        {
                            let started = Instant::now();
                            match durable.persist_progress() {
                                Ok(_) => {
                                    observe.record_checkpoint(&group_label, started.elapsed());
                                    last_checkpoint = Instant::now();
                                }
                                Err(err) => {
                                    tracing::warn!(group_id = %group_label, error = %err, "checkpoint write failed");
                                }
                            }
                        }
                        let before = durable.engine().inflight_records();
                        let _ = durable.tick(Instant::now());
                        note_replay(
                            &observe,
                            &group_label,
                            &adapter,
                            before,
                            durable.engine().inflight_records(),
                        );
                    }
                    RuntimeCommand::Stop { reply } => {
                        let started = Instant::now();
                        let res = durable
                            .engine_mut()
                            .pause()
                            .map_err(RuntimeError::from)
                            .and_then(|()| durable.snapshot_to_store().map_err(Into::into));
                        if res.is_ok() {
                            observe.record_checkpoint(&group_label, started.elapsed());
                        }
                        let _ = reply.send(res);
                        return Ok(GroupExit::Paused);
                    }
                    RuntimeCommand::Shutdown { reply } => {
                        let res = durable.snapshot_to_store().map_err(Into::into);
                        let _ = reply.send(res);
                        return Ok(GroupExit::Shutdown);
                    }
                }
                serve_waiters(&mut waiters, &mut durable, &observe, &group_label, &adapter);
                if durable.engine().is_drained() {
                    durable.engine_mut().stop()?;
                    durable.snapshot_to_store()?;
                    tracing::info!(group_id = %group_label, "group drained");
                    return Ok(GroupExit::Drained);
                }
            }
            Ok(GroupExit::Shutdown)
        };

        let result = run.await;
        for task in [Some(fetch_tick), Some(timeout_tick), fetch_task]
            .into_iter()
            .flatten()
        {
            task.abort();
            let _ = task.await;
        }
        result
    });

    let abort = join.abort_handle();
    SpawnedGroup {
        handle,
        abort,
        join,
    }
}

/// A consumer waiting in [`RuntimeCommand::AssignWait`] for records.
struct Waiter {
    consumer: ConsumerId,
    reply: oneshot::Sender<RuntimeResult<Batch>>,
    deadline: Instant,
}

/// Answer an assign. When the caller is gone, the batch goes back to the
/// buffer for another consumer.
fn deliver<S: StateStore>(
    durable: &mut DurableGroup<S>,
    observe: &Observe,
    group: &str,
    adapter: &str,
    result: RuntimeResult<Batch>,
    reply: oneshot::Sender<RuntimeResult<Batch>>,
) {
    let Ok(batch) = result else {
        let _ = reply.send(result);
        return;
    };
    let id = batch.id;
    let records = batch.records.len() as u64;
    if reply.send(Ok(batch)).is_err() {
        let before = durable.engine().inflight_records();
        durable.engine_mut().requeue_batch(id);
        note_replay(
            observe,
            group,
            adapter,
            before,
            durable.engine().inflight_records(),
        );
    } else {
        observe.record_deliver(group, adapter, records);
    }
}

/// Hand buffered records to waiting consumers in arrival order, and answer
/// `NoWork` to those whose wait has run out.
fn serve_waiters<S: StateStore>(
    waiters: &mut VecDeque<Waiter>,
    durable: &mut DurableGroup<S>,
    observe: &Observe,
    group: &str,
    adapter: &str,
) {
    let now = Instant::now();
    let mut still_waiting = VecDeque::with_capacity(waiters.len());
    while let Some(waiter) = waiters.pop_front() {
        if waiter.reply.is_closed() {
            continue;
        }
        if durable.engine().buffer_len() > 0 {
            let result = durable.assign_batch(&waiter.consumer).map_err(Into::into);
            deliver(durable, observe, group, adapter, result, waiter.reply);
        } else if now >= waiter.deadline {
            let _ = waiter.reply.send(Err(CoreError::NoWork.into()));
        } else {
            still_waiting.push_back(waiter);
        }
    }
    *waiters = still_waiting;
}

/// Send `command()` to the owner every `period` until the owner is gone.
fn spawn_ticker(
    tx: mpsc::Sender<RuntimeCommand>,
    period: Duration,
    command: fn() -> RuntimeCommand,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if tx.send(command()).await.is_err() {
                break;
            }
        }
    })
}

/// Own the source and run one read per request. Results return to the owner
/// through its command mailbox.
fn spawn_fetcher(
    mut source: Box<dyn RecordSource>,
    owner: mpsc::Sender<RuntimeCommand>,
    group_label: String,
) -> (mpsc::Sender<FetchRequest>, JoinHandle<()>) {
    let (requests, mut incoming) = mpsc::channel::<FetchRequest>(1);
    let task = tokio::spawn(async move {
        while let Some(FetchRequest { cursor, limit }) = incoming.recv().await {
            let started = Instant::now();
            let result = source
                .fetch_after(&cursor, limit)
                .instrument(tracing::debug_span!("group_fetch", group_id = %group_label))
                .await;
            let reply = RuntimeCommand::Fetched {
                cursor,
                result,
                latency: started.elapsed(),
            };
            if owner.send(reply).await.is_err() {
                break;
            }
        }
    });
    (requests, task)
}

fn note_replay(observe: &Observe, group: &str, adapter: &str, before: usize, after: usize) {
    let replayed = before.saturating_sub(after);
    if replayed > 0 {
        observe.record_replay(group, adapter, replayed as u64);
    }
}

/// Apply one ack in memory. The caller writes the checkpoint.
fn apply_ack<S: StateStore>(
    durable: &mut DurableGroup<S>,
    observe: &Observe,
    group: &str,
    adapter: &str,
    batch_id: BatchId,
) -> RuntimeResult<()> {
    let started = Instant::now();
    let before_inflight = durable.engine().inflight_records();
    if durable.ack_in_memory(batch_id)? == AckOutcome::Stale {
        observe.record_stale_ack(group);
    }
    let acked = before_inflight.saturating_sub(durable.engine().inflight_records());
    if acked > 0 {
        observe.record_ack(group, adapter, acked as u64, started.elapsed());
    }
    Ok(())
}

pub(crate) fn map_join_result(
    result: Result<RuntimeResult<GroupExit>, tokio::task::JoinError>,
) -> RuntimeResult<()> {
    match result {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(RuntimeError::TaskFailed),
    }
}
