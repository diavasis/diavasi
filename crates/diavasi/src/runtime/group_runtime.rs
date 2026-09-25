use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::task::{AbortHandle, JoinHandle};
use tracing::Instrument;

use crate::core::{BatchId, RecordSource, SourceError};
use crate::observe::Observe;
use crate::store::{DurableGroup, StateStore};

use super::command::{BufferStats, LiveSnapshot, RuntimeCommand};
use super::error::{RuntimeError, RuntimeResult};
use super::handle::GroupHandle;

pub const DEFAULT_COMMAND_CAPACITY: usize = 64;
pub const DEFAULT_FETCH_INTERVAL: Duration = Duration::from_millis(5);
pub const DEFAULT_TICK_INTERVAL: Duration = Duration::from_millis(25);

#[derive(Debug, Clone)]
pub struct GroupRuntimeConfig {
    pub command_capacity: usize,
    pub fetch_interval: Duration,
    pub tick_interval: Duration,
}

impl Default for GroupRuntimeConfig {
    fn default() -> Self {
        Self {
            command_capacity: DEFAULT_COMMAND_CAPACITY,
            fetch_interval: DEFAULT_FETCH_INTERVAL,
            tick_interval: DEFAULT_TICK_INTERVAL,
        }
    }
}

pub struct SpawnedGroup {
    pub handle: GroupHandle,
    pub abort: AbortHandle,
    pub join: JoinHandle<RuntimeResult<()>>,
}

/// Spawn the owner task and fetch/timeout children for an already-opened group.
pub fn spawn_group_runtime<S>(
    mut durable: DurableGroup<S>,
    config: GroupRuntimeConfig,
    mut source: Option<Box<dyn RecordSource>>,
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

    let fetch_tx = tx.clone();
    let fetch_interval = config.fetch_interval;
    let fetch_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(fetch_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if fetch_tx.send(RuntimeCommand::Fetch).await.is_err() {
                break;
            }
        }
    });

    let tick_tx = tx;
    let tick_interval = config.tick_interval;
    let tick_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(tick_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if tick_tx.send(RuntimeCommand::Tick).await.is_err() {
                break;
            }
        }
    });

    let join = tokio::spawn(async move {
        let run = async {
            while let Some(cmd) = rx.recv().await {
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
                        match durable.assign_batch(&consumer) {
                            Ok(batch) => {
                                let id = batch.id;
                                let records = batch.records.len() as u64;
                                if reply.send(Ok(batch)).is_err() {
                                    let before = durable.engine().inflight_records();
                                    durable.engine_mut().requeue_batch(id);
                                    note_replay(
                                        &observe,
                                        &group_label,
                                        &adapter,
                                        before,
                                        durable.engine().inflight_records(),
                                    );
                                } else {
                                    observe.record_deliver(&group_label, &adapter, records);
                                }
                            }
                            Err(err) => {
                                let _ = reply.send(Err(err.into()));
                            }
                        }
                    }
                    RuntimeCommand::Ack { batch_id, reply } => {
                        let result = tracing::debug_span!("group_ack", group_id = %group_label)
                            .in_scope(|| {
                                apply_ack(&mut durable, &observe, &group_label, &adapter, batch_id)
                            });
                        if let Err(ref err) = result {
                            tracing::warn!(group_id = %group_label, error = %err, "ack failed");
                        }
                        let _ = reply.send(result);
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
                        let _ = reply.send(durable.drain().map_err(Into::into));
                    }
                    RuntimeCommand::Fetch => {
                        if let Some(source) = source.as_mut() {
                            let slots = durable.engine().buffer_free_records();
                            if slots == 0 {
                                continue;
                            }
                            let cursor = durable.engine().fetched_cursor().clone();
                            let started = Instant::now();
                            let fetched = source
                                .fetch_after(&cursor, slots)
                                .instrument(tracing::debug_span!(
                                    "group_fetch",
                                    group_id = %group_label
                                ))
                                .await;
                            let latency = started.elapsed();
                            match fetched {
                                Ok(records) => {
                                    let sizes: Vec<usize> =
                                        records.iter().map(|record| record.byte_len()).collect();
                                    match durable.engine_mut().ingest(records) {
                                        Ok(0) => {}
                                        Ok(accepted) => {
                                            let bytes: usize =
                                                sizes.into_iter().take(accepted).sum();
                                            observe.record_fetch(
                                                &group_label,
                                                &adapter,
                                                accepted as u64,
                                                bytes as u64,
                                                latency,
                                            );
                                        }
                                        Err(err) => return Err(err.into()),
                                    }
                                }
                                Err(SourceError(message)) => {
                                    observe.record_fetch(&group_label, &adapter, 0, 0, latency);
                                    observe.record_adapter_error(&group_label, &adapter);
                                    tracing::warn!(
                                        group_id = %group_label,
                                        adapter = %adapter,
                                        error = %message,
                                        "adapter fetch failed"
                                    );
                                    tokio::time::sleep(Duration::from_millis(200)).await;
                                    return Err(RuntimeError::Source(message));
                                }
                            }
                        } else {
                            let started = Instant::now();
                            match durable.poll_fetch() {
                                Ok(n) if n > 0 => {
                                    let bytes =
                                        n.saturating_mul(durable.engine().config().payload_size);
                                    observe.record_fetch(
                                        &group_label,
                                        &adapter,
                                        n as u64,
                                        bytes as u64,
                                        started.elapsed(),
                                    );
                                }
                                Ok(_) => {}
                                Err(err) => return Err(err.into()),
                            }
                        }
                    }
                    RuntimeCommand::Tick => {
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
                        let res = durable.snapshot_to_store().map_err(Into::into);
                        if res.is_ok() {
                            observe.record_checkpoint(&group_label, started.elapsed());
                        }
                        let _ = reply.send(res);
                        break;
                    }
                }
            }
            Ok(())
        };

        let result = run.await;
        fetch_task.abort();
        tick_task.abort();
        let _ = fetch_task.await;
        let _ = tick_task.await;
        result
    });

    let abort = join.abort_handle();
    SpawnedGroup {
        handle,
        abort,
        join,
    }
}

fn note_replay(observe: &Observe, group: &str, adapter: &str, before: usize, after: usize) {
    let replayed = before.saturating_sub(after);
    if replayed > 0 {
        observe.record_replay(group, adapter, replayed as u64);
    }
}

fn apply_ack<S: StateStore>(
    durable: &mut DurableGroup<S>,
    observe: &Observe,
    group: &str,
    adapter: &str,
    batch_id: BatchId,
) -> RuntimeResult<()> {
    let started = Instant::now();
    let before_cursor = durable.committed_cursor().clone();
    let before_inflight = durable.engine().inflight_records();
    durable.ack(batch_id)?;
    let elapsed = started.elapsed();
    let acked = before_inflight.saturating_sub(durable.engine().inflight_records());
    if acked > 0 {
        observe.record_ack(group, adapter, acked as u64, elapsed);
    }
    if durable.committed_cursor() != &before_cursor {
        observe.record_checkpoint(group, elapsed);
    }
    Ok(())
}

pub(crate) fn map_join_result(
    result: Result<RuntimeResult<()>, tokio::task::JoinError>,
) -> RuntimeResult<()> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(RuntimeError::TaskFailed),
    }
}
