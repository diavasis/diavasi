use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::task::{AbortHandle, JoinHandle};

use crate::core::{RecordSource, SourceError};
use crate::store::{DurableGroup, StateStore};

use super::command::{BufferStats, RuntimeCommand};
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
) -> SpawnedGroup
where
    S: StateStore + 'static,
{
    let group_id = durable.group_id().clone();
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
                        let _ = reply.send(durable.leave_consumer(&consumer).map_err(Into::into));
                    }
                    RuntimeCommand::Assign { consumer, reply } => {
                        let _ = reply.send(durable.assign_batch(&consumer).map_err(Into::into));
                    }
                    RuntimeCommand::Ack { batch_id, reply } => {
                        let _ = reply.send(durable.ack(batch_id).map_err(Into::into));
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
                            match source.fetch_after(&cursor, slots).await {
                                Ok(records) => {
                                    if let Err(err) = durable.engine_mut().ingest(records) {
                                        return Err(err.into());
                                    }
                                }
                                Err(SourceError(message)) => {
                                    tokio::time::sleep(Duration::from_millis(200)).await;
                                    return Err(RuntimeError::Source(message));
                                }
                            }
                        } else {
                            let _ = durable.poll_fetch();
                        }
                    }
                    RuntimeCommand::Tick => {
                        let _ = durable.tick(Instant::now());
                    }
                    RuntimeCommand::Stop { reply } => {
                        let res = durable.snapshot_to_store().map_err(Into::into);
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

pub(crate) fn map_join_result(
    result: Result<RuntimeResult<()>, tokio::task::JoinError>,
) -> RuntimeResult<()> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(RuntimeError::TaskFailed),
    }
}
