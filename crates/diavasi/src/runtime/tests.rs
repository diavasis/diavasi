use std::sync::Arc;
use std::time::Duration;

use crate::core::{ConsumerId, GroupConfig, GroupId, OrderingValue, RecordSource, SourceError};
use crate::runtime::{
    GroupHandle, GroupRuntimeConfig, GroupSupervisor, RuntimeError, spawn_group_runtime,
};
use crate::store::{DurableGroup, RedbStore, StateStore};

fn cfg(id: &str, total: u64, buf: usize, batch: usize) -> GroupConfig {
    GroupConfig {
        group_id: GroupId::new(id).unwrap(),
        total_records: total,
        payload_size: 8,
        max_buffer_records: buf,
        max_buffer_bytes: 1024 * 1024,
        batch_max_records: batch,
        batch_timeout: Duration::from_secs(60),
    }
}

fn temp_store() -> (tempfile::TempDir, Arc<RedbStore>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("diavasi.redb");
    let store = Arc::new(RedbStore::create(&path).unwrap());
    (dir, store)
}

fn persist_group(store: &Arc<RedbStore>, id: &str, total: u64, buf: usize, batch: usize) {
    let g = DurableGroup::create(
        Arc::clone(store),
        cfg(id, total, buf, batch),
        "synthetic-u64",
    )
    .unwrap();
    drop(g);
}

async fn drain_handle(handle: &GroupHandle, consumer: &ConsumerId, total: u64) {
    let mut idle = 0u32;
    loop {
        match handle.assign(consumer).await {
            Ok(batch) => {
                idle = 0;
                handle.ack(batch.id).await.unwrap();
            }
            Err(RuntimeError::Core(crate::core::CoreError::NoWork)) => {
                idle += 1;
                tokio::time::sleep(Duration::from_millis(5)).await;
                let cursor = handle.snapshot_cursor().await.unwrap();
                if cursor == Some(OrderingValue::single_u64(total)) {
                    break;
                }
                let stats = handle.buffer_stats().await.unwrap();
                if stats.buffer_len == 0 && stats.inflight_len == 0 && idle > 200 {
                    break;
                }
            }
            Err(e) => panic!("{e}"),
        }
        let cursor = handle.snapshot_cursor().await.unwrap();
        if cursor == Some(OrderingValue::single_u64(total)) {
            break;
        }
    }
    assert_eq!(
        handle.snapshot_cursor().await.unwrap(),
        Some(OrderingValue::single_u64(total))
    );
}

/// Drive the supervisor until the killed group runs again. Restarts wait
/// at least `RETRY_FIRST`.
async fn recovered_handle<S: StateStore + 'static>(
    sup: &mut GroupSupervisor<S>,
    gid: &GroupId,
) -> GroupHandle {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    // The killed task is still listed until the supervisor collects it.
    while sup.get_handle(gid).is_some() {
        sup.supervise_once().await.unwrap();
        assert!(
            std::time::Instant::now() < deadline,
            "killed group was not collected"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    loop {
        sup.supervise_once().await.unwrap();
        if let Some(handle) = sup.get_handle(gid) {
            return handle;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "group was not restarted"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn many_groups_isolated() {
    let (_dir, store) = temp_store();
    for id in ["g1", "g2", "g3"] {
        persist_group(&store, id, 20, 8, 4);
    }
    let mut sup =
        GroupSupervisor::new(Arc::clone(&store)).with_runtime_config(GroupRuntimeConfig {
            fetch_interval: Duration::from_millis(2),
            tick_interval: Duration::from_millis(20),
            ..Default::default()
        });

    let mut handles = Vec::new();
    for id in ["g1", "g2", "g3"] {
        let gid = GroupId::new(id).unwrap();
        let h = sup.start_group(&gid).await.unwrap();
        let c = ConsumerId::new(format!("c-{id}")).unwrap();
        h.join(c.clone()).await.unwrap();
        handles.push((h, c));
    }

    let mut tasks = Vec::new();
    for (h, c) in handles {
        tasks.push(tokio::spawn(async move {
            drain_handle(&h, &c, 20).await;
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(sup.list_running().len(), 3);
}

#[tokio::test]
async fn kill_one_others_healthy() {
    let (_dir, store) = temp_store();
    for id in ["ga", "gb", "gc"] {
        persist_group(&store, id, 40, 8, 4);
    }
    let mut sup =
        GroupSupervisor::new(Arc::clone(&store)).with_runtime_config(GroupRuntimeConfig {
            fetch_interval: Duration::from_millis(2),
            ..Default::default()
        });

    let ha = {
        let h = sup.start_group(&GroupId::new("ga").unwrap()).await.unwrap();
        h.join(ConsumerId::new("ca").unwrap()).await.unwrap();
        h
    };
    let hb = {
        let h = sup.start_group(&GroupId::new("gb").unwrap()).await.unwrap();
        h.join(ConsumerId::new("cb").unwrap()).await.unwrap();
        h
    };
    let hc = {
        let h = sup.start_group(&GroupId::new("gc").unwrap()).await.unwrap();
        h.join(ConsumerId::new("cc").unwrap()).await.unwrap();
        h
    };

    for _ in 0..3 {
        if let Ok(b) = ha.assign(&ConsumerId::new("ca").unwrap()).await {
            ha.ack(b.id).await.unwrap();
        } else {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    let before_b = hb.snapshot_cursor().await.unwrap();
    sup.abort_group(&GroupId::new("ga").unwrap()).unwrap();

    let tb = tokio::spawn({
        let hb = hb.clone();
        async move {
            drain_handle(&hb, &ConsumerId::new("cb").unwrap(), 40).await;
        }
    });
    let tc = tokio::spawn({
        let hc = hc.clone();
        async move {
            drain_handle(&hc, &ConsumerId::new("cc").unwrap(), 40).await;
        }
    });
    tb.await.unwrap();
    tc.await.unwrap();
    assert!(hb.snapshot_cursor().await.unwrap() >= before_b);
    assert_eq!(
        hc.snapshot_cursor().await.unwrap(),
        Some(OrderingValue::single_u64(40))
    );
}

#[tokio::test]
async fn killed_group_recovers() {
    let (_dir, store) = temp_store();
    persist_group(&store, "g1", 30, 8, 3);
    let mut sup =
        GroupSupervisor::new(Arc::clone(&store)).with_runtime_config(GroupRuntimeConfig {
            fetch_interval: Duration::from_millis(2),
            ..Default::default()
        });
    let gid = GroupId::new("g1").unwrap();
    let h = sup.start_group(&gid).await.unwrap();
    let c = ConsumerId::new("c1").unwrap();
    h.join(c.clone()).await.unwrap();

    for _ in 0..5 {
        loop {
            match h.assign(&c).await {
                Ok(b) => {
                    h.ack(b.id).await.unwrap();
                    break;
                }
                Err(RuntimeError::Core(crate::core::CoreError::NoWork)) => {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(e) => panic!("{e}"),
            }
        }
    }
    let durable_before = store.load_checkpoint(&gid).unwrap();
    assert!(durable_before.is_some());

    sup.abort_group(&gid).unwrap();
    let h = recovered_handle(&mut sup, &gid).await;
    assert!(sup.outcome(&gid).unwrap().recovered);
    assert_eq!(h.snapshot_cursor().await.unwrap(), durable_before);
    h.join(c.clone()).await.unwrap();
    drain_handle(&h, &c, 30).await;
}

#[tokio::test]
async fn repeated_kill_recover_soak() {
    let (_dir, store) = temp_store();
    let total = 80u64;
    persist_group(&store, "soak", total, 8, 4);
    let mut sup =
        GroupSupervisor::new(Arc::clone(&store)).with_runtime_config(GroupRuntimeConfig {
            fetch_interval: Duration::from_millis(2),
            ..Default::default()
        });
    let gid = GroupId::new("soak").unwrap();
    let mut h = sup.start_group(&gid).await.unwrap();
    let c = ConsumerId::new("c1").unwrap();
    h.join(c.clone()).await.unwrap();

    let mut kills = 0;
    let mut acks_since_kill = 0u32;
    let mut steps = 0;
    loop {
        steps += 1;
        assert!(steps < 5_000, "soak did not finish");
        match h.assign(&c).await {
            Ok(batch) => {
                h.ack(batch.id).await.unwrap();
                acks_since_kill += 1;
            }
            Err(RuntimeError::Core(crate::core::CoreError::NoWork)) => {
                tokio::time::sleep(Duration::from_millis(3)).await;
            }
            Err(RuntimeError::ChannelClosed) | Err(RuntimeError::ChannelFull) => {
                h = recovered_handle(&mut sup, &gid).await;
                h.join(c.clone()).await.unwrap();
                continue;
            }
            Err(e) => panic!("{e}"),
        }

        // Each kill doubles the restart delay, so three kills keep this short.
        if kills < 3 && acks_since_kill >= 2 {
            let cursor = h.snapshot_cursor().await.unwrap();
            let done = cursor == Some(OrderingValue::single_u64(total));
            if !done {
                kills += 1;
                acks_since_kill = 0;
                let _ = sup.abort_group(&gid);
                h = recovered_handle(&mut sup, &gid).await;
                h.join(c.clone()).await.unwrap();
            }
        }

        if h.snapshot_cursor().await.unwrap() == Some(OrderingValue::single_u64(total)) {
            break;
        }
    }
    assert!(kills >= 3, "expected several kills, got {kills}");
    assert_eq!(
        store.load_checkpoint(&gid).unwrap(),
        Some(OrderingValue::single_u64(total))
    );
}

#[tokio::test]
async fn backpressure_under_runtime() {
    let (_dir, store) = temp_store();
    persist_group(&store, "bp", 100, 4, 2);
    let mut sup =
        GroupSupervisor::new(Arc::clone(&store)).with_runtime_config(GroupRuntimeConfig {
            fetch_interval: Duration::from_millis(2),
            ..Default::default()
        });
    let h = sup.start_group(&GroupId::new("bp").unwrap()).await.unwrap();
    let mut saw_full = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let stats = h.buffer_stats().await.unwrap();
        assert!(stats.buffer_len <= stats.max_buffer_records);
        assert!(stats.buffer_bytes <= stats.max_buffer_bytes);
        if stats.buffer_len == stats.max_buffer_records {
            saw_full = true;
            for _ in 0..10 {
                tokio::time::sleep(Duration::from_millis(5)).await;
                let s = h.buffer_stats().await.unwrap();
                assert_eq!(s.buffer_len, s.max_buffer_records);
            }
            break;
        }
    }
    assert!(saw_full, "buffer never reached cap");
}

#[tokio::test]
async fn caps_hold_while_consuming() {
    let (_dir, store) = temp_store();
    persist_group(&store, "caps", 60, 5, 2);
    let mut sup =
        GroupSupervisor::new(Arc::clone(&store)).with_runtime_config(GroupRuntimeConfig {
            fetch_interval: Duration::from_millis(2),
            ..Default::default()
        });
    let h = sup
        .start_group(&GroupId::new("caps").unwrap())
        .await
        .unwrap();
    let c = ConsumerId::new("c1").unwrap();
    h.join(c.clone()).await.unwrap();

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let watcher = {
        let h = h.clone();
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let s = h.buffer_stats().await.unwrap();
                assert!(s.buffer_len <= s.max_buffer_records);
                assert!(s.buffer_bytes <= s.max_buffer_bytes);
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
    };
    drain_handle(&h, &c, 60).await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    // A failed assertion inside the watcher surfaces here as a panic.
    watcher.await.unwrap();
}

#[tokio::test]
async fn stop_group_no_respawn() {
    let (_dir, store) = temp_store();
    persist_group(&store, "g1", 10, 4, 2);
    let mut sup = GroupSupervisor::new(store);
    let gid = GroupId::new("g1").unwrap();
    let _ = sup.start_group(&gid).await.unwrap();
    sup.stop_group(&gid).await.unwrap();
    assert!(sup.get_handle(&gid).is_none());
    let recovered = sup.supervise_once().await.unwrap();
    assert!(recovered.is_empty());
}

struct FailSource;

impl RecordSource for FailSource {
    fn fetch_after<'a>(
        &'a mut self,
        _cursor: &'a crate::core::LogicalCursor,
        _limit: usize,
    ) -> futures::future::BoxFuture<'a, Result<Vec<crate::core::Record>, SourceError>> {
        Box::pin(async { Err(SourceError::Transient("database unavailable".into())) })
    }
}

#[tokio::test]
async fn source_fetch_error_stops_the_group_task() {
    let (_dir, store) = temp_store();
    persist_group(&store, "g1", 4, 4, 2);
    let gid = GroupId::new("g1").unwrap();
    let durable = DurableGroup::open(Arc::clone(&store), &gid).unwrap();
    let spawned = spawn_group_runtime(
        durable,
        GroupRuntimeConfig {
            fetch_interval: Duration::from_millis(5),
            ..Default::default()
        },
        Some(Box::new(FailSource)),
        crate::observe::Observe::new(),
        "synthetic".into(),
    );
    let err = spawned.join.await.unwrap().unwrap_err();
    assert!(err.to_string().contains("database unavailable"), "{err}");
}

async fn wait_recovered(
    sup: &mut GroupSupervisor<RedbStore>,
    gid: &GroupId,
) -> crate::runtime::GroupOutcome {
    for _ in 0..50 {
        sup.supervise_once().await.unwrap();
        if let Some(outcome) = sup.outcome(gid) {
            if outcome.recovered {
                return outcome;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("group did not recover");
}

#[tokio::test]
async fn abort_restarts_without_moving_the_cursor() {
    let (_dir, store) = temp_store();
    persist_group(&store, "g1", 20, 8, 4);
    let mut sup =
        GroupSupervisor::new(Arc::clone(&store)).with_runtime_config(GroupRuntimeConfig {
            fetch_interval: Duration::from_millis(5),
            tick_interval: Duration::from_millis(20),
            ..Default::default()
        });
    let gid = GroupId::new("g1").unwrap();
    sup.start_group(&gid).await.unwrap();
    sup.abort_group(&gid).unwrap();
    let outcome = wait_recovered(&mut sup, &gid).await;
    assert_eq!(
        outcome.last_stop_reason,
        Some(crate::runtime::StopReason::Aborted)
    );
    assert!(outcome.recovered);
    assert_eq!(sup.observe().counters("g1", "synthetic").restarts, 1);
    assert_eq!(store.load_checkpoint(&gid).unwrap(), None);
}

#[tokio::test]
async fn source_error_is_explicit_then_recovered() {
    let (_dir, store) = temp_store();
    persist_group(&store, "g1", 8, 4, 2);
    let mut sup =
        GroupSupervisor::new(Arc::clone(&store)).with_runtime_config(GroupRuntimeConfig {
            fetch_interval: Duration::from_millis(5),
            ..Default::default()
        });
    let gid = GroupId::new("g1").unwrap();
    sup.start_group_with_source(&gid, Some(Box::new(FailSource)))
        .await
        .unwrap();
    let outcome = wait_recovered(&mut sup, &gid).await;
    assert_eq!(
        outcome.last_stop_reason,
        Some(crate::runtime::StopReason::SourceUnavailable(
            "database unavailable".into()
        ))
    );
    assert!(outcome.recovered);
    assert!(sup.observe().counters("g1", "synthetic").adapter_errors >= 1);
    assert_eq!(store.load_checkpoint(&gid).unwrap(), None);
}

#[test]
fn checkpoint_write_failure_does_not_advance_the_cursor() {
    use crate::store::{CrashAction, CrashPoint, StoreError};

    let (dir, store) = temp_store();
    let mut group = DurableGroup::create(Arc::clone(&store), cfg("g1", 8, 10, 2), "synthetic-u64")
        .unwrap()
        .with_crash_hook(Arc::new(|point| {
            if point == CrashPoint::BeforeTxnCommit {
                CrashAction::Abort
            } else {
                CrashAction::Continue
            }
        }));
    group.start().unwrap();
    let consumer = ConsumerId::new("c1").unwrap();
    group.join_consumer(consumer.clone()).unwrap();
    assert_eq!(group.poll_fetch().unwrap(), 8);
    let batch = group.assign_batch(&consumer).unwrap();
    let err = group.ack(batch.id).unwrap_err();
    assert!(matches!(
        err,
        StoreError::SimulatedCrash(CrashPoint::BeforeTxnCommit)
    ));
    let gid = group.group_id().clone();
    drop(group);
    drop(store);
    let store = Arc::new(RedbStore::open(dir.path().join("diavasi.redb")).unwrap());
    let group = DurableGroup::open(store, &gid).unwrap();
    assert_eq!(group.committed_cursor(), &None);
}

#[tokio::test]
async fn slow_consumer_lag_stays_within_the_buffer_cap() {
    use crate::observe::GaugeSample;

    let (_dir, store) = temp_store();
    persist_group(&store, "g1", 30, 4, 2);
    let mut sup =
        GroupSupervisor::new(Arc::clone(&store)).with_runtime_config(GroupRuntimeConfig {
            fetch_interval: Duration::from_millis(2),
            tick_interval: Duration::from_secs(60),
            ..Default::default()
        });
    let gid = GroupId::new("g1").unwrap();
    let handle = sup.start_group(&gid).await.unwrap();
    let consumer = ConsumerId::new("c1").unwrap();
    handle.join(consumer.clone()).await.unwrap();
    let mut assigned = false;
    for _ in 0..100 {
        match handle.assign(&consumer).await {
            Ok(_) => {
                assigned = true;
                break;
            }
            Err(RuntimeError::Core(crate::core::CoreError::NoWork)) => {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(err) => panic!("{err}"),
        }
    }
    assert!(assigned, "consumer received no batch");
    let mut filled = None;
    for _ in 0..100 {
        let snap = handle.live_snapshot().await.unwrap();
        if snap.buffer_records == 4 && snap.inflight_records == 2 {
            filled = Some(snap);
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let snap = filled.expect("buffer did not fill while the consumer held a batch");
    assert!(snap.buffer_records <= 4);
    assert_eq!(snap.buffer_records + snap.inflight_records, 6);
    let text = sup.observe().render(
        1,
        &[GaugeSample {
            group_id: "g1".into(),
            buffer_records: snap.buffer_records as u64,
            buffer_bytes: snap.buffer_bytes as u64,
            inflight_records: snap.inflight_records as u64,
            checkpoint_lag: 6,
            consumer_count: snap.consumers.len() as u64,
        }],
    );
    assert!(
        text.contains("diavasi_group_checkpoint_lag{group_id=\"g1\"} 6"),
        "{text}"
    );
    assert!(
        text.contains("diavasi_group_inflight_records{group_id=\"g1\"} 2"),
        "{text}"
    );
}

// Regression tests for the v0.12.0 review. Each name carries its finding id.

use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::Instant;

use futures::future::BoxFuture;

use crate::core::{LogicalCursor, Record, SyntheticSource};
use crate::runtime::{SourceFactory, SourceOpen};
use crate::store::{ConnectionRecord, StoreKey, seal_secret};

/// Counts `open` calls. The first `fail_opens` calls fail. Opened sources
/// either always fail their fetch or read a small synthetic stream.
struct ScriptedFactory {
    opens: Arc<AtomicUsize>,
    fail_opens: usize,
    fetch_fails: bool,
}

impl SourceFactory for ScriptedFactory {
    fn kind(&self) -> &str {
        "scripted"
    }

    fn open(
        &self,
        _request: SourceOpen,
    ) -> BoxFuture<'static, Result<Box<dyn RecordSource>, String>> {
        let n = self.opens.fetch_add(1, AtomicOrdering::SeqCst) + 1;
        let fail_open = n <= self.fail_opens;
        let fetch_fails = self.fetch_fails;
        Box::pin(async move {
            if fail_open {
                return Err(format!("open {n} refused"));
            }
            if fetch_fails {
                Ok(Box::new(FailSource) as Box<dyn RecordSource>)
            } else {
                Ok(Box::new(SyntheticSource::new(8, 8)) as Box<dyn RecordSource>)
            }
        })
    }

    fn validate(&self, _request: SourceOpen) -> BoxFuture<'static, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
}

fn persist_scripted_group(store: &Arc<RedbStore>, key: &StoreKey, id: &str) {
    store
        .put_connection(&ConnectionRecord {
            id: "conn".into(),
            kind: "scripted".into(),
            config_json: serde_json::json!({}),
            sealed_secret: seal_secret(key, b"unused").unwrap(),
        })
        .unwrap();
    DurableGroup::create_with_source(
        Arc::clone(store),
        cfg(id, 8, 4, 2),
        "scripted",
        Some("conn".into()),
        Some(serde_json::json!({})),
    )
    .unwrap();
}

/// Blocks the first fetch for six seconds, then fails it.
struct StuckThenFailSource;

impl RecordSource for StuckThenFailSource {
    fn fetch_after<'a>(
        &'a mut self,
        _cursor: &'a LogicalCursor,
        _limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Record>, SourceError>> {
        Box::pin(async {
            tokio::time::sleep(Duration::from_secs(6)).await;
            Err(SourceError::Transient("stuck fetch failed".into()))
        })
    }
}

/// Returns nothing and counts the fetches.
struct CountingEmptySource(Arc<AtomicUsize>);

impl RecordSource for CountingEmptySource {
    fn fetch_after<'a>(
        &'a mut self,
        _cursor: &'a LogicalCursor,
        _limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Record>, SourceError>> {
        self.0.fetch_add(1, AtomicOrdering::SeqCst);
        Box::pin(async { Ok(Vec::new()) })
    }
}

/// Takes two seconds to return nothing.
struct SlowEmptySource;

impl RecordSource for SlowEmptySource {
    fn fetch_after<'a>(
        &'a mut self,
        _cursor: &'a LogicalCursor,
        _limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Record>, SourceError>> {
        Box::pin(async {
            tokio::time::sleep(Duration::from_secs(2)).await;
            Ok(Vec::new())
        })
    }
}

/// B4: when reopening the source fails, the supervisor keeps the group and
/// retries until it runs again.
#[tokio::test]
async fn regress_b04_failed_recovery_is_retried() {
    let (_dir, store) = temp_store();
    let key = StoreKey::generate();
    persist_scripted_group(&store, &key, "g1");
    let opens = Arc::new(AtomicUsize::new(0));
    let mut sup =
        GroupSupervisor::new(Arc::clone(&store)).with_runtime_config(GroupRuntimeConfig {
            fetch_interval: Duration::from_millis(5),
            ..Default::default()
        });
    sup.set_source_factory(
        Arc::new(ScriptedFactory {
            opens: Arc::clone(&opens),
            fail_opens: 2,
            fetch_fails: false,
        }),
        key,
    );
    let gid = GroupId::new("g1").unwrap();
    sup.start_group_with_source(&gid, Some(Box::new(FailSource)))
        .await
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let _ = sup.supervise_once().await;
        let recovered = sup.get_handle(&gid).is_some()
            && sup.outcome(&gid).is_some_and(|outcome| outcome.recovered);
        if recovered {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "group was not recovered; {} opens",
            opens.load(AtomicOrdering::SeqCst)
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(opens.load(AtomicOrdering::SeqCst) >= 3);
}

/// B5: a source that keeps failing is reopened with growing delays, not
/// several times a second.
#[tokio::test]
async fn regress_b05_persistent_source_errors_back_off() {
    let (_dir, store) = temp_store();
    let key = StoreKey::generate();
    persist_scripted_group(&store, &key, "g1");
    let opens = Arc::new(AtomicUsize::new(0));
    let mut sup =
        GroupSupervisor::new(Arc::clone(&store)).with_runtime_config(GroupRuntimeConfig {
            fetch_interval: Duration::from_millis(5),
            ..Default::default()
        });
    sup.set_source_factory(
        Arc::new(ScriptedFactory {
            opens: Arc::clone(&opens),
            fail_opens: 0,
            fetch_fails: true,
        }),
        key,
    );
    let gid = GroupId::new("g1").unwrap();
    sup.start_group(&gid).await.unwrap();

    let started = Instant::now();
    while started.elapsed() < Duration::from_millis(2_500) {
        let _ = sup.supervise_once().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let n = opens.load(AtomicOrdering::SeqCst);
    assert!(n <= 5, "{n} source opens in 2.5 s");
}

/// B7: a pause that fails must not leave the group marked as cleanly
/// stopped. Here the pause times out behind a stuck fetch, then the fetch
/// fails and the task exits on its own. The supervisor must recover it.
#[tokio::test]
async fn regress_b07_failed_pause_keeps_recovery() {
    let (_dir, store) = temp_store();
    persist_group(&store, "g1", 8, 4, 2);
    let mut sup =
        GroupSupervisor::new(Arc::clone(&store)).with_runtime_config(GroupRuntimeConfig {
            command_capacity: 1,
            fetch_interval: Duration::from_millis(5),
            tick_interval: Duration::from_millis(5),
            ..Default::default()
        });
    let gid = GroupId::new("g1").unwrap();
    sup.start_group_with_source(&gid, Some(Box::new(StuckThenFailSource)))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    if sup.stop_group(&gid).await.is_ok() {
        // The owner answered despite the stuck fetch. Nothing else to check.
        assert!(sup.get_handle(&gid).is_none());
        return;
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let _ = sup.supervise_once().await;
        if sup.get_handle(&gid).is_some()
            && sup.outcome(&gid).is_some_and(|outcome| outcome.recovered)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "group that failed after a failed pause was not recovered"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// P1: a group whose source has nothing new backs off instead of querying
/// every fetch interval.
#[tokio::test]
async fn regress_p01_idle_group_backs_off() {
    let (_dir, store) = temp_store();
    persist_group(&store, "g1", 8, 4, 2);
    let mut sup = GroupSupervisor::new(Arc::clone(&store));
    let gid = GroupId::new("g1").unwrap();
    let fetches = Arc::new(AtomicUsize::new(0));
    sup.start_group_with_source(
        &gid,
        Some(Box::new(CountingEmptySource(Arc::clone(&fetches)))),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let n = fetches.load(AtomicOrdering::SeqCst);
    assert!(n <= 30, "{n} empty fetches in one second");
}

/// P2: a slow source read does not block commands to the group owner.
#[tokio::test]
async fn regress_p02_owner_answers_while_a_fetch_is_slow() {
    let (_dir, store) = temp_store();
    persist_group(&store, "g1", 8, 4, 2);
    let mut sup = GroupSupervisor::new(Arc::clone(&store));
    let gid = GroupId::new("g1").unwrap();
    let handle = sup
        .start_group_with_source(&gid, Some(Box::new(SlowEmptySource)))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let started = Instant::now();
    let answered = tokio::time::timeout(Duration::from_secs(1), handle.live_snapshot()).await;
    let elapsed = started.elapsed();
    assert!(answered.is_ok(), "live_snapshot got no answer within 1 s");
    assert!(
        elapsed < Duration::from_millis(300),
        "live_snapshot waited {elapsed:?} behind a fetch"
    );
}

/// Counts checkpoint writes on top of a redb store.
struct CountingStore {
    inner: RedbStore,
    writes: AtomicUsize,
}

impl StateStore for CountingStore {
    fn schema_version(&self) -> u32 {
        self.inner.schema_version()
    }
    fn put_connection(&self, conn: &ConnectionRecord) -> crate::store::StoreResult<()> {
        self.inner.put_connection(conn)
    }
    fn get_connection(&self, id: &str) -> crate::store::StoreResult<Option<ConnectionRecord>> {
        self.inner.get_connection(id)
    }
    fn list_connections(&self) -> crate::store::StoreResult<Vec<ConnectionRecord>> {
        self.inner.list_connections()
    }
    fn delete_connection(&self, id: &str) -> crate::store::StoreResult<()> {
        self.inner.delete_connection(id)
    }
    fn put_group(&self, group: &crate::store::GroupRecord) -> crate::store::StoreResult<()> {
        self.inner.put_group(group)
    }
    fn insert_group(
        &self,
        group: &crate::store::GroupRecord,
        cursor: &LogicalCursor,
    ) -> crate::store::StoreResult<()> {
        self.inner.insert_group(group, cursor)
    }
    fn get_group(
        &self,
        id: &GroupId,
    ) -> crate::store::StoreResult<Option<crate::store::GroupRecord>> {
        self.inner.get_group(id)
    }
    fn list_groups(&self) -> crate::store::StoreResult<Vec<crate::store::GroupRecord>> {
        self.inner.list_groups()
    }
    fn delete_group(&self, id: &GroupId) -> crate::store::StoreResult<()> {
        self.inner.delete_group(id)
    }
    fn load_checkpoint(&self, id: &GroupId) -> crate::store::StoreResult<LogicalCursor> {
        self.inner.load_checkpoint(id)
    }
    fn commit_checkpoint(
        &self,
        id: &GroupId,
        cursor: &LogicalCursor,
    ) -> crate::store::StoreResult<()> {
        self.inner.commit_checkpoint(id, cursor)
    }
    fn commit_progress(
        &self,
        group: &crate::store::GroupRecord,
        cursor: &LogicalCursor,
    ) -> crate::store::StoreResult<()> {
        self.writes.fetch_add(1, AtomicOrdering::SeqCst);
        self.inner.commit_progress(group, cursor)
    }
    fn commit_progress_with_hook(
        &self,
        group: &crate::store::GroupRecord,
        cursor: &LogicalCursor,
        before_commit: &dyn Fn() -> crate::store::StoreResult<()>,
    ) -> crate::store::StoreResult<()> {
        self.writes.fetch_add(1, AtomicOrdering::SeqCst);
        self.inner
            .commit_progress_with_hook(group, cursor, before_commit)
    }
}

/// Start a synthetic group of `total` one-record batches on a counting store
/// and hand out every batch to one consumer.
async fn counted_group(
    config: GroupRuntimeConfig,
    total: u64,
) -> (
    tempfile::TempDir,
    Arc<CountingStore>,
    GroupHandle,
    Vec<crate::core::Batch>,
) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(CountingStore {
        inner: RedbStore::create(dir.path().join("diavasi.redb")).unwrap(),
        writes: AtomicUsize::new(0),
    });
    DurableGroup::create(
        Arc::clone(&store),
        cfg("g1", total, total as usize, 1),
        "synthetic-u64",
    )
    .unwrap();
    let durable = DurableGroup::open(Arc::clone(&store), &GroupId::new("g1").unwrap()).unwrap();
    let spawned = spawn_group_runtime(
        durable,
        config,
        None,
        crate::observe::Observe::new(),
        "synthetic".into(),
    );
    let handle = spawned.handle;
    let consumer = ConsumerId::new("c1").unwrap();
    handle.join(consumer.clone()).await.unwrap();
    let mut batches = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(3);
    while batches.len() < total as usize {
        match handle.assign(&consumer).await {
            Ok(batch) => batches.push(batch),
            Err(RuntimeError::Core(crate::core::CoreError::NoWork)) => {
                assert!(Instant::now() < deadline, "batches were not fetched");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(err) => panic!("{err}"),
        }
    }
    (dir, store, handle, batches)
}

/// P4: acks that wait in the mailbox together share one checkpoint write,
/// and each is answered only after that write.
#[tokio::test]
async fn regress_p04_waiting_acks_share_one_checkpoint_write() {
    let config = GroupRuntimeConfig {
        fetch_interval: Duration::from_millis(5),
        tick_interval: Duration::from_secs(60),
        ..Default::default()
    };
    let (_dir, store, handle, batches) = counted_group(config, 8).await;
    let before = store.writes.load(AtomicOrdering::SeqCst);
    let results = futures::future::join_all(batches.iter().map(|b| handle.ack(b.id))).await;
    assert!(results.iter().all(Result::is_ok));
    let writes = store.writes.load(AtomicOrdering::SeqCst) - before;
    assert!(writes <= 3, "{writes} checkpoint writes for 8 waiting acks");
    assert_eq!(
        store.load_checkpoint(&GroupId::new("g1").unwrap()).unwrap(),
        Some(OrderingValue::single_u64(8)),
        "acks were answered before the checkpoint was written"
    );
}

/// P4: with a checkpoint interval, acks are answered at once and the
/// checkpoint follows within about one interval.
#[tokio::test]
async fn regress_p04_checkpoint_interval_writes_later() {
    let config = GroupRuntimeConfig {
        fetch_interval: Duration::from_millis(5),
        tick_interval: Duration::from_millis(20),
        checkpoint_interval: Duration::from_millis(300),
        ..Default::default()
    };
    let (_dir, store, handle, batches) = counted_group(config, 2).await;
    let gid = GroupId::new("g1").unwrap();
    let start = store.load_checkpoint(&gid).unwrap();
    handle.ack(batches[0].id).await.unwrap();
    handle.ack(batches[1].id).await.unwrap();
    assert_eq!(
        handle.snapshot_cursor().await.unwrap(),
        Some(OrderingValue::single_u64(2))
    );
    assert_eq!(
        store.load_checkpoint(&gid).unwrap(),
        start,
        "written before the interval"
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    while store.load_checkpoint(&gid).unwrap() != Some(OrderingValue::single_u64(2)) {
        assert!(Instant::now() < deadline, "checkpoint was never written");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Returns nothing until `ready_at`, then records 1 and 2 once.
struct LateSource {
    ready_at: Instant,
    sent: bool,
}

impl RecordSource for LateSource {
    fn fetch_after<'a>(
        &'a mut self,
        _cursor: &'a LogicalCursor,
        _limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Record>, SourceError>> {
        let records = if !self.sent && Instant::now() >= self.ready_at {
            self.sent = true;
            (1..=2)
                .map(|id| Record {
                    ordering: OrderingValue::single_u64(id),
                    payload: bytes::Bytes::from_static(b"x"),
                })
                .collect()
        } else {
            Vec::new()
        };
        Box::pin(async move { Ok(records) })
    }
}

/// P6: a consumer waiting for records gets them as soon as they arrive, and
/// hears `NoWork` only when its wait runs out, without polling.
#[tokio::test]
async fn regress_p06_assign_wait_answers_when_records_arrive() {
    let (_dir, store) = temp_store();
    persist_group(&store, "g1", 8, 8, 2);
    let mut sup =
        GroupSupervisor::new(Arc::clone(&store)).with_runtime_config(GroupRuntimeConfig {
            idle_fetch_max: Duration::from_millis(50),
            ..Default::default()
        });
    let gid = GroupId::new("g1").unwrap();
    let source = LateSource {
        ready_at: Instant::now() + Duration::from_millis(300),
        sent: false,
    };
    let handle = sup
        .start_group_with_source(&gid, Some(Box::new(source)))
        .await
        .unwrap();
    let consumer = ConsumerId::new("c1").unwrap();
    handle.join(consumer.clone()).await.unwrap();

    let started = Instant::now();
    let batch = handle
        .assign_wait(&consumer, Duration::from_secs(3))
        .await
        .expect("records arrived during the wait");
    let waited = started.elapsed();
    assert_eq!(batch.records.len(), 2);
    assert!(waited < Duration::from_secs(1), "answered after {waited:?}");

    let started = Instant::now();
    let idle = handle
        .assign_wait(&consumer, Duration::from_millis(200))
        .await;
    assert!(matches!(
        idle,
        Err(RuntimeError::Core(crate::core::CoreError::NoWork))
    ));
    assert!(started.elapsed() >= Duration::from_millis(200));
}
