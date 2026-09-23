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

fn matching_handle<S: StateStore + 'static>(
    sup: &GroupSupervisor<S>,
    gid: &GroupId,
) -> GroupHandle {
    for _ in 0..50 {
        if let Some(h) = sup.get_handle(gid) {
            return h;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("handle missing after recover");
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
    tokio::time::sleep(Duration::from_millis(20)).await;
    let recovered = sup.supervise_once().await.unwrap();
    assert_eq!(recovered, vec![gid.clone()]);

    let h = sup.get_handle(&gid).unwrap();
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
                tokio::time::sleep(Duration::from_millis(10)).await;
                let _ = sup.supervise_once().await.unwrap();
                h = matching_handle(&sup, &gid);
                h.join(c.clone()).await.unwrap();
                continue;
            }
            Err(e) => panic!("{e}"),
        }

        if kills < 5 && acks_since_kill >= 2 {
            let cursor = h.snapshot_cursor().await.unwrap();
            let done = cursor == Some(OrderingValue::single_u64(total));
            if !done {
                kills += 1;
                acks_since_kill = 0;
                let _ = sup.abort_group(&gid);
                tokio::time::sleep(Duration::from_millis(15)).await;
                let _ = sup.supervise_once().await.unwrap();
                h = matching_handle(&sup, &gid);
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
async fn caps_hold_while_draining() {
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

    let watcher = {
        let h = h.clone();
        tokio::spawn(async move {
            for _ in 0..500 {
                let s = h.buffer_stats().await.unwrap();
                assert!(s.buffer_len <= s.max_buffer_records);
                assert!(s.buffer_bytes <= s.max_buffer_bytes);
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
    };
    drain_handle(&h, &c, 60).await;
    watcher.abort();
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
        Box::pin(async { Err(SourceError("database unavailable".into())) })
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
    );
    let err = spawned.join.await.unwrap().unwrap_err();
    assert!(err.to_string().contains("database unavailable"), "{err}");
}
