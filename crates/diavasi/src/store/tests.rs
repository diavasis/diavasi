//! Store CRUD, durability, and crash-injection recovery tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::Duration;

use crate::core::{BatchId, ConsumerId, CoreError, GroupConfig, GroupId, OrderingValue};
use crate::store::crash::{CrashAction, CrashPoint};
use crate::store::crypto::{StoreKey, open_secret, seal_secret};
use crate::store::durable::DurableGroup;
use crate::store::error::StoreError;
use crate::store::redb::RedbStore;
use crate::store::trait_::StateStore;
use crate::store::types::{ConnectionRecord, GroupRecord};

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

fn reopen(dir: &tempfile::TempDir) -> Arc<RedbStore> {
    let path = dir.path().join("diavasi.redb");
    Arc::new(RedbStore::open(&path).unwrap())
}

fn drain_all<S: StateStore>(g: &mut DurableGroup<S>, consumer: &ConsumerId) {
    let mut idle = 0u32;
    loop {
        let _ = g.poll_fetch().unwrap();
        match g.assign_batch(consumer) {
            Ok(batch) => {
                idle = 0;
                g.ack(batch.id).unwrap();
            }
            Err(StoreError::Core(CoreError::NoWork)) => {
                idle += 1;
                if g.engine().buffer_len() == 0 && g.engine().inflight_len() == 0 {
                    let n = g.poll_fetch().unwrap();
                    if n == 0 {
                        break;
                    }
                }
                assert!(idle < 100, "drain stuck");
            }
            Err(e) => panic!("{e}"),
        }
    }
}

fn abort_at(point: CrashPoint) -> crate::store::CrashHook {
    Arc::new(move |p| {
        if p == point {
            CrashAction::Abort
        } else {
            CrashAction::Continue
        }
    })
}

#[test]
fn schema_open_reopen_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("diavasi.redb");
    {
        let store = RedbStore::create(&path).unwrap();
        assert_eq!(store.schema_version(), 1);
    }
    let store = RedbStore::open(&path).unwrap();
    assert_eq!(store.schema_version(), 1);
    assert!(store.list_groups().unwrap().is_empty());
}

#[test]
fn connection_crud_secrets_sealed() {
    let (dir, store) = temp_store();
    let key = StoreKey::generate();
    let sealed = seal_secret(&key, b"super-secret-password").unwrap();
    let conn = ConnectionRecord {
        id: "c1".into(),
        kind: "synthetic".into(),
        config_json: serde_json::json!({"host": "localhost"}),
        sealed_secret: sealed,
    };
    store.put_connection(&conn).unwrap();
    let got = store.get_connection("c1").unwrap().unwrap();
    assert_eq!(got.kind, "synthetic");
    assert_ne!(got.sealed_secret.ciphertext, b"super-secret-password");
    let plain = open_secret(&key, &got.sealed_secret).unwrap();
    assert_eq!(plain, b"super-secret-password");

    drop(store);
    let bytes = std::fs::read(dir.path().join("diavasi.redb")).unwrap();
    let needle = b"super-secret-password";
    assert!(
        !bytes.windows(needle.len()).any(|w| w == needle),
        "plaintext password found in DB file"
    );

    let store = reopen(&dir);
    assert_eq!(store.list_connections().unwrap().len(), 1);
    store.delete_connection("c1").unwrap();
    assert!(store.get_connection("c1").unwrap().is_none());
}

#[test]
fn group_and_checkpoint_round_trip() {
    let (_dir, store) = temp_store();
    let config = cfg("g1", 10, 4, 2);
    let group = GroupRecord {
        config: config.clone(),
        lifecycle: crate::core::GroupLifecycle::Stopped,
        next_batch_id: 1,
        ordering_contract: "synthetic-u64".into(),
        connection_id: None,
        source_spec: None,
    };
    store.put_group(&group).unwrap();
    let cursor = Some(OrderingValue::single_u64(4));
    store.commit_checkpoint(&config.group_id, &cursor).unwrap();
    let loaded = store.get_group(&config.group_id).unwrap().unwrap();
    assert_eq!(loaded.config.total_records, 10);
    assert_eq!(store.load_checkpoint(&config.group_id).unwrap(), cursor);
}

#[test]
fn ack_advances_durable_cursor() {
    let (dir, store) = temp_store();
    let mut g =
        DurableGroup::create(Arc::clone(&store), cfg("g1", 6, 10, 2), "synthetic-u64").unwrap();
    g.start().unwrap();
    let c = ConsumerId::new("c1").unwrap();
    g.join_consumer(c.clone()).unwrap();
    assert_eq!(g.poll_fetch().unwrap(), 6);
    let b1 = g.assign_batch(&c).unwrap();
    g.ack(b1.id).unwrap();
    assert_eq!(g.committed_cursor(), &Some(OrderingValue::single_u64(2)));
    let gid = g.group_id().clone();
    drop(g);
    drop(store);

    let store = reopen(&dir);
    let g = DurableGroup::open(store, &gid).unwrap();
    assert_eq!(g.committed_cursor(), &Some(OrderingValue::single_u64(2)));
}

#[test]
fn crash_before_txn_commit_keeps_previous_cursor() {
    let (dir, store) = temp_store();
    let mut g = DurableGroup::create(Arc::clone(&store), cfg("g1", 8, 10, 2), "synthetic-u64")
        .unwrap()
        .with_crash_hook(abort_at(CrashPoint::BeforeTxnCommit));
    g.start().unwrap();
    let c = ConsumerId::new("c1").unwrap();
    g.join_consumer(c.clone()).unwrap();
    assert_eq!(g.poll_fetch().unwrap(), 8);
    let b1 = g.assign_batch(&c).unwrap();
    let err = g.ack(b1.id).unwrap_err();
    assert!(matches!(
        err,
        StoreError::SimulatedCrash(CrashPoint::BeforeTxnCommit)
    ));
    let gid = g.group_id().clone();
    drop(g);
    drop(store);

    let store = reopen(&dir);
    let mut g = DurableGroup::open(store, &gid).unwrap();
    assert_eq!(g.committed_cursor(), &None);
    g.join_consumer(c.clone()).unwrap();
    drain_all(&mut g, &c);
    assert_eq!(g.committed_cursor(), &Some(OrderingValue::single_u64(8)));
}

#[test]
fn crash_after_txn_commit_keeps_new_cursor() {
    let (dir, store) = temp_store();
    let mut g = DurableGroup::create(Arc::clone(&store), cfg("g1", 8, 10, 2), "synthetic-u64")
        .unwrap()
        .with_crash_hook(abort_at(CrashPoint::AfterTxnCommit));
    g.start().unwrap();
    let c = ConsumerId::new("c1").unwrap();
    g.join_consumer(c.clone()).unwrap();
    assert_eq!(g.poll_fetch().unwrap(), 8);
    let b1 = g.assign_batch(&c).unwrap();
    let err = g.ack(b1.id).unwrap_err();
    assert!(matches!(
        err,
        StoreError::SimulatedCrash(CrashPoint::AfterTxnCommit)
    ));
    let gid = g.group_id().clone();
    drop(g);
    drop(store);

    let store = reopen(&dir);
    let mut g = DurableGroup::open(store, &gid).unwrap();
    assert_eq!(g.committed_cursor(), &Some(OrderingValue::single_u64(2)));
    g.join_consumer(c.clone()).unwrap();
    drain_all(&mut g, &c);
    assert_eq!(g.committed_cursor(), &Some(OrderingValue::single_u64(8)));
}

#[test]
fn each_crash_point_allows_safe_resume() {
    let points = [
        CrashPoint::AfterDeliver,
        CrashPoint::AfterAckApplied,
        CrashPoint::BeforeCheckpointCompute,
        CrashPoint::BeforeTxnBegin,
        CrashPoint::BeforeTxnCommit,
        CrashPoint::AfterTxnCommit,
        CrashPoint::AfterAckResponse,
    ];
    for point in points {
        let (dir, store) = temp_store();
        let gid = GroupId::new(format!("g-{point:?}")).unwrap();
        let mut config = cfg(gid.as_str(), 10, 10, 2);
        config.group_id = gid.clone();
        let mut g = DurableGroup::create(Arc::clone(&store), config, "synthetic-u64")
            .unwrap()
            .with_crash_hook(abort_at(point));
        g.start().unwrap();
        let c = ConsumerId::new("c1").unwrap();
        g.join_consumer(c.clone()).unwrap();
        let _ = g.poll_fetch().unwrap();

        match point {
            CrashPoint::AfterDeliver => {
                let err = g.assign_batch(&c).unwrap_err();
                assert!(matches!(
                    err,
                    StoreError::SimulatedCrash(CrashPoint::AfterDeliver)
                ));
            }
            CrashPoint::AfterAckResponse => {
                let b = g.assign_batch(&c).unwrap();
                let err = g.ack(b.id).unwrap_err();
                assert!(matches!(
                    err,
                    StoreError::SimulatedCrash(CrashPoint::AfterAckResponse)
                ));
                // Cursor already durable when response hook fires after successful commit.
                assert_eq!(g.committed_cursor(), &Some(OrderingValue::single_u64(2)));
            }
            CrashPoint::AfterTxnCommit => {
                let b = g.assign_batch(&c).unwrap();
                let err = g.ack(b.id).unwrap_err();
                assert!(matches!(
                    err,
                    StoreError::SimulatedCrash(CrashPoint::AfterTxnCommit)
                ));
            }
            p => {
                let b = g.assign_batch(&c).unwrap();
                let err = g.ack(b.id).unwrap_err();
                assert!(
                    matches!(err, StoreError::SimulatedCrash(pt) if pt == p),
                    "point={point:?} err={err:?}"
                );
            }
        }

        drop(g);
        drop(store);
        let store = reopen(&dir);
        let mut g = DurableGroup::open(store, &gid).unwrap();
        g.join_consumer(c.clone()).unwrap();
        drain_all(&mut g, &c);
        assert_eq!(
            g.committed_cursor(),
            &Some(OrderingValue::single_u64(10)),
            "failed to drain after crash at {point:?}"
        );
    }
}

#[test]
fn restart_soak_no_omissions() {
    let (dir, _initial) = temp_store();
    let total = 40u64;
    let gid = GroupId::new("soak").unwrap();
    let path = dir.path().join("diavasi.redb");
    // Recreate cleanly via existing file from temp_store.
    drop(_initial);
    let store = Arc::new(RedbStore::create(&path).unwrap());

    let crash_count = Arc::new(AtomicUsize::new(0));
    let make_hook = |counter: Arc<AtomicUsize>| -> crate::store::CrashHook {
        Arc::new(move |p| {
            if p == CrashPoint::BeforeTxnCommit {
                let n = counter.fetch_add(1, AtomicOrdering::SeqCst);
                if n < 8 && n % 2 == 0 {
                    return CrashAction::Abort;
                }
            }
            CrashAction::Continue
        })
    };

    let mut g = DurableGroup::create(
        Arc::clone(&store),
        cfg("soak", total, 8, 3),
        "synthetic-u64",
    )
    .unwrap()
    .with_crash_hook(make_hook(Arc::clone(&crash_count)));
    g.start().unwrap();
    let c = ConsumerId::new("c1").unwrap();
    g.join_consumer(c.clone()).unwrap();

    let mut seen_keys = std::collections::BTreeSet::new();
    let mut steps = 0usize;
    drop(store);

    loop {
        steps += 1;
        assert!(steps < 1_000, "soak did not finish");
        let _ = g.poll_fetch().unwrap();
        match g.assign_batch(&c) {
            Ok(batch) => {
                for r in &batch.records {
                    if let [crate::core::OrderingAtom::U64(v)] = r.ordering.atoms() {
                        seen_keys.insert(*v);
                    }
                }
                let id = batch.id;
                match g.ack(id) {
                    Ok(_) => {}
                    Err(StoreError::SimulatedCrash(_)) => {
                        drop(g);
                        let store = reopen(&dir);
                        let durable = store.load_checkpoint(&gid).unwrap();
                        g = DurableGroup::open(store, &gid)
                            .unwrap()
                            .with_crash_hook(make_hook(Arc::clone(&crash_count)));
                        assert!(
                            g.committed_cursor() == &durable,
                            "recovered cursor mismatch"
                        );
                        g.join_consumer(c.clone()).unwrap();
                    }
                    Err(e) => panic!("{e}"),
                }
            }
            Err(StoreError::Core(CoreError::NoWork)) => {
                if g.engine().buffer_len() == 0 && g.engine().inflight_len() == 0 {
                    let n = g.poll_fetch().unwrap();
                    if n == 0 {
                        break;
                    }
                }
            }
            Err(e) => panic!("{e}"),
        }
        if g.committed_cursor() == &Some(OrderingValue::single_u64(total)) {
            break;
        }
    }

    assert_eq!(
        g.committed_cursor(),
        &Some(OrderingValue::single_u64(total))
    );
    for k in 1..=total {
        assert!(
            seen_keys.contains(&k),
            "key {k} never appeared in an assigned batch (at-least-once accounting)"
        );
    }
}

#[test]
fn out_of_order_ack_only_persists_contiguous() {
    let (_dir, store) = temp_store();
    let mut g = DurableGroup::create(store, cfg("g1", 6, 10, 2), "synthetic-u64").unwrap();
    g.start().unwrap();
    let c = ConsumerId::new("c1").unwrap();
    g.join_consumer(c.clone()).unwrap();
    assert_eq!(g.poll_fetch().unwrap(), 6);
    let b1 = g.assign_batch(&c).unwrap();
    let b2 = g.assign_batch(&c).unwrap();
    let b3 = g.assign_batch(&c).unwrap();
    g.ack(b2.id).unwrap();
    g.ack(b3.id).unwrap();
    assert_eq!(g.committed_cursor(), &None);
    g.ack(b1.id).unwrap();
    assert_eq!(g.committed_cursor(), &Some(OrderingValue::single_u64(6)));
}

#[test]
fn duplicate_ack_noop() {
    let (_dir, store) = temp_store();
    let mut g = DurableGroup::create(store, cfg("g1", 2, 10, 2), "synthetic-u64").unwrap();
    g.start().unwrap();
    let c = ConsumerId::new("c1").unwrap();
    g.join_consumer(c.clone()).unwrap();
    let _ = g.poll_fetch().unwrap();
    let b = g.assign_batch(&c).unwrap();
    g.ack(b.id).unwrap();
    let committed = g.committed_cursor().clone();
    g.ack(b.id).unwrap();
    g.ack(BatchId::from_u64(999)).unwrap();
    assert_eq!(g.committed_cursor(), &committed);
}

#[test]
fn list_connections_and_groups_sorted() {
    let (_dir, store) = temp_store();
    let key = StoreKey::generate();
    for id in ["z-conn", "a-conn", "m-conn"] {
        store
            .put_connection(&ConnectionRecord {
                id: id.into(),
                kind: "synthetic".into(),
                config_json: serde_json::json!({}),
                sealed_secret: seal_secret(&key, b"x").unwrap(),
            })
            .unwrap();
    }
    let ids: Vec<_> = store
        .list_connections()
        .unwrap()
        .into_iter()
        .map(|c| c.id)
        .collect();
    assert_eq!(ids, vec!["a-conn", "m-conn", "z-conn"]);

    for id in ["g-z", "g-a", "g-m"] {
        let mut config = cfg(id, 1, 1, 1);
        config.group_id = GroupId::new(id).unwrap();
        store
            .put_group(&GroupRecord {
                config,
                lifecycle: crate::core::GroupLifecycle::Stopped,
                next_batch_id: 1,
                ordering_contract: "synthetic-u64".into(),
                connection_id: None,
                source_spec: None,
            })
            .unwrap();
    }
    let gids: Vec<_> = store
        .list_groups()
        .unwrap()
        .into_iter()
        .map(|g| g.group_id().as_str().to_string())
        .collect();
    assert_eq!(gids, vec!["g-a", "g-m", "g-z"]);
}

#[test]
fn missing_gets_and_delete_group_clears_checkpoint() {
    let (_dir, store) = temp_store();
    assert!(store.get_connection("missing").unwrap().is_none());
    let gid = GroupId::new("gone").unwrap();
    assert!(store.get_group(&gid).unwrap().is_none());
    assert_eq!(store.load_checkpoint(&gid).unwrap(), None);

    let config = cfg("gone", 3, 2, 1);
    store
        .put_group(&GroupRecord {
            config: config.clone(),
            lifecycle: crate::core::GroupLifecycle::Stopped,
            next_batch_id: 1,
            ordering_contract: "synthetic-u64".into(),
            connection_id: None,
            source_spec: None,
        })
        .unwrap();
    store
        .commit_checkpoint(&gid, &Some(OrderingValue::single_u64(2)))
        .unwrap();
    assert!(store.load_checkpoint(&gid).unwrap().is_some());
    store.delete_group(&gid).unwrap();
    assert!(store.get_group(&gid).unwrap().is_none());
    assert_eq!(store.load_checkpoint(&gid).unwrap(), None);
}

#[test]
fn commit_progress_updates_group_and_cursor_atomically() {
    let (_dir, store) = temp_store();
    let config = cfg("g1", 5, 2, 1);
    let mut group = GroupRecord {
        config: config.clone(),
        lifecycle: crate::core::GroupLifecycle::Running,
        next_batch_id: 1,
        ordering_contract: "synthetic-u64".into(),
        connection_id: None,
        source_spec: None,
    };
    store.put_group(&group).unwrap();
    group.next_batch_id = 9;
    group.lifecycle = crate::core::GroupLifecycle::Draining;
    let cursor = Some(OrderingValue::single_u64(3));
    store.commit_progress(&group, &cursor).unwrap();
    let loaded = store.get_group(&config.group_id).unwrap().unwrap();
    assert_eq!(loaded.next_batch_id, 9);
    assert_eq!(loaded.lifecycle, crate::core::GroupLifecycle::Draining);
    assert_eq!(store.load_checkpoint(&config.group_id).unwrap(), cursor);
}

#[test]
fn connection_and_group_overwrite() {
    let (_dir, store) = temp_store();
    let key = StoreKey::generate();
    let mut conn = ConnectionRecord {
        id: "c1".into(),
        kind: "synthetic".into(),
        config_json: serde_json::json!({"n": 1}),
        sealed_secret: seal_secret(&key, b"a").unwrap(),
    };
    store.put_connection(&conn).unwrap();
    conn.config_json = serde_json::json!({"n": 2});
    conn.kind = "postgres".into();
    store.put_connection(&conn).unwrap();
    let got = store.get_connection("c1").unwrap().unwrap();
    assert_eq!(got.kind, "postgres");
    assert_eq!(got.config_json["n"], 2);

    let mut group = GroupRecord {
        config: cfg("g1", 1, 1, 1),
        lifecycle: crate::core::GroupLifecycle::Stopped,
        next_batch_id: 1,
        ordering_contract: "v1".into(),
        connection_id: None,
        source_spec: None,
    };
    store.put_group(&group).unwrap();
    group.ordering_contract = "v2".into();
    store.put_group(&group).unwrap();
    assert_eq!(
        store
            .get_group(&group.config.group_id)
            .unwrap()
            .unwrap()
            .ordering_contract,
        "v2"
    );
}

#[test]
fn schema_version_too_new_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("diavasi.redb");
    {
        let store = RedbStore::create(&path).unwrap();
        drop(store);
    }
    // Bump schema_version past what this binary supports.
    {
        use ::redb::{Database, TableDefinition};
        const META: TableDefinition<'_, &str, u32> = TableDefinition::new("meta");
        let db = Database::open(&path).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut meta = txn.open_table(META).unwrap();
            meta.insert("schema_version", 99u32).unwrap();
        }
        txn.commit().unwrap();
    }
    let err = match RedbStore::open(&path) {
        Err(e) => e,
        Ok(_) => panic!("expected schema version error"),
    };
    assert!(
        matches!(
            err,
            StoreError::SchemaVersion {
                found: 99,
                supported: 1
            }
        ),
        "{err:?}"
    );
}

#[test]
fn schema_version_too_old_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("diavasi.redb");
    {
        let _ = RedbStore::create(&path).unwrap();
    }
    {
        use ::redb::{Database, TableDefinition};
        const META: TableDefinition<'_, &str, u32> = TableDefinition::new("meta");
        let db = Database::open(&path).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut meta = txn.open_table(META).unwrap();
            meta.insert("schema_version", 0u32).unwrap();
        }
        txn.commit().unwrap();
    }
    let err = match RedbStore::open(&path) {
        Err(e) => e,
        Ok(_) => panic!("expected schema version error"),
    };
    assert!(
        matches!(
            err,
            StoreError::SchemaVersion {
                found: 0,
                supported: 1
            }
        ),
        "{err:?}"
    );
}

#[test]
fn open_missing_group_errors() {
    let (_dir, store) = temp_store();
    let err = match DurableGroup::open(store, &GroupId::new("nope").unwrap()) {
        Err(e) => e,
        Ok(_) => panic!("expected GroupNotFound"),
    };
    assert!(matches!(err, StoreError::GroupNotFound(_)));
}

#[test]
fn snapshot_to_store_persists_progress_without_ack_path() {
    let (dir, store) = temp_store();
    let mut g =
        DurableGroup::create(Arc::clone(&store), cfg("g1", 10, 10, 2), "synthetic-u64").unwrap();
    g.start().unwrap();
    let c = ConsumerId::new("c1").unwrap();
    g.join_consumer(c.clone()).unwrap();
    assert_eq!(g.poll_fetch().unwrap(), 10);
    let b = g.assign_batch(&c).unwrap();
    // Advance in memory via engine_mut, then snapshot (controlled shutdown path).
    g.engine_mut().ack(b.id).unwrap();
    assert_eq!(g.committed_cursor(), &Some(OrderingValue::single_u64(2)));
    g.snapshot_to_store().unwrap();
    let gid = g.group_id().clone();
    let next_batch = g.engine().snapshot().next_batch_id;
    drop(g);
    drop(store);

    let store = reopen(&dir);
    let g = DurableGroup::open(store, &gid).unwrap();
    assert_eq!(g.committed_cursor(), &Some(OrderingValue::single_u64(2)));
    assert_eq!(g.engine().snapshot().next_batch_id, next_batch);
}

#[test]
fn next_batch_id_survives_ack_persist_and_reopen() {
    let (dir, store) = temp_store();
    let mut g =
        DurableGroup::create(Arc::clone(&store), cfg("g1", 6, 10, 2), "synthetic-u64").unwrap();
    g.start().unwrap();
    let c = ConsumerId::new("c1").unwrap();
    g.join_consumer(c.clone()).unwrap();
    let _ = g.poll_fetch().unwrap();
    let b1 = g.assign_batch(&c).unwrap();
    let b2 = g.assign_batch(&c).unwrap();
    assert_eq!(b1.id.as_u64(), 1);
    assert_eq!(b2.id.as_u64(), 2);
    g.ack(b1.id).unwrap();
    let gid = g.group_id().clone();
    drop(g);
    drop(store);

    let store = reopen(&dir);
    let mut g = DurableGroup::open(store, &gid).unwrap();
    g.join_consumer(c.clone()).unwrap();
    let _ = g.poll_fetch().unwrap();
    let b3 = g.assign_batch(&c).unwrap();
    // Recovered next_batch_id must not reuse 1/2.
    assert!(b3.id.as_u64() >= 3, "reused batch id {}", b3.id);
}

#[test]
fn tick_timeout_requeues_then_completes_durably() {
    let (dir, store) = temp_store();
    let mut config = cfg("g1", 4, 10, 2);
    config.batch_timeout = Duration::from_millis(1);
    let mut g = DurableGroup::create(Arc::clone(&store), config, "synthetic-u64").unwrap();
    g.start().unwrap();
    let c = ConsumerId::new("c1").unwrap();
    g.join_consumer(c.clone()).unwrap();
    let _ = g.poll_fetch().unwrap();
    let b = g.assign_batch(&c).unwrap();
    let later = std::time::Instant::now() + Duration::from_secs(1);
    assert_eq!(g.tick(later).unwrap(), 1);
    assert_eq!(g.engine().inflight_len(), 0);
    // Re-assign and ack to durable completion.
    let b2 = g.assign_batch(&c).unwrap();
    assert_eq!(b2.records[0].ordering, b.records[0].ordering);
    g.ack(b2.id).unwrap();
    let gid = g.group_id().clone();
    drop(g);
    drop(store);
    let g = DurableGroup::open(reopen(&dir), &gid).unwrap();
    assert_eq!(g.committed_cursor(), &Some(OrderingValue::single_u64(2)));
}

#[test]
fn set_crash_hook_can_be_cleared() {
    let (_dir, store) = temp_store();
    let mut g = DurableGroup::create(Arc::clone(&store), cfg("g1", 4, 4, 2), "synthetic-u64")
        .unwrap()
        .with_crash_hook(abort_at(CrashPoint::AfterAckApplied));
    g.start().unwrap();
    let c = ConsumerId::new("c1").unwrap();
    g.join_consumer(c.clone()).unwrap();
    let _ = g.poll_fetch().unwrap();
    let b = g.assign_batch(&c).unwrap();
    assert!(g.ack(b.id).is_err());
    // The hook aborted after the engine applied the ack and before the write.
    let gid = GroupId::new("g1").unwrap();
    assert_eq!(g.committed_cursor(), &Some(OrderingValue::single_u64(2)));
    assert_eq!(store.load_checkpoint(&gid).unwrap(), None);
    g.set_crash_hook(crate::store::no_crash());
    drain_all(&mut g, &c);
    assert_eq!(g.committed_cursor(), &Some(OrderingValue::single_u64(4)));
}

/// B8: creating a group whose id already exists must fail and must not reset
/// the existing group's committed cursor.
#[test]
fn regress_b08_create_rejects_an_existing_group_and_keeps_its_cursor() {
    let (_dir, store) = temp_store();
    let mut g =
        DurableGroup::create(Arc::clone(&store), cfg("g1", 10, 10, 2), "synthetic-u64").unwrap();
    g.start().unwrap();
    let c = ConsumerId::new("c1").unwrap();
    g.join_consumer(c.clone()).unwrap();
    g.poll_fetch().unwrap();
    let batch = g.assign_batch(&c).unwrap();
    g.ack(batch.id).unwrap();
    let gid = GroupId::new("g1").unwrap();
    let before = store.load_checkpoint(&gid).unwrap();
    assert_eq!(before, Some(OrderingValue::single_u64(2)));

    let again = DurableGroup::create(Arc::clone(&store), cfg("g1", 10, 10, 2), "synthetic-u64");
    assert!(again.is_err(), "second create of g1 succeeded");
    assert_eq!(store.load_checkpoint(&gid).unwrap(), before);
}

/// B9: progress committed after the group was deleted must not bring the
/// group record back.
#[test]
fn regress_b09_commit_after_delete_does_not_resurrect_the_group() {
    let (_dir, store) = temp_store();
    let mut g =
        DurableGroup::create(Arc::clone(&store), cfg("g1", 10, 10, 2), "synthetic-u64").unwrap();
    g.start().unwrap();
    let c = ConsumerId::new("c1").unwrap();
    g.join_consumer(c.clone()).unwrap();
    g.poll_fetch().unwrap();
    let batch = g.assign_batch(&c).unwrap();
    let gid = GroupId::new("g1").unwrap();
    store.delete_group(&gid).unwrap();

    let result = g.ack(batch.id);
    assert!(
        result.is_err(),
        "ack committed progress for a deleted group"
    );
    assert!(store.get_group(&gid).unwrap().is_none(), "group came back");
}
