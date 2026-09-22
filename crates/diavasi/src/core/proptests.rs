//! Property-based tests for Stage 1 core invariants.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use proptest::prelude::*;

use crate::core::{ConsumerId, CoreError, GroupConfig, GroupEngine, GroupId, OrderingValue};

fn cfg(total: u64) -> GroupConfig {
    GroupConfig {
        group_id: GroupId::new("prop").unwrap(),
        total_records: total,
        payload_size: 4,
        max_buffer_records: 8,
        max_buffer_bytes: 4096,
        batch_max_records: 3,
        batch_timeout: Duration::from_secs(3600),
    }
}

#[derive(Debug, Clone)]
enum Action {
    Fetch,
    Assign(usize),
    AckOldest,
    AckNewest,
    Leave(usize),
    Join(usize),
    TimeoutTick,
    CrashRecover,
}

fn arb_action() -> impl Strategy<Value = Action> {
    prop_oneof![
        Just(Action::Fetch),
        (0usize..3).prop_map(Action::Assign),
        Just(Action::AckOldest),
        Just(Action::AckNewest),
        (0usize..3).prop_map(Action::Leave),
        (0usize..3).prop_map(Action::Join),
        Just(Action::TimeoutTick),
        Just(Action::CrashRecover),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn committed_monotonic_and_no_gap_skip(
        total in 5u64..40,
        actions in prop::collection::vec(arb_action(), 1..80)
    ) {
        let mut engine = GroupEngine::new(cfg(total)).unwrap();
        engine.start().unwrap();
        let consumers: Vec<ConsumerId> = (0..3)
            .map(|i| ConsumerId::new(format!("c{i}")).unwrap())
            .collect();
        for c in &consumers {
            let _ = engine.join_consumer(c.clone());
        }

        let mut pending: Vec<crate::core::BatchId> = Vec::new();
        let mut last_committed: Option<OrderingValue> = None;

        for action in actions {
            match action {
                Action::Fetch => {
                    let _ = engine.poll_fetch();
                }
                Action::Assign(i) => {
                    let c = &consumers[i % consumers.len()];
                    if let Ok(batch) = engine.assign_batch(c) {
                        pending.push(batch.id);
                    }
                }
                Action::AckOldest => {
                    if !pending.is_empty() {
                        let id = pending.remove(0);
                        engine.ack(id).unwrap();
                    }
                }
                Action::AckNewest => {
                    if let Some(id) = pending.pop() {
                        engine.ack(id).unwrap();
                    }
                }
                Action::Leave(i) => {
                    let c = &consumers[i % consumers.len()];
                    let _ = engine.leave_consumer(c);
                    // Batches for that consumer are requeued; drop unknown pending ids later via ack noop.
                }
                Action::Join(i) => {
                    let c = consumers[i % consumers.len()].clone();
                    let _ = engine.join_consumer(c);
                }
                Action::TimeoutTick => {
                    let _ = engine.tick(Instant::now() + Duration::from_secs(10_000));
                }
                Action::CrashRecover => {
                    let snap = engine.snapshot();
                    let committed = snap.committed_cursor.clone();
                    engine = GroupEngine::recover_from(snap).unwrap();
                    for c in &consumers {
                        let _ = engine.join_consumer(c.clone());
                    }
                    pending.clear();
                    prop_assert_eq!(engine.committed_cursor(), &committed);
                }
            }

            let committed = engine.committed_cursor().clone();
            if let (Some(prev), Some(cur)) = (&last_committed, &committed) {
                prop_assert!(cur >= prev, "committed moved backward");
            }
            if committed.is_some() {
                last_committed = committed;
            }

            // Buffer never exceeds caps.
            prop_assert!(engine.buffer_len() <= engine.config().max_buffer_records);
            prop_assert!(engine.buffer_bytes() <= engine.config().max_buffer_bytes);
        }
    }

    #[test]
    fn fair_drain_no_silent_loss(total in 5u64..25) {
        let mut engine = GroupEngine::new(cfg(total)).unwrap();
        engine.start().unwrap();
        let c = ConsumerId::new("c0").unwrap();
        engine.join_consumer(c.clone()).unwrap();

        let mut seen = HashSet::new();
        let mut steps = 0;
        while steps < 10_000 {
            steps += 1;
            let _ = engine.poll_fetch().unwrap();
            match engine.assign_batch(&c) {
                Ok(batch) => {
                    for r in &batch.records {
                        let id = match r.ordering.atoms() {
                            [crate::core::OrderingAtom::U64(v)] => *v,
                            _ => panic!("expected u64 key"),
                        };
                        prop_assert!(seen.insert(id), "double delivery of same key before ack is ok for at-least-once, but we track unique acked");
                        // For at-least-once, duplicates of unacked can happen after requeue.
                        // Here we only assign once without requeue in this test.
                    }
                    // Undo the unique check semantics: allow tracking acked only
                    engine.ack(batch.id).unwrap();
                }
                Err(CoreError::NoWork) => {
                    if engine.buffer_len() == 0
                        && engine.inflight_len() == 0
                        && engine
                            .config()
                            .total_records
                            == engine
                                .committed_cursor()
                                .as_ref()
                                .and_then(|v| match v.atoms() {
                                    [crate::core::OrderingAtom::U64(x)] => Some(*x),
                                    _ => None,
                                })
                                .unwrap_or(0)
                    {
                        break;
                    }
                    if engine.buffer_len() == 0 && engine.inflight_len() == 0 {
                        // source exhausted relative to fetch
                        let left = engine
                            .config()
                            .total_records
                            .saturating_sub(
                                engine
                                    .fetched_cursor()
                                    .as_ref()
                                    .and_then(|v| match v.atoms() {
                                        [crate::core::OrderingAtom::U64(x)] => Some(*x),
                                        _ => None,
                                    })
                                    .unwrap_or(0),
                            );
                        if left == 0 {
                            break;
                        }
                    }
                }
                Err(e) => return Err(TestCaseError::fail(e.to_string())),
            }
        }

        prop_assert_eq!(
            engine.committed_cursor(),
            &Some(OrderingValue::single_u64(total))
        );
        prop_assert_eq!(seen.len() as u64, total);
    }

    #[test]
    fn crash_mid_flight_replays_without_loss(total in 8u64..30, ack_first in 1usize..5) {
        let mut engine = GroupEngine::new(cfg(total)).unwrap();
        engine.start().unwrap();
        let c = ConsumerId::new("c0").unwrap();
        engine.join_consumer(c.clone()).unwrap();

        let mut acked = 0usize;
        while acked < ack_first {
            let _ = engine.poll_fetch().unwrap();
            match engine.assign_batch(&c) {
                Ok(batch) => {
                    acked += batch.records.len();
                    engine.ack(batch.id).unwrap();
                }
                Err(CoreError::NoWork) => {
                    let _ = engine.poll_fetch().unwrap();
                }
                Err(e) => return Err(TestCaseError::fail(e.to_string())),
            }
        }
        let _ = engine.poll_fetch().unwrap();
        let _ = engine.assign_batch(&c); // leave unacked
        let snap = engine.snapshot();
        let committed_at_crash = snap.committed_cursor.clone();

        let mut engine = GroupEngine::recover_from(snap).unwrap();
        engine.join_consumer(c.clone()).unwrap();

        let mut delivered_after = HashSet::new();
        let mut steps = 0;
        while steps < 10_000 {
            steps += 1;
            let _ = engine.poll_fetch().unwrap();
            match engine.assign_batch(&c) {
                Ok(batch) => {
                    for r in &batch.records {
                        if let [crate::core::OrderingAtom::U64(v)] = r.ordering.atoms() {
                            delivered_after.insert(*v);
                        }
                    }
                    engine.ack(batch.id).unwrap();
                }
                Err(CoreError::NoWork) => {
                    if engine.committed_cursor() == &Some(OrderingValue::single_u64(total)) {
                        break;
                    }
                    if engine.buffer_len() == 0
                        && engine.inflight_len() == 0
                        && engine.poll_fetch().unwrap() == 0
                    {
                        break;
                    }
                }
                Err(e) => return Err(TestCaseError::fail(e.to_string())),
            }
        }

        prop_assert_eq!(
            engine.committed_cursor(),
            &Some(OrderingValue::single_u64(total))
        );
        // Every key after committed_at_crash must appear in post-crash deliveries.
        let start = match &committed_at_crash {
            None => 1u64,
            Some(v) => match v.atoms() {
                [crate::core::OrderingAtom::U64(x)] => x + 1,
                _ => 1,
            },
        };
        for id in start..=total {
            prop_assert!(
                delivered_after.contains(&id),
                "missing replay of record {id} after crash"
            );
        }
    }
}
