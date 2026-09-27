use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use diavasi::control::{ConnectionCreateRequest, ControlService, GroupCreateRequest};
use diavasi::core::{ConsumerId, GroupId, OrderingAtom, RecordSource};
use diavasi::dataplane::{
    ConsumerClient, ConsumerOptions, DataPlaneConfig, SharedProgress, generate_self_signed,
    serve_dataplane_on,
};
use diavasi::runtime::RuntimeError;
use diavasi::store::{RedbStore, StateStore, StoreKey};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;

use crate::RedisFactory;
use crate::connect::{RedisEndpoint, connect};
use crate::reader::RedisSource;

static N: AtomicU64 = AtomicU64::new(0);

/// Reads a database URL. With `DIAVASI_REQUIRE_DB=1` a missing URL fails the
/// test instead of skipping it.
fn env_url(name: &str) -> Option<String> {
    let url = std::env::var(name).ok().filter(|url| !url.is_empty());
    if url.is_none() && std::env::var("DIAVASI_REQUIRE_DB").as_deref() == Ok("1") {
        panic!("DIAVASI_REQUIRE_DB=1 but {name} is not set");
    }
    url
}

fn redis_url() -> Option<String> {
    env_url("REDIS_URL")
}

struct Cleanup {
    endpoint: RedisEndpoint,
    stream: String,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        let endpoint = self.endpoint.clone();
        let stream = self.stream.clone();
        cleanup_blocking(move || async move {
            if let Ok(mut conn) = connect(&endpoint).await {
                let _: redis::RedisResult<()> = conn.del(&stream).await;
            }
        });
    }
}

/// Run async cleanup to completion on its own thread and runtime. `Drop`
/// runs as a test ends, when a task spawned on the test's runtime would never
/// run and the table or stream would be left behind.
fn cleanup_blocking<F>(work: impl FnOnce() -> F + Send + 'static)
where
    F: std::future::Future<Output = ()>,
{
    let _ = std::thread::spawn(move || {
        if let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            runtime.block_on(work());
        }
    })
    .join();
}

struct Lab {
    _dir: tempfile::TempDir,
    service: Arc<ControlService>,
    conn: ConnectionManager,
    stream: String,
    group: String,
    connection_id: String,
    endpoint: RedisEndpoint,
    _cleanup: Cleanup,
}

impl Lab {
    async fn open() -> Option<Self> {
        let url = redis_url()?;
        let endpoint = RedisEndpoint::from_url(&url).expect("REDIS_URL");
        let n = N.fetch_add(1, Ordering::Relaxed);
        let mut conn = connect(&endpoint).await.expect("connect");
        let pong: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .expect("ping");
        assert_eq!(pong, "PONG");
        let stream = format!("s9_{}_{n}", std::process::id());
        let group = format!("g{n}");
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(RedbStore::create(dir.path().join("meta.redb")).unwrap());
        let key = StoreKey::generate();
        let service = Arc::new(ControlService::new(
            Arc::clone(&store),
            key.clone(),
            "127.0.0.1:0".to_string(),
        ));
        service.install_source_factory(Arc::new(RedisFactory)).await;
        let connection_id = format!("rd{n}");
        service
            .create_connection(ConnectionCreateRequest {
                id: connection_id.clone(),
                kind: "redis".into(),
                config_json: endpoint.config_json(),
                secret: {
                    let secret = endpoint.secret();
                    if secret.is_empty() {
                        "unused".into()
                    } else {
                        secret
                    }
                },
            })
            .unwrap();
        let _cleanup = Cleanup {
            endpoint: endpoint.clone(),
            stream: stream.clone(),
        };
        Some(Self {
            _dir: dir,
            service,
            conn,
            stream,
            group,
            connection_id,
            endpoint,
            _cleanup,
        })
    }

    async fn xadd(&mut self, id: &str, body: &str) -> String {
        self.conn
            .xadd(&self.stream, id, &[("body", body)])
            .await
            .unwrap()
    }

    async fn xadd_fields(&mut self, id: &str, fields: &[(&str, &str)]) -> String {
        self.conn.xadd(&self.stream, id, fields).await.unwrap()
    }

    fn spec(&self, extra: serde_json::Value) -> serde_json::Value {
        let mut spec = serde_json::json!({
            "stream": self.stream,
            "group": self.group,
        });
        if let serde_json::Value::Object(map) = extra {
            for (key, value) in map {
                spec[key] = value;
            }
        }
        spec
    }

    async fn group(&self, id: &str, spec: serde_json::Value, batch: usize) {
        self.service
            .create_group(GroupCreateRequest {
                group_id: id.into(),
                total_records: 0,
                payload_size: 0,
                max_buffer_records: 256,
                max_buffer_bytes: 8 * 1024 * 1024,
                batch_max_records: batch,
                batch_timeout_ms: 5_000,
                ordering_contract: "redis-stream".into(),
                connection_id: Some(self.connection_id.clone()),
                source_spec: Some(spec),
            })
            .await
            .unwrap();
        self.service.start_group(id).await.unwrap();
    }
}

fn pair(record: &diavasi::core::Record) -> (u64, u64) {
    match record.ordering.atoms() {
        [OrderingAtom::U64(ms), OrderingAtom::U64(seq)] => (*ms, *seq),
        other => panic!("unexpected ordering {other:?}"),
    }
}

async fn drain(service: &ControlService, group: &str, consumer: &str) -> Vec<(u64, u64)> {
    let gid = GroupId::new(group).unwrap();
    let handle = service
        .supervisor()
        .lock()
        .await
        .get_handle(&gid)
        .expect("running");
    let cid = ConsumerId::new(consumer).unwrap();
    handle.join(cid.clone()).await.unwrap();
    let mut ids = Vec::new();
    let mut idle = 0u32;
    loop {
        match handle.assign(&cid).await {
            Ok(batch) => {
                idle = 0;
                ids.extend(batch.records.iter().map(pair));
                handle.ack(batch.id).await.unwrap();
            }
            Err(RuntimeError::Core(diavasi::core::CoreError::NoWork)) => {
                idle += 1;
                let stats = handle.buffer_stats().await.unwrap();
                if stats.buffer_len == 0 && stats.inflight_len == 0 && idle > 30 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(err) => panic!("{err}"),
        }
    }
    ids
}

#[tokio::test]
async fn unsupported_kind_is_rejected_before_connect() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(RedbStore::create(dir.path().join("meta.redb")).unwrap());
    let service = Arc::new(ControlService::new(
        store,
        StoreKey::generate(),
        "127.0.0.1:0".to_string(),
    ));
    service.install_source_factory(Arc::new(RedisFactory)).await;
    service
        .create_connection(ConnectionCreateRequest {
            id: "s".into(),
            kind: "scylla".into(),
            config_json: serde_json::json!({}),
            secret: "x".into(),
        })
        .unwrap();
    let err = service
        .create_group(GroupCreateRequest {
            group_id: "g".into(),
            total_records: 0,
            payload_size: 0,
            max_buffer_records: 8,
            max_buffer_bytes: 1024,
            batch_max_records: 1,
            batch_timeout_ms: 5_000,
            ordering_contract: "scylla".into(),
            connection_id: Some("s".into()),
            source_spec: Some(serde_json::json!({"stream": "events", "group": "g"})),
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("unsupported"), "{err}");
}

#[tokio::test]
async fn stream_ids_are_delivered_in_numeric_order() {
    let Some(mut lab) = Lab::open().await else {
        return;
    };
    lab.xadd("9-1", "nine").await;
    lab.xadd("10-0", "ten").await;
    lab.group("g", lab.spec(serde_json::json!({})), 10).await;
    assert_eq!(drain(&lab.service, "g", "c").await, vec![(9, 1), (10, 0)]);
}

#[tokio::test]
async fn empty_stream_assigns_nothing() {
    let Some(mut lab) = Lab::open().await else {
        return;
    };
    let id = lab.xadd("*", "gone").await;
    let _: i64 = lab.conn.xdel(&lab.stream, &[id]).await.unwrap();
    lab.group("g", lab.spec(serde_json::json!({})), 10).await;
    assert!(drain(&lab.service, "g", "c").await.is_empty());
    let cursor = lab.service.checkpoint("g").await.unwrap().durable_cursor;
    assert!(cursor.is_none());
}

#[tokio::test]
async fn setid_rewinds_a_read_that_was_not_checkpointed() {
    let Some(mut lab) = Lab::open().await else {
        return;
    };
    lab.xadd("1-0", "a").await;
    lab.xadd("2-0", "b").await;
    let secret = if lab.endpoint.secret().is_empty() {
        b"unused".to_vec()
    } else {
        lab.endpoint.secret().into_bytes()
    };
    let mut source = RedisSource::open(diavasi::runtime::SourceOpen {
        connection: lab
            .service
            .store()
            .get_connection(&lab.connection_id)
            .unwrap()
            .unwrap(),
        source_spec: lab.spec(serde_json::json!({})),
        secret,
    })
    .await
    .unwrap();
    let first = source.fetch_after(&None, 1).await.unwrap();
    assert_eq!(pair(&first[0]), (1, 0));
    let again = source.fetch_after(&None, 1).await.unwrap();
    assert_eq!(pair(&again[0]), (1, 0));
    let cursor = Some(again[0].ordering.clone());
    let rest = source.fetch_after(&cursor, 10).await.unwrap();
    assert_eq!(rest.iter().map(pair).collect::<Vec<_>>(), vec![(2, 0)]);
}

#[tokio::test]
async fn insert_ahead_is_delivered_and_delete_is_omitted() {
    let Some(mut lab) = Lab::open().await else {
        return;
    };
    lab.xadd("1-0", "a").await;
    lab.xadd("2-0", "b").await;
    lab.xadd("3-0", "c").await;
    lab.group("g", lab.spec(serde_json::json!({})), 1).await;
    let gid = GroupId::new("g").unwrap();
    let handle = lab
        .service
        .supervisor()
        .lock()
        .await
        .get_handle(&gid)
        .unwrap();
    let cid = ConsumerId::new("c").unwrap();
    handle.join(cid.clone()).await.unwrap();
    let batch = loop {
        match handle.assign(&cid).await {
            Ok(batch) => break batch,
            Err(RuntimeError::Core(_)) => tokio::time::sleep(Duration::from_millis(5)).await,
            Err(err) => panic!("{err}"),
        }
    };
    assert_eq!(pair(&batch.records[0]), (1, 0));
    handle.ack(batch.id).await.unwrap();
    lab.service
        .supervisor()
        .lock()
        .await
        .abort_group(&gid)
        .unwrap();
    let _: i64 = lab.conn.xdel(&lab.stream, &["3-0"]).await.unwrap();
    let ahead = lab.xadd("*", "d").await;
    let (ms, seq) = ahead.split_once('-').unwrap();
    let ahead = (ms.parse::<u64>().unwrap(), seq.parse::<u64>().unwrap());
    await_restart(&lab.service, "g").await;
    let ids = drain(&lab.service, "g", "c2").await;
    assert_eq!(ids, vec![(2, 0), ahead]);
}

#[tokio::test]
async fn fields_keep_the_requested_names() {
    let Some(mut lab) = Lab::open().await else {
        return;
    };
    lab.xadd_fields("1-0", &[("body", "a"), ("secret", "no")])
        .await;
    lab.group("g", lab.spec(serde_json::json!({"fields": ["body"]})), 1)
        .await;
    let gid = GroupId::new("g").unwrap();
    let handle = lab
        .service
        .supervisor()
        .lock()
        .await
        .get_handle(&gid)
        .unwrap();
    let cid = ConsumerId::new("c").unwrap();
    handle.join(cid.clone()).await.unwrap();
    let batch = loop {
        match handle.assign(&cid).await {
            Ok(batch) => break batch,
            Err(RuntimeError::Core(_)) => tokio::time::sleep(Duration::from_millis(5)).await,
            Err(err) => panic!("{err}"),
        }
    };
    let payload = String::from_utf8(batch.records[0].payload.to_vec()).unwrap();
    assert_eq!(payload, r#"{"body":"a"}"#);
}

#[tokio::test]
async fn missing_stream_and_wrong_type_are_rejected() {
    let Some(mut lab) = Lab::open().await else {
        return;
    };
    let missing = lab.spec(serde_json::json!({"stream": format!("{}_missing", lab.stream)}));
    let err = lab
        .service
        .create_group(GroupCreateRequest {
            group_id: "missing".into(),
            total_records: 0,
            payload_size: 0,
            max_buffer_records: 8,
            max_buffer_bytes: 1024,
            batch_max_records: 1,
            batch_timeout_ms: 5_000,
            ordering_contract: "redis-stream".into(),
            connection_id: Some(lab.connection_id.clone()),
            source_spec: Some(missing),
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("does not exist"), "{err}");

    let _: () = lab.conn.set(&lab.stream, "hello").await.unwrap();
    let err = lab
        .service
        .create_group(GroupCreateRequest {
            group_id: "typed".into(),
            total_records: 0,
            payload_size: 0,
            max_buffer_records: 8,
            max_buffer_bytes: 1024,
            batch_max_records: 1,
            batch_timeout_ms: 5_000,
            ordering_contract: "redis-stream".into(),
            connection_id: Some(lab.connection_id.clone()),
            source_spec: Some(lab.spec(serde_json::json!({}))),
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("has type"), "{err}");
}

#[tokio::test]
async fn two_consumers_share_one_traversal() {
    let Some(mut lab) = Lab::open().await else {
        return;
    };
    for id in ["1-0", "2-0", "3-0", "4-0"] {
        lab.xadd(id, id).await;
    }
    lab.group("g", lab.spec(serde_json::json!({})), 1).await;
    let mut a = drain(&lab.service, "g", "a").await;
    lab.service
        .supervisor()
        .lock()
        .await
        .get_handle(&GroupId::new("g").unwrap())
        .unwrap()
        .leave(&ConsumerId::new("a").unwrap())
        .await
        .unwrap();
    let mut b = drain(&lab.service, "g", "b").await;
    a.append(&mut b);
    a.sort();
    a.dedup();
    assert_eq!(a, vec![(1, 0), (2, 0), (3, 0), (4, 0)]);
}

#[tokio::test]
async fn connection_loss_reconnects() {
    let Some(mut lab) = Lab::open().await else {
        return;
    };
    lab.xadd("1-0", "a").await;
    lab.xadd("2-0", "b").await;
    let secret = if lab.endpoint.secret().is_empty() {
        b"unused".to_vec()
    } else {
        lab.endpoint.secret().into_bytes()
    };
    let mut source = RedisSource::open(diavasi::runtime::SourceOpen {
        connection: lab
            .service
            .store()
            .get_connection(&lab.connection_id)
            .unwrap()
            .unwrap(),
        source_spec: lab.spec(serde_json::json!({})),
        secret,
    })
    .await
    .unwrap();
    let first = source.fetch_after(&None, 1).await.unwrap();
    assert_eq!(pair(&first[0]), (1, 0));
    source.poison().await.unwrap();
    let cursor = Some(first[0].ordering.clone());
    let rest = source.fetch_after(&cursor, 10).await.unwrap();
    assert_eq!(rest.iter().map(pair).collect::<Vec<_>>(), vec![(2, 0)]);
}

#[tokio::test]
async fn data_plane_consumes_stream_entries() {
    let Some(mut lab) = Lab::open().await else {
        return;
    };
    for (id, body) in [("1-0", "a"), ("2-0", "b"), ("3-0", "c"), ("4-0", "d")] {
        lab.xadd(id, body).await;
    }
    lab.group("g", lab.spec(serde_json::json!({})), 4).await;
    let (ca, cert, key) = generate_self_signed(&[]).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let supervisor = lab.service.supervisor();
    tokio::spawn(async move {
        let _ = serve_dataplane_on(
            DataPlaneConfig {
                bind: addr,
                tls_cert_pem: cert.into_bytes(),
                tls_key_pem: key.into_bytes(),
                api_token: "tok".into(),
                supervisor,
                heartbeat_interval: Duration::from_secs(30),
                heartbeat_timeout: Duration::from_secs(30),
            },
            listener,
        )
        .await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let report = ConsumerClient::run(ConsumerOptions {
        addr: addr.to_string(),
        ca_pem: ca.into_bytes(),
        token: "tok".into(),
        group_id: "g".into(),
        consumer_id: "py".into(),
        max_in_flight: 2,
        stop_after_batches: None,
        expect_records: None,
        idle_after_join: None,
        leave_after_join: false,
        duplicate_first_ack: false,
        ack_delay: Duration::ZERO,
        shared_progress: Some(SharedProgress {
            acked: Arc::new(AtomicU64::new(0)),
            target: 4,
        }),
        timeout: Duration::from_secs(10),
    })
    .await
    .unwrap();
    assert_eq!(report.record_ids.len(), 4);
}

/// Drive the supervisor until `group` runs again after an abort. Restarts
/// wait at least `RETRY_FIRST`, and the aborted task stays listed until the
/// supervisor collects it.
async fn await_restart(service: &ControlService, group: &str) {
    let gid = GroupId::new(group).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let running = || async { service.supervisor().lock().await.get_handle(&gid).is_some() };
    while running().await {
        service.supervise_once().await.unwrap();
        assert!(
            std::time::Instant::now() < deadline,
            "aborted group was not collected"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    while !running().await {
        service.supervise_once().await.unwrap();
        assert!(
            std::time::Instant::now() < deadline,
            "group {group} was not restarted"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// Regression tests for the v0.12.0 review. Each name carries its finding id.

async fn open_source(lab: &Lab) -> RedisSource {
    let secret = if lab.endpoint.secret().is_empty() {
        b"unused".to_vec()
    } else {
        lab.endpoint.secret().into_bytes()
    };
    RedisSource::open(diavasi::runtime::SourceOpen {
        connection: lab
            .service
            .store()
            .get_connection(&lab.connection_id)
            .unwrap()
            .unwrap(),
        source_spec: lab.spec(serde_json::json!({})),
        secret,
    })
    .await
    .unwrap()
}

async fn read_one_at_a_time(mut source: RedisSource) -> Vec<(u64, u64)> {
    let mut cursor = None;
    let mut ids = Vec::new();
    while ids.len() <= 600 {
        let batch = source.fetch_after(&cursor, 1).await.unwrap();
        let Some(last) = batch.last() else {
            break;
        };
        cursor = Some(last.ordering.clone());
        ids.extend(batch.iter().map(pair));
    }
    ids
}

/// B15: two readers configured with the same Redis group name each read
/// the whole stream. Neither moves the other's position.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn regress_b15_readers_sharing_a_group_name_each_read_every_entry() {
    let Some(mut lab) = Lab::open().await else {
        return;
    };
    for i in 1..=300u64 {
        lab.xadd(&format!("{i}-0"), "x").await;
    }
    let first = tokio::spawn(read_one_at_a_time(open_source(&lab).await));
    let second = tokio::spawn(read_one_at_a_time(open_source(&lab).await));
    let expected: Vec<(u64, u64)> = (1..=300).map(|i| (i, 0)).collect();
    assert_eq!(first.await.unwrap(), expected, "first reader");
    assert_eq!(second.await.unwrap(), expected, "second reader");
}

/// B15: reading the stream does not create or move a Redis consumer group.
#[tokio::test]
async fn regress_b15_reading_leaves_no_consumer_group() {
    let Some(mut lab) = Lab::open().await else {
        return;
    };
    for i in 1..=3u64 {
        lab.xadd(&format!("{i}-0"), "x").await;
    }
    lab.group("g", lab.spec(serde_json::json!({})), 8).await;
    assert_eq!(drain(&lab.service, "g", "c").await.len(), 3);
    let groups: Vec<redis::Value> = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg(&lab.stream)
        .query_async(&mut lab.conn)
        .await
        .unwrap();
    assert!(
        groups.is_empty(),
        "reads created consumer groups: {groups:?}"
    );
}

/// B15: entries trimmed away before the committed cursor was reached are a
/// reported gap, not a silent skip.
#[tokio::test]
async fn regress_b15_trim_past_the_cursor_is_reported() {
    let Some(mut lab) = Lab::open().await else {
        return;
    };
    for i in 1..=10u64 {
        lab.xadd(&format!("{i}-0"), "x").await;
    }
    lab.group("g", lab.spec(serde_json::json!({})), 3).await;
    let gid = GroupId::new("g").unwrap();
    let handle = lab
        .service
        .supervisor()
        .lock()
        .await
        .get_handle(&gid)
        .unwrap();
    let consumer = ConsumerId::new("c").unwrap();
    handle.join(consumer.clone()).await.unwrap();
    let batch = loop {
        match handle.assign(&consumer).await {
            Ok(batch) => break batch,
            Err(RuntimeError::Core(diavasi::core::CoreError::NoWork)) => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(err) => panic!("{err}"),
        }
    };
    assert_eq!(batch.records.iter().map(pair).last(), Some((3, 0)));
    handle.ack(batch.id).await.unwrap();
    lab.service.pause_group("g").await.unwrap();

    let _: i64 = redis::cmd("XTRIM")
        .arg(&lab.stream)
        .arg("MINID")
        .arg("7-0")
        .query_async(&mut lab.conn)
        .await
        .unwrap();
    lab.service.start_group("g").await.unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        // Tests run without `serve`, so drive the supervisor that records
        // why the group task stopped.
        let _ = lab.service.supervise_once().await;
        let diag = lab.service.diagnostics("g").await.unwrap();
        if diag
            .last_stop_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("trim"))
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "entries 4 to 6 were trimmed before delivery and nothing reported it"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
