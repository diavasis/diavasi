use std::collections::HashSet;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use diavasi::control::{ConnectionCreateRequest, ControlService, GroupCreateRequest};
use diavasi::core::{ConsumerId, GroupId, OrderingAtom, RecordSource};
use diavasi::dataplane::{
    ConsumerClient, ConsumerOptions, DataPlaneConfig, SharedProgress, generate_self_signed,
    serve_dataplane,
};
use diavasi::runtime::{RuntimeError, SourceOpen};
use diavasi::store::{RedbStore, StateStore, StoreKey};
use scylla::client::session::Session;
use serde_json::Value;
use tokio::sync::Mutex;

use crate::ScyllaFactory;
use crate::connect::{ScyllaEndpoint, connect, execute, seed_bucket};
use crate::reader::ScyllaSource;

static N: AtomicU64 = AtomicU64::new(0);

fn schema_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Reads a database URL. With `DIAVASI_REQUIRE_DB=1` a missing URL fails the
/// test instead of skipping it.
fn env_url(name: &str) -> Option<String> {
    let url = std::env::var(name).ok().filter(|url| !url.is_empty());
    if url.is_none() && std::env::var("DIAVASI_REQUIRE_DB").as_deref() == Ok("1") {
        panic!("DIAVASI_REQUIRE_DB=1 but {name} is not set");
    }
    url
}

fn scylla_url() -> Option<String> {
    env_url("SCYLLA_URL")
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
    _guard: tokio::sync::MutexGuard<'static, ()>,
    _dir: tempfile::TempDir,
    service: std::sync::Arc<ControlService>,
    session: Session,
    endpoint: ScyllaEndpoint,
    keyspace: String,
    connection_id: String,
}

impl Lab {
    async fn open() -> Option<Self> {
        let url = scylla_url()?;
        let _guard = schema_lock().lock().await;
        let endpoint = ScyllaEndpoint::from_url(&url).expect("SCYLLA_URL");
        let session = connect(&endpoint).await.expect("scylla is reachable");
        let n = N.fetch_add(1, Ordering::Relaxed);
        let keyspace = format!("k{}n{n}", std::process::id());
        execute(
            &session,
            &format!(
                "CREATE KEYSPACE {keyspace} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
            ),
        )
        .await
        .expect("create keyspace");
        let dir = tempfile::tempdir().unwrap();
        let store = std::sync::Arc::new(RedbStore::create(dir.path().join("meta.redb")).unwrap());
        let service = std::sync::Arc::new(ControlService::new(
            std::sync::Arc::clone(&store),
            StoreKey::generate(),
            "127.0.0.1:0".to_string(),
        ));
        service
            .install_source_factory(std::sync::Arc::new(ScyllaFactory))
            .await;
        let connection_id = format!("sy{n}");
        service
            .create_connection(ConnectionCreateRequest {
                id: connection_id.clone(),
                kind: "scylla".into(),
                config_json: endpoint.config_json(),
                secret: "unused".into(),
            })
            .unwrap();
        Some(Self {
            _guard,
            _dir: dir,
            service,
            session,
            endpoint,
            keyspace,
            connection_id,
        })
    }
}

impl Drop for Lab {
    fn drop(&mut self) {
        let keyspace = self.keyspace.clone();
        let endpoint = self.endpoint.clone();
        cleanup_blocking(move || async move {
            if let Ok(session) = connect(&endpoint).await {
                let _ = execute(&session, &format!("DROP KEYSPACE IF EXISTS {keyspace}")).await;
            }
        });
    }
}

impl Lab {
    async fn table(&self, clustering: &str) {
        execute(
            &self.session,
            &format!(
                "CREATE TABLE {}.events (bucket int, id bigint, body text, secret text, PRIMARY KEY (bucket, id)) WITH CLUSTERING ORDER BY (id {clustering})",
                self.keyspace
            ),
        )
        .await
        .unwrap();
    }

    async fn insert(&self, bucket: i32, id: i64, body: &str) {
        self.session
            .query_unpaged(
                format!(
                    "INSERT INTO {}.events (bucket, id, body) VALUES (?, ?, ?)",
                    self.keyspace
                ),
                (bucket, id, body),
            )
            .await
            .unwrap();
    }

    async fn delete_id(&self, bucket: i32, id: i64) {
        self.session
            .query_unpaged(
                format!(
                    "DELETE FROM {}.events WHERE bucket = ? AND id = ?",
                    self.keyspace
                ),
                (bucket, id),
            )
            .await
            .unwrap();
    }

    fn partition_spec(&self, bucket: i32, extra: Value) -> Value {
        let mut spec = serde_json::json!({
            "keyspace": self.keyspace,
            "table": "events",
            "partition": {"bucket": bucket},
        });
        if let Value::Object(map) = extra {
            for (key, value) in map {
                spec[key] = value;
            }
        }
        spec
    }

    async fn group(&self, id: &str, spec: Value, batch: usize) {
        self.service
            .create_group(GroupCreateRequest {
                group_id: id.into(),
                total_records: 0,
                payload_size: 1,
                max_buffer_records: 4_096,
                max_buffer_bytes: 64 * 1024 * 1024,
                batch_max_records: batch,
                batch_timeout_ms: 5_000,
                ordering_contract: "scylla-partition".into(),
                connection_id: Some(self.connection_id.clone()),
                source_spec: Some(spec),
            })
            .await
            .unwrap();
        self.service.start_group(id).await.unwrap();
    }

    fn open_request(&self, spec: Value) -> SourceOpen {
        SourceOpen {
            connection: self
                .service
                .store()
                .get_connection(&self.connection_id)
                .unwrap()
                .unwrap(),
            source_spec: spec,
            secret: b"unused".to_vec(),
        }
    }
}

fn payload_i64(record: &diavasi::core::Record, field: &str) -> i64 {
    let value: Value = serde_json::from_slice(&record.payload).unwrap();
    value[field].as_i64().unwrap()
}

fn ordering_i64(record: &diavasi::core::Record) -> i64 {
    match record.ordering.atoms() {
        [OrderingAtom::I64(id)] => *id,
        other => panic!("unexpected ordering {other:?}"),
    }
}

async fn drain(service: &ControlService, group: &str, consumer: &str) -> Vec<i64> {
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
                ids.extend(batch.records.iter().map(|record| payload_i64(record, "id")));
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
async fn partition_order_and_payload() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.table("ASC").await;
    lab.insert(0, 1, "a").await;
    lab.insert(0, 3, "c").await;
    lab.insert(0, 2, "b").await;
    lab.session
        .query_unpaged(
            format!(
                "INSERT INTO {}.events (bucket, id, body, secret) VALUES (?, ?, ?, ?)",
                lab.keyspace
            ),
            (0i32, 4i64, "d", "hidden"),
        )
        .await
        .unwrap();
    lab.group(
        "g",
        lab.partition_spec(0, serde_json::json!({"columns": ["body"]})),
        10,
    )
    .await;
    let ids = drain(&lab.service, "g", "c").await;
    assert_eq!(ids, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn columns_omit_unlisted_fields() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.table("ASC").await;
    lab.session
        .query_unpaged(
            format!(
                "INSERT INTO {}.events (bucket, id, body, secret) VALUES (?, ?, ?, ?)",
                lab.keyspace
            ),
            (0i32, 1i64, "a", "hidden"),
        )
        .await
        .unwrap();
    let mut source = ScyllaSource::open(
        lab.open_request(lab.partition_spec(0, serde_json::json!({"columns": ["body"]}))),
    )
    .await
    .unwrap();
    let rows = source.fetch_after(&None, 10).await.unwrap();
    let payload: Value = serde_json::from_slice(&rows[0].payload).unwrap();
    assert_eq!(payload["body"], "a");
    assert_eq!(payload["id"], 1);
    assert!(payload.get("secret").is_none());
}

#[tokio::test]
async fn empty_partition_checkpoints_nothing() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.table("ASC").await;
    lab.group("g", lab.partition_spec(9, serde_json::json!({})), 10)
        .await;
    assert!(drain(&lab.service, "g", "c").await.is_empty());
    let cursor = lab.service.checkpoint("g").await.unwrap().durable_cursor;
    assert!(cursor.is_none());
}

#[tokio::test]
async fn resume_skips_a_committed_row_and_a_delete() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.table("ASC").await;
    lab.insert(0, 1, "a").await;
    lab.insert(0, 2, "b").await;
    lab.insert(0, 3, "c").await;
    lab.group("g", lab.partition_spec(0, serde_json::json!({})), 1)
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
    assert_eq!(payload_i64(&batch.records[0], "id"), 1);
    handle.ack(batch.id).await.unwrap();
    lab.service
        .supervisor()
        .lock()
        .await
        .abort_group(&gid)
        .unwrap();
    lab.delete_id(0, 2).await;
    lab.insert(0, 4, "d").await;
    lab.insert(0, 0, "behind").await;
    await_restart(&lab.service, "g").await;
    assert_eq!(drain(&lab.service, "g", "c2").await, vec![3, 4]);
}

#[tokio::test]
async fn descending_clustering_is_delivered_high_to_low() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.table("DESC").await;
    lab.insert(0, 1, "a").await;
    lab.insert(0, 2, "b").await;
    lab.insert(0, 3, "c").await;
    let mut source =
        ScyllaSource::open(lab.open_request(lab.partition_spec(0, serde_json::json!({}))))
            .await
            .unwrap();
    let rows = source.fetch_after(&None, 10).await.unwrap();
    let ids: Vec<i64> = rows.iter().map(|row| payload_i64(row, "id")).collect();
    assert_eq!(ids, vec![3, 2, 1]);
    let mut previous = None;
    for row in &rows {
        if let Some(prev) = &previous {
            assert!(row.ordering > *prev);
        }
        previous = Some(row.ordering.clone());
    }
    let cursor = Some(rows[0].ordering.clone());
    let rest = source.fetch_after(&cursor, 10).await.unwrap();
    assert_eq!(
        rest.iter()
            .map(|row| payload_i64(row, "id"))
            .collect::<Vec<_>>(),
        vec![2, 1]
    );
    assert!(ordering_i64(&rows[0]) < ordering_i64(&rest[0]));
}

#[tokio::test]
async fn compound_clustering_uses_schema_direction() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    execute(
        &lab.session,
        &format!(
            "CREATE TABLE {}.compound (bucket int, a int, b int, body text, PRIMARY KEY (bucket, a, b)) WITH CLUSTERING ORDER BY (a ASC, b DESC)",
            lab.keyspace
        ),
    )
    .await
    .unwrap();
    for (a, b) in [(1, 1), (1, 3), (2, 0)] {
        lab.session
            .query_unpaged(
                format!(
                    "INSERT INTO {}.compound (bucket, a, b, body) VALUES (?, ?, ?, ?)",
                    lab.keyspace
                ),
                (0i32, a, b, "x"),
            )
            .await
            .unwrap();
    }
    let spec = serde_json::json!({
        "keyspace": lab.keyspace,
        "table": "compound",
        "partition": {"bucket": 0},
    });
    let mut source = ScyllaSource::open(lab.open_request(spec)).await.unwrap();
    let rows = source.fetch_after(&None, 10).await.unwrap();
    let pairs: Vec<(i64, i64)> = rows
        .iter()
        .map(|row| (payload_i64(row, "a"), payload_i64(row, "b")))
        .collect();
    assert_eq!(pairs, vec![(1, 3), (1, 1), (2, 0)]);
    let cursor = Some(rows[0].ordering.clone());
    let rest = source.fetch_after(&cursor, 10).await.unwrap();
    let rest: Vec<(i64, i64)> = rest
        .iter()
        .map(|row| (payload_i64(row, "a"), payload_i64(row, "b")))
        .collect();
    assert_eq!(rest, vec![(1, 1), (2, 0)]);
}

#[tokio::test]
async fn missing_table_and_bad_keys_fail_at_create() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.table("ASC").await;
    let missing = serde_json::json!({
        "keyspace": lab.keyspace,
        "table": "missing",
        "partition": {"bucket": 0},
    });
    let err = lab
        .service
        .create_group(GroupCreateRequest {
            group_id: "missing".into(),
            total_records: 0,
            payload_size: 1,
            max_buffer_records: 8,
            max_buffer_bytes: 1024,
            batch_max_records: 1,
            batch_timeout_ms: 5_000,
            ordering_contract: "scylla-partition".into(),
            connection_id: Some(lab.connection_id.clone()),
            source_spec: Some(missing),
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("does not exist"), "{err}");

    execute(
        &lab.session,
        &format!(
            "CREATE TABLE {}.wide (bucket int, extra int, id bigint, body text, PRIMARY KEY ((bucket, extra), id))",
            lab.keyspace
        ),
    )
    .await
    .unwrap();
    let partial = serde_json::json!({
        "keyspace": lab.keyspace,
        "table": "wide",
        "partition": {"bucket": 0},
    });
    let err = lab
        .service
        .create_group(GroupCreateRequest {
            group_id: "partial".into(),
            total_records: 0,
            payload_size: 1,
            max_buffer_records: 8,
            max_buffer_bytes: 1024,
            batch_max_records: 1,
            batch_timeout_ms: 5_000,
            ordering_contract: "scylla-partition".into(),
            connection_id: Some(lab.connection_id.clone()),
            source_spec: Some(partial),
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("every partition key"), "{err}");

    execute(
        &lab.session,
        &format!(
            "CREATE TABLE {}.doubles (bucket double, id bigint, PRIMARY KEY (bucket, id))",
            lab.keyspace
        ),
    )
    .await
    .unwrap();
    let doubles = serde_json::json!({
        "keyspace": lab.keyspace,
        "table": "doubles",
        "partition": {"bucket": 1.5},
    });
    let err = lab
        .service
        .create_group(GroupCreateRequest {
            group_id: "doubles".into(),
            total_records: 0,
            payload_size: 1,
            max_buffer_records: 8,
            max_buffer_bytes: 1024,
            batch_max_records: 1,
            batch_timeout_ms: 5_000,
            ordering_contract: "scylla-partition".into(),
            connection_id: Some(lab.connection_id.clone()),
            source_spec: Some(doubles),
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not supported"), "{err}");
}

#[tokio::test]
async fn token_scan_orders_by_token_and_resumes() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.table("ASC").await;
    for bucket in [0, 1, 2, 3] {
        lab.insert(bucket, 1, "row").await;
    }
    let spec = serde_json::json!({
        "keyspace": lab.keyspace,
        "table": "events",
        "scan": "token",
    });
    let mut source = ScyllaSource::open(lab.open_request(spec)).await.unwrap();
    let rows = source.fetch_after(&None, 10).await.unwrap();
    assert_eq!(rows.len(), 4);
    let mut tokens = Vec::new();
    let mut buckets = HashSet::new();
    for row in &rows {
        match row.ordering.atoms().first() {
            Some(OrderingAtom::I64(token)) => tokens.push(*token),
            other => panic!("token atom {other:?}"),
        }
        buckets.insert(payload_i64(row, "bucket"));
    }
    let mut sorted = tokens.clone();
    sorted.sort();
    assert_eq!(tokens, sorted);
    assert_eq!(buckets, HashSet::from([0, 1, 2, 3]));
    let cursor = Some(rows[0].ordering.clone());
    let rest = source.fetch_after(&cursor, 10).await.unwrap();
    assert_eq!(rest.len(), 3);
    assert!(rest[0].ordering > rows[0].ordering);
}

#[tokio::test]
async fn two_consumers_share_one_partition() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.table("ASC").await;
    for id in 1..=4 {
        lab.insert(0, id, "x").await;
    }
    lab.group("g", lab.partition_spec(0, serde_json::json!({})), 1)
        .await;
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
    assert_eq!(a, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn poisoned_session_reconnects() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.table("ASC").await;
    lab.insert(0, 1, "a").await;
    lab.insert(0, 2, "b").await;
    let mut source =
        ScyllaSource::open(lab.open_request(lab.partition_spec(0, serde_json::json!({}))))
            .await
            .unwrap();
    let first = source.fetch_after(&None, 1).await.unwrap();
    assert_eq!(payload_i64(&first[0], "id"), 1);
    source.poison();
    let cursor = Some(first[0].ordering.clone());
    let rest = source.fetch_after(&cursor, 10).await.unwrap();
    assert_eq!(
        rest.iter()
            .map(|row| payload_i64(row, "id"))
            .collect::<Vec<_>>(),
        vec![2]
    );
}

#[tokio::test]
async fn data_plane_consumes_partition_rows() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.table("ASC").await;
    for id in 1..=4 {
        lab.insert(0, id, "x").await;
    }
    lab.group("g", lab.partition_spec(0, serde_json::json!({})), 4)
        .await;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let (ca, cert, key) = generate_self_signed().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let supervisor = lab.service.supervisor();
    tokio::spawn(async move {
        let _ = serve_dataplane(DataPlaneConfig {
            bind: addr,
            tls_cert_pem: cert.into_bytes(),
            tls_key_pem: key.into_bytes(),
            api_token: "tok".into(),
            supervisor,
            heartbeat_interval: Duration::from_secs(30),
            heartbeat_timeout: Duration::from_secs(30),
        })
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
            acked: std::sync::Arc::new(AtomicU64::new(0)),
            target: 4,
        }),
        timeout: Duration::from_secs(10),
    })
    .await
    .unwrap();
    let mut ids = report.record_ids;
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn partition_of_tens_of_thousands() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    execute(
        &lab.session,
        &format!(
            "CREATE TABLE {}.events (bucket int, id bigint, body text, PRIMARY KEY (bucket, id))",
            lab.keyspace
        ),
    )
    .await
    .unwrap();
    seed_bucket(&lab.session, &lab.keyspace, "events", 20_000, "x")
        .await
        .unwrap();
    lab.group("g", lab.partition_spec(0, serde_json::json!({})), 500)
        .await;
    let ids = drain(&lab.service, "g", "c").await;
    assert_eq!(ids.len(), 20_000);
    assert_eq!(ids[0], 1);
    assert_eq!(ids[19_999], 20_000);
}

#[tokio::test]
#[ignore]
async fn partition_of_millions() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    execute(
        &lab.session,
        &format!(
            "CREATE TABLE {}.events (bucket int, id bigint, body text, PRIMARY KEY (bucket, id))",
            lab.keyspace
        ),
    )
    .await
    .unwrap();
    seed_bucket(&lab.session, &lab.keyspace, "events", 1_000_000, "x")
        .await
        .unwrap();
    lab.group("g", lab.partition_spec(0, serde_json::json!({})), 1_000)
        .await;
    let ids = drain(&lab.service, "g", "c").await;
    assert_eq!(ids.len(), 1_000_000);
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

/// B16: a token scan resumes inside a wide partition and reads every row of
/// it, then continues with the next partitions.
#[tokio::test]
async fn regress_b16_token_scan_resumes_inside_a_wide_partition() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.table("ASC").await;
    seed_bucket(&lab.session, &lab.keyspace, "events", 3_000, "x")
        .await
        .unwrap();
    for bucket in [1, 2, 3] {
        lab.insert(bucket, 1, "row").await;
    }
    let spec = serde_json::json!({
        "keyspace": lab.keyspace,
        "table": "events",
        "scan": "token",
    });
    let mut source = ScyllaSource::open(lab.open_request(spec)).await.unwrap();
    let mut cursor = None;
    let mut seen = HashSet::new();
    loop {
        let rows = source
            .fetch_after(&cursor, 50)
            .await
            .unwrap_or_else(|err| panic!("fetch failed after {} rows: {err}", seen.len()));
        let Some(last) = rows.last() else {
            break;
        };
        cursor = Some(last.ordering.clone());
        for row in &rows {
            let key = (payload_i64(row, "bucket"), payload_i64(row, "id"));
            assert!(seen.insert(key), "row {key:?} delivered twice");
        }
    }
    assert_eq!(seen.len(), 3_003);
}
