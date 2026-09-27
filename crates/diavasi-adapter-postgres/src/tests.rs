use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use diavasi::control::{ConnectionCreateRequest, ControlService, GroupCreateRequest};
use diavasi::core::{
    ConsumerId, GroupId, LogicalCursor, OrderingAtom, OrderingValue, RecordSource,
};
use diavasi::dataplane::{
    ConsumerClient, ConsumerOptions, DataPlaneConfig, generate_self_signed, serve_dataplane,
};
use diavasi::runtime::{RuntimeError, SourceOpen};
use diavasi::store::{ConnectionRecord, RedbStore, SealedSecret, StoreKey};
use tokio_postgres::Client;

use crate::PostgresFactory;
use crate::catalog::describe;
use crate::connect::{PgEndpoint, connect};
use crate::reader::PostgresSource;
use crate::spec::SourceSpec;

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

fn database_url() -> Option<String> {
    env_url("DATABASE_URL")
}

struct Cleanup {
    endpoint: PgEndpoint,
    table: String,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        let endpoint = self.endpoint.clone();
        let table = self.table.clone();
        cleanup_blocking(move || async move {
            if let Ok(client) = connect(&endpoint).await {
                let _ = client
                    .batch_execute(&format!("DROP TABLE IF EXISTS {table}"))
                    .await;
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
    admin: Client,
    table: String,
    key: StoreKey,
    path: std::path::PathBuf,
    cleanup: Cleanup,
}

impl Lab {
    async fn open(ddl: &str) -> Option<Self> {
        let url = database_url()?;
        let (endpoint, password) = PgEndpoint::from_database_url(&url).expect("DATABASE_URL");
        let admin = connect(&endpoint).await.expect("connect");
        let n = N.fetch_add(1, Ordering::Relaxed);
        let table = format!("s6_{}_{n}", std::process::id());
        admin
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {table}; CREATE TABLE {table} ({ddl})"
            ))
            .await
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.redb");
        let store = Arc::new(RedbStore::create(&path).unwrap());
        let key = StoreKey::generate();
        let service = Arc::new(ControlService::new(
            Arc::clone(&store),
            key.clone(),
            "127.0.0.1:0".to_string(),
        ));
        service
            .install_source_factory(Arc::new(PostgresFactory))
            .await;
        service
            .create_connection(ConnectionCreateRequest {
                id: format!("pg{n}"),
                kind: "postgres".into(),
                config_json: endpoint.config_json(),
                secret: password,
            })
            .unwrap();
        Some(Self {
            _dir: dir,
            service,
            admin,
            table: table.clone(),
            key,
            path,
            cleanup: Cleanup { endpoint, table },
        })
    }

    fn spec(
        &self,
        order_by: serde_json::Value,
        payload: &[&str],
        filter: Option<&str>,
    ) -> serde_json::Value {
        let mut spec = serde_json::json!({
            "table": self.table,
            "order_by": order_by,
            "payload": payload,
        });
        if let Some(filter) = filter {
            spec["filter"] = serde_json::json!(filter);
        }
        spec
    }

    async fn group(&self, id: &str, spec: serde_json::Value, batch: usize) {
        let n = self.table.rsplit('_').next().unwrap();
        self.service
            .create_group(GroupCreateRequest {
                group_id: id.into(),
                total_records: 0,
                payload_size: 1,
                max_buffer_records: 256,
                max_buffer_bytes: 8 * 1024 * 1024,
                batch_max_records: batch,
                batch_timeout_ms: 5_000,
                ordering_contract: "postgres-keyset".into(),
                connection_id: Some(format!("pg{n}")),
                source_spec: Some(spec),
            })
            .await
            .unwrap();
        self.service.start_group(id).await.unwrap();
    }

    async fn reopen(self) -> Self {
        let Self {
            _dir,
            admin,
            table,
            key,
            path,
            service,
            cleanup,
        } = self;
        service
            .supervisor()
            .lock()
            .await
            .stop_group(&GroupId::new("g").unwrap())
            .await
            .ok();
        drop(service);
        let store = Arc::new(RedbStore::open(&path).unwrap());
        let service = Arc::new(ControlService::new(
            store,
            key.clone(),
            "127.0.0.1:0".to_string(),
        ));
        service
            .install_source_factory(Arc::new(PostgresFactory))
            .await;
        Self {
            _dir,
            service,
            admin,
            table,
            key,
            path,
            cleanup,
        }
    }
}

async fn drain_i64(service: &ControlService, group: &str, consumer: &str) -> Vec<i64> {
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
                for record in &batch.records {
                    match record.ordering.atoms() {
                        [OrderingAtom::I64(id)] => ids.push(*id),
                        other => panic!("unexpected ordering {other:?}"),
                    }
                }
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

fn int_order(column: &str, ty: &str) -> serde_json::Value {
    serde_json::json!([{ "column": column, "type": ty }])
}

#[tokio::test]
async fn int_primary_key_round_trip() {
    let Some(lab) = Lab::open("id int8 primary key, body text not null").await else {
        return;
    };
    lab.admin
        .batch_execute(&format!(
            "INSERT INTO {} (id, body) VALUES (1, 'a'), (2, 'b'), (3, 'c')",
            lab.table
        ))
        .await
        .unwrap();
    let spec = lab.spec(int_order("id", "int8"), &["body"], None);
    lab.group("g", spec, 2).await;
    let mut ids = drain_i64(&lab.service, "g", "c1").await;
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3]);
}

#[tokio::test]
async fn empty_table_assigns_nothing() {
    let Some(lab) = Lab::open("id int8 primary key, body text not null").await else {
        return;
    };
    let spec = lab.spec(int_order("id", "int8"), &["body"], None);
    lab.group("g", spec, 2).await;
    let ids = drain_i64(&lab.service, "g", "c1").await;
    assert!(ids.is_empty());
    let cursor = lab.service.checkpoint("g").await.unwrap().durable_cursor;
    assert!(cursor.is_none());
}

#[tokio::test]
async fn timestamptz_and_id_composite() {
    let Some(lab) = Lab::open(
        "created_at timestamptz not null, id int8 not null, body text not null, primary key (created_at, id)",
    )
    .await
    else {
        return;
    };
    lab.admin
        .batch_execute(&format!(
            "INSERT INTO {} (created_at, id, body) VALUES
             ('2020-01-01T00:00:00Z', 1, 'a'),
             ('2020-01-01T00:00:00Z', 2, 'b'),
             ('2020-01-02T00:00:00Z', 1, 'c')",
            lab.table
        ))
        .await
        .unwrap();
    let spec = lab.spec(
        serde_json::json!([
            {"column": "created_at", "type": "timestamptz"},
            {"column": "id", "type": "int8"}
        ]),
        &["body"],
        None,
    );
    lab.group("g", spec, 10).await;
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
    let mut keys = Vec::new();
    let mut idle = 0u32;
    loop {
        match handle.assign(&cid).await {
            Ok(batch) => {
                idle = 0;
                for record in &batch.records {
                    keys.push(format!("{:?}", record.ordering.atoms()));
                }
                handle.ack(batch.id).await.unwrap();
            }
            Err(RuntimeError::Core(diavasi::core::CoreError::NoWork)) => {
                idle += 1;
                if idle > 30 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(err) => panic!("{err}"),
        }
    }
    assert_eq!(keys.len(), 3, "{keys:?}");
}

#[tokio::test]
async fn text_key_uses_byte_order() {
    let Some(lab) = Lab::open("name text primary key, body text not null").await else {
        return;
    };
    lab.admin
        .batch_execute(&format!(
            "INSERT INTO {} (name, body) VALUES ('b', 'one'), ('a', 'two')",
            lab.table
        ))
        .await
        .unwrap();
    let spec = lab.spec(
        serde_json::json!([{"column": "name", "type": "text"}]),
        &["body"],
        None,
    );
    lab.group("g", spec, 10).await;
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
    let mut names = Vec::new();
    let mut idle = 0;
    loop {
        match handle.assign(&cid).await {
            Ok(batch) => {
                idle = 0;
                for record in batch.records {
                    match record.ordering.atoms() {
                        [OrderingAtom::Bytes(bytes)] => {
                            names.push(String::from_utf8(bytes.clone()).unwrap())
                        }
                        other => panic!("{other:?}"),
                    }
                }
                handle.ack(batch.id).await.unwrap();
            }
            Err(RuntimeError::Core(_)) => {
                idle += 1;
                if idle > 30 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(err) => panic!("{err}"),
        }
    }
    assert_eq!(names, vec!["a".to_string(), "b".to_string()]);
}

#[tokio::test]
async fn inserts_ahead_appear_and_inserts_behind_do_not() {
    let Some(lab) = Lab::open("id int4 primary key, body text not null").await else {
        return;
    };
    lab.admin
        .execute(
            &format!(
                "INSERT INTO {} (id, body) VALUES (1, 'a'), (2, 'b')",
                lab.table
            ),
            &[],
        )
        .await
        .unwrap();
    let spec = lab.spec(int_order("id", "int4"), &["body"], None);
    lab.group("g", spec, 1).await;
    let first = drain_i64(&lab.service, "g", "c1").await;
    assert_eq!(first, vec![1, 2]);
    lab.admin
        .execute(
            &format!(
                "INSERT INTO {} (id, body) VALUES (0, 'behind'), (3, 'ahead')",
                lab.table
            ),
            &[],
        )
        .await
        .unwrap();
    let gid = GroupId::new("g").unwrap();
    lab.service
        .supervisor()
        .lock()
        .await
        .get_handle(&gid)
        .unwrap()
        .leave(&ConsumerId::new("c1").unwrap())
        .await
        .unwrap();
    let more = drain_i64(&lab.service, "g", "c2").await;
    assert_eq!(more, vec![3]);
}

#[tokio::test]
async fn delete_and_payload_update_do_not_redeliver() {
    let Some(lab) = Lab::open("id int8 primary key, body text not null").await else {
        return;
    };
    lab.admin
        .batch_execute(&format!(
            "INSERT INTO {} (id, body) VALUES (1, 'a'), (2, 'b'), (3, 'c')",
            lab.table
        ))
        .await
        .unwrap();
    let spec = lab.spec(int_order("id", "int8"), &["body"], None);
    lab.group("g", spec, 1).await;
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
    assert_eq!(batch.records[0].ordering.atoms(), [OrderingAtom::I64(1)]);
    handle.ack(batch.id).await.unwrap();
    lab.service
        .supervisor()
        .lock()
        .await
        .abort_group(&gid)
        .unwrap();
    lab.admin
        .batch_execute(&format!(
            "UPDATE {} SET body = 'changed' WHERE id = 1; DELETE FROM {} WHERE id = 3",
            lab.table, lab.table
        ))
        .await
        .unwrap();
    await_restart(&lab.service, "g").await;
    handle.leave(&cid).await.ok();
    let rest = drain_i64(&lab.service, "g", "c2").await;
    assert_eq!(rest, vec![2]);
}

#[tokio::test]
async fn ordering_column_update_breaks_the_contract() {
    let Some(lab) =
        Lab::open("id int8 primary key, seq int8 not null unique, body text not null").await
    else {
        return;
    };
    lab.admin
        .batch_execute(&format!(
            "INSERT INTO {} (id, seq, body) VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c')",
            lab.table
        ))
        .await
        .unwrap();
    let spec = lab.spec(int_order("seq", "int8"), &["body"], None);
    lab.group("g", spec, 1).await;
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
    handle.ack(batch.id).await.unwrap();
    lab.service
        .supervisor()
        .lock()
        .await
        .abort_group(&gid)
        .unwrap();
    lab.admin
        .execute(
            &format!("UPDATE {} SET seq = 0 WHERE seq = 2", lab.table),
            &[],
        )
        .await
        .unwrap();
    await_restart(&lab.service, "g").await;
    handle.leave(&cid).await.ok();
    let rest = drain_i64(&lab.service, "g", "c2").await;
    assert_eq!(
        rest,
        vec![3],
        "row moved behind the cursor is not delivered"
    );
}

#[tokio::test]
async fn restart_resumes_from_committed_cursor() {
    let Some(lab) = Lab::open("id int8 primary key, body text not null").await else {
        return;
    };
    lab.admin
        .batch_execute(&format!(
            "INSERT INTO {} (id, body) SELECT g, 'x' FROM generate_series(1, 6) g",
            lab.table
        ))
        .await
        .unwrap();
    let spec = lab.spec(int_order("id", "int8"), &["body"], None);
    lab.group("g", spec, 1).await;
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
    handle.ack(batch.id).await.unwrap();
    let second = loop {
        match handle.assign(&cid).await {
            Ok(batch) => break batch,
            Err(RuntimeError::Core(_)) => tokio::time::sleep(Duration::from_millis(5)).await,
            Err(err) => panic!("{err}"),
        }
    };
    assert_eq!(second.records[0].ordering.atoms(), [OrderingAtom::I64(2)]);
    drop(second);
    drop(handle);
    let lab = lab.reopen().await;
    lab.service.start_group("g").await.unwrap();
    let rest = drain_i64(&lab.service, "g", "c2").await;
    assert_eq!(rest, vec![2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn two_consumers_and_crash_cover_the_uncommitted_tail() {
    let Some(lab) = Lab::open("id int8 primary key, body text not null").await else {
        return;
    };
    lab.admin
        .batch_execute(&format!(
            "INSERT INTO {} (id, body) SELECT g, 'x' FROM generate_series(1, 6) g",
            lab.table
        ))
        .await
        .unwrap();
    let spec = lab.spec(int_order("id", "int8"), &["body"], None);
    lab.group("g", spec, 2).await;
    let mut a = drain_i64(&lab.service, "g", "a").await;
    let gid = GroupId::new("g").unwrap();
    lab.service
        .supervisor()
        .lock()
        .await
        .get_handle(&gid)
        .unwrap()
        .leave(&ConsumerId::new("a").unwrap())
        .await
        .unwrap();
    let mut b = drain_i64(&lab.service, "g", "b").await;
    a.append(&mut b);
    a.sort();
    a.dedup();
    assert_eq!(a, vec![1, 2, 3, 4, 5, 6]);

    lab.admin
        .batch_execute(&format!(
            "TRUNCATE {}; INSERT INTO {} (id, body) SELECT g, 'y' FROM generate_series(1, 4) g",
            lab.table, lab.table
        ))
        .await
        .unwrap();
    lab.service
        .supervisor()
        .lock()
        .await
        .abort_group(&gid)
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = lab.service.supervise_once().await.unwrap();
    // The committed cursor is already past this table's previous rows. New ids
    // at or behind that cursor stay unseen; this crash check uses a fresh group.
    let spec = lab.spec(int_order("id", "int8"), &["body"], None);
    lab.group("g2", spec, 1).await;
    let handle = lab
        .service
        .supervisor()
        .lock()
        .await
        .get_handle(&GroupId::new("g2").unwrap())
        .unwrap();
    let cid = ConsumerId::new("drop").unwrap();
    handle.join(cid.clone()).await.unwrap();
    let inflight = loop {
        match handle.assign(&cid).await {
            Ok(batch) => break batch,
            Err(RuntimeError::Core(_)) => tokio::time::sleep(Duration::from_millis(5)).await,
            Err(err) => panic!("{err}"),
        }
    };
    assert!(!inflight.records.is_empty());
    drop(inflight);
    drop(handle);
    lab.service
        .supervisor()
        .lock()
        .await
        .abort_group(&GroupId::new("g2").unwrap())
        .unwrap();
    await_restart(&lab.service, "g2").await;
    let mut ids = drain_i64(&lab.service, "g2", "resume").await;
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn connection_loss_reconnects() {
    let Some(lab) = Lab::open("id int8 primary key, body text not null").await else {
        return;
    };
    lab.admin
        .batch_execute(&format!(
            "INSERT INTO {} (id, body) SELECT g, 'x' FROM generate_series(1, 4) g",
            lab.table
        ))
        .await
        .unwrap();
    let spec = lab.spec(int_order("id", "int8"), &["body"], None);
    lab.group("g", spec, 1).await;
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
    handle.ack(batch.id).await.unwrap();
    lab.admin
        .batch_execute(&format!(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'diavasi:{}'",
            lab.table
        ))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let _ = lab.service.supervise_once().await;
    handle.leave(&cid).await.ok();
    let rest = drain_i64(&lab.service, "g", "c2").await;
    assert_eq!(rest, vec![2, 3, 4]);
}

#[tokio::test]
async fn unsafe_contract_requires_acknowledgement() {
    let Some(lab) = Lab::open("id int8 primary key, note text not null").await else {
        return;
    };
    let n = lab.table.rsplit('_').next().unwrap().to_string();
    let spec = lab.spec(
        serde_json::json!([{"column": "note", "type": "text"}]),
        &["id"],
        None,
    );
    let err = lab
        .service
        .create_group(GroupCreateRequest {
            group_id: "bad".into(),
            total_records: 0,
            payload_size: 1,
            max_buffer_records: 8,
            max_buffer_bytes: 1024,
            batch_max_records: 1,
            batch_timeout_ms: 5_000,
            ordering_contract: "postgres-keyset".into(),
            connection_id: Some(format!("pg{n}")),
            source_spec: Some(spec.clone()),
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("unique index"), "{err}");
    let mut ack = spec;
    ack["acknowledge_unsafe"] = serde_json::json!(true);
    lab.service
        .create_group(GroupCreateRequest {
            group_id: "ok".into(),
            total_records: 0,
            payload_size: 1,
            max_buffer_records: 8,
            max_buffer_bytes: 1024,
            batch_max_records: 1,
            batch_timeout_ms: 5_000,
            ordering_contract: "postgres-keyset".into(),
            connection_id: Some(format!("pg{n}")),
            source_spec: Some(ack),
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn data_plane_consumes_postgres_rows() {
    let Some(lab) = Lab::open("id int8 primary key, body text not null").await else {
        return;
    };
    lab.admin
        .batch_execute(&format!(
            "INSERT INTO {} (id, body) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')",
            lab.table
        ))
        .await
        .unwrap();
    let spec = lab.spec(int_order("id", "int8"), &["body"], None);
    lab.group("g", spec, 2).await;
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
        expect_records: Some(4),
        idle_after_join: None,
        leave_after_join: false,
        duplicate_first_ack: false,
        ack_delay: Duration::ZERO,
        shared_progress: None,
        timeout: Duration::from_secs(10),
    })
    .await
    .unwrap();
    let mut ids = report.record_ids;
    ids.sort();
    ids.dedup();
    assert_eq!(ids, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn tens_of_thousands_of_rows() {
    let Some(lab) = Lab::open("id int8 primary key, body text not null").await else {
        return;
    };
    lab.admin
        .batch_execute(&format!(
            "INSERT INTO {} (id, body) SELECT g, 'x' FROM generate_series(1, 20000) g",
            lab.table
        ))
        .await
        .unwrap();
    let spec = lab.spec(int_order("id", "int8"), &["body"], None);
    lab.group("g", spec, 200).await;
    let mut ids = drain_i64(&lab.service, "g", "c").await;
    ids.sort();
    assert_eq!(ids.len(), 20_000);
    assert_eq!(ids.first().copied(), Some(1));
    assert_eq!(ids.last().copied(), Some(20_000));
}

#[tokio::test]
#[ignore = "millions of rows; run with --ignored against DATABASE_URL"]
async fn millions_of_rows() {
    let Some(lab) = Lab::open("id int8 primary key, body text not null").await else {
        return;
    };
    lab.admin
        .batch_execute(&format!(
            "INSERT INTO {} (id, body) SELECT g, 'x' FROM generate_series(1, 1000000) g",
            lab.table
        ))
        .await
        .unwrap();
    let spec = lab.spec(int_order("id", "int8"), &["body"], None);
    lab.group("g", spec, 500).await;
    let ids = drain_i64(&lab.service, "g", "c").await;
    assert_eq!(ids.len(), 1_000_000);
}

fn describe_err(
    result: Result<std::collections::HashMap<String, crate::catalog::ColumnInfo>, String>,
) -> String {
    match result {
        Ok(_) => panic!("expected a catalog error"),
        Err(err) => err,
    }
}

fn source_open(lab: &Lab, spec: serde_json::Value) -> SourceOpen {
    let url = database_url().expect("database");
    let (_, password) = PgEndpoint::from_database_url(&url).unwrap();
    SourceOpen {
        connection: ConnectionRecord {
            id: "pg".into(),
            kind: "postgres".into(),
            config_json: lab.cleanup.endpoint.config_json(),
            sealed_secret: SealedSecret {
                nonce: Vec::new(),
                ciphertext: Vec::new(),
            },
        },
        source_spec: spec,
        secret: password.into_bytes(),
    }
}

#[tokio::test]
async fn catalog_rejects_a_bad_contract() {
    let Some(lab) = Lab::open("id int8 primary key, note text, doc json").await else {
        return;
    };
    let good = SourceSpec::parse(&lab.spec(int_order("id", "int8"), &["note"], None)).unwrap();

    let mut missing_table = good.clone();
    missing_table.table = "missing_table".into();
    assert!(describe_err(describe(&lab.admin, &missing_table).await).contains("not found"));

    let mut missing_column = good.clone();
    missing_column.order_by[0].name = "missing".into();
    assert!(describe_err(describe(&lab.admin, &missing_column).await).contains("missing"));

    let mut wrong_type = good.clone();
    wrong_type.order_by[0].ty = crate::spec::ColType::Int4;
    assert!(describe_err(describe(&lab.admin, &wrong_type).await).contains("type"));

    let mut nullable = good.clone();
    nullable.order_by[0].name = "note".into();
    nullable.order_by[0].ty = crate::spec::ColType::Text;
    assert!(describe_err(describe(&lab.admin, &nullable).await).contains("nullable"));

    let mut missing_payload = good.clone();
    missing_payload.payload = vec!["gone".into()];
    assert!(describe_err(describe(&lab.admin, &missing_payload).await).contains("payload"));

    let mut unsupported = good.clone();
    unsupported.payload = vec!["doc".into()];
    assert!(describe_err(describe(&lab.admin, &unsupported).await).contains("unsupported"));
}

#[tokio::test]
async fn mixed_payload_types_and_filter() {
    let Some(lab) = Lab::open(
        "i2 int2 primary key, n4 int4 not null, n8 int8 not null, raw bytea not null, name varchar not null, note text, ts timestamptz",
    )
    .await
    else {
        return;
    };
    lab.admin
        .batch_execute(&format!(
            "INSERT INTO {} (i2, n4, n8, raw, name, note, ts) VALUES
             (1, 4, 8, decode('00ff','hex'), 'a', NULL, '1969-12-31 23:59:59+00'),
             (2, 5, 9, decode('01','hex'), 'b', 'ok', NULL)",
            lab.table
        ))
        .await
        .unwrap();
    let spec = lab.spec(
        int_order("i2", "int2"),
        &["i2", "n4", "n8", "raw", "name", "note", "ts"],
        Some("i2 > 0"),
    );
    lab.group("g", spec, 1).await;
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
    let mut payloads = Vec::new();
    let mut idle = 0u32;
    loop {
        match handle.assign(&cid).await {
            Ok(batch) => {
                idle = 0;
                for record in &batch.records {
                    payloads.push(
                        serde_json::from_slice::<serde_json::Value>(&record.payload).unwrap(),
                    );
                }
                handle.ack(batch.id).await.unwrap();
            }
            Err(RuntimeError::Core(_)) => {
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
    assert_eq!(payloads.len(), 2);
    assert_eq!(payloads[0]["i2"], 1);
    assert_eq!(payloads[0]["n4"], 4);
    assert_eq!(payloads[0]["n8"], 8);
    assert_eq!(payloads[0]["raw"], "00ff");
    assert_eq!(payloads[0]["name"], "a");
    assert!(payloads[0]["note"].is_null());
    assert_eq!(payloads[0]["ts"], -1_000_000);
    assert_eq!(payloads[1]["note"], "ok");
    assert!(payloads[1]["ts"].is_null());
}

#[tokio::test]
async fn bytea_key_resumes_in_byte_order() {
    let Some(lab) = Lab::open("raw bytea primary key, body text not null").await else {
        return;
    };
    lab.admin
        .batch_execute(&format!(
            "INSERT INTO {} (raw, body) VALUES (decode('00ff','hex'), 'a'), (decode('01','hex'), 'b')",
            lab.table
        ))
        .await
        .unwrap();
    let spec = lab.spec(
        serde_json::json!([{ "column": "raw", "type": "bytea" }]),
        &["body"],
        None,
    );
    lab.group("g", spec, 1).await;
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
    let mut keys = Vec::new();
    let mut idle = 0u32;
    loop {
        match handle.assign(&cid).await {
            Ok(batch) => {
                idle = 0;
                for record in &batch.records {
                    match record.ordering.atoms() {
                        [OrderingAtom::Bytes(bytes)] => keys.push(bytes.clone()),
                        other => panic!("unexpected ordering {other:?}"),
                    }
                }
                handle.ack(batch.id).await.unwrap();
            }
            Err(RuntimeError::Core(_)) => {
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
    assert_eq!(keys, vec![vec![0x00, 0xff], vec![0x01]]);
}

#[tokio::test]
async fn fetch_limit_zero_and_bad_cursor() {
    let Some(lab) = Lab::open("id int8 primary key, body text not null").await else {
        return;
    };
    let spec = lab.spec(int_order("id", "int8"), &["body"], None);
    let mut source = PostgresSource::open(source_open(&lab, spec)).await.unwrap();
    let empty: LogicalCursor = None;
    assert!(source.fetch_after(&empty, 0).await.unwrap().is_empty());
    let wide = Some(OrderingValue::new(vec![OrderingAtom::I64(1), OrderingAtom::I64(2)]).unwrap());
    let err = source.fetch_after(&wide, 1).await.unwrap_err();
    assert!(err.to_string().contains("cursor width"));
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

async fn try_group(
    lab: &Lab,
    id: &str,
    spec: serde_json::Value,
) -> Result<(), diavasi::control::ControlError> {
    let n = lab.table.rsplit('_').next().unwrap();
    lab.service
        .create_group(GroupCreateRequest {
            group_id: id.into(),
            total_records: 0,
            payload_size: 1,
            max_buffer_records: 256,
            max_buffer_bytes: 8 * 1024 * 1024,
            batch_max_records: 16,
            batch_timeout_ms: 5_000,
            ordering_contract: "postgres-keyset".into(),
            connection_id: Some(format!("pg{n}")),
            source_spec: Some(spec),
        })
        .await
        .map(|_| ())
}

/// B1: an order that is only a prefix of a unique index is not unique, so
/// keyset resume would skip rows. It needs `acknowledge_unsafe`. An order
/// that starts with every column of a unique index is accepted.
#[tokio::test]
async fn regress_b01_prefix_of_a_composite_unique_index_is_not_unique() {
    let Some(lab) =
        Lab::open("a int8 not null, b int8 not null, note text not null, unique (a, b)").await
    else {
        return;
    };
    let order = |cols: &[(&str, &str)]| {
        serde_json::Value::Array(
            cols.iter()
                .map(|(column, ty)| serde_json::json!({"column": column, "type": ty}))
                .collect(),
        )
    };
    let prefix = SourceSpec::parse(&lab.spec(order(&[("a", "int8")]), &["note"], None)).unwrap();
    let err = describe(&lab.admin, &prefix)
        .await
        .err()
        .expect("order_by [a] accepted although only (a, b) is unique");
    assert!(err.contains("unique"), "{err}");

    let exact =
        SourceSpec::parse(&lab.spec(order(&[("a", "int8"), ("b", "int8")]), &["note"], None))
            .unwrap();
    describe(&lab.admin, &exact)
        .await
        .expect("order_by [a, b] matches the unique index");

    let longer = SourceSpec::parse(&lab.spec(
        order(&[("a", "int8"), ("b", "int8"), ("note", "text")]),
        &["note"],
        None,
    ))
    .unwrap();
    describe(&lab.admin, &longer)
        .await
        .expect("order_by [a, b, note] starts with the unique index");
}

/// B14: a filter cannot close its own parentheses and bypass the keyset.
#[tokio::test]
async fn regress_b14_filter_cannot_escape_its_parentheses() {
    let Some(lab) = Lab::open("id int8 primary key, note text not null").await else {
        return;
    };
    let spec = lab.spec(int_order("id", "int8"), &["note"], Some("true) OR (true"));
    assert!(
        try_group(&lab, "g", spec).await.is_err(),
        "a filter that escapes its parentheses was accepted"
    );
}

/// B14: `--` inside a string literal is data, not a comment.
#[tokio::test]
async fn regress_b14_filter_may_contain_double_dash_in_a_literal() {
    let Some(lab) = Lab::open("id int8 primary key, note text not null").await else {
        return;
    };
    lab.admin
        .batch_execute(&format!(
            "INSERT INTO {} (id, note) VALUES (1, 'a--b'), (2, 'keep')",
            lab.table
        ))
        .await
        .unwrap();
    let spec = lab.spec(int_order("id", "int8"), &["note"], Some("note <> 'a--b'"));
    try_group(&lab, "g", spec)
        .await
        .expect("a literal containing -- was rejected");
    lab.service.start_group("g").await.unwrap();
    assert_eq!(drain_i64(&lab.service, "g", "c").await, vec![2]);
}
