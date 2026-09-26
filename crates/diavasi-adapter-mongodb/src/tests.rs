use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use diavasi::control::{ConnectionCreateRequest, ControlService, GroupCreateRequest};
use diavasi::core::{ConsumerId, GroupId, OrderingAtom, RecordSource};
use diavasi::dataplane::{
    ConsumerClient, ConsumerOptions, DataPlaneConfig, generate_self_signed, serve_dataplane,
};
use diavasi::runtime::RuntimeError;
use diavasi::store::{RedbStore, StateStore, StoreKey};
use mongodb::Client;
use mongodb::bson::binary::Binary;
use mongodb::bson::doc;
use mongodb::bson::oid::ObjectId;
use mongodb::bson::spec::BinarySubtype;
use mongodb::bson::{Bson, DateTime, Document};
use mongodb::options::IndexOptions;
use mongodb::{Collection, IndexModel};

use crate::MongoFactory;
use crate::connect::{MongoEndpoint, connect};
use crate::reader::MongoSource;

static N: AtomicU64 = AtomicU64::new(0);

fn mongodb_url() -> Option<String> {
    std::env::var("MONGODB_URL")
        .ok()
        .filter(|url| !url.is_empty())
}

struct Cleanup {
    client: Client,
    database: String,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        let client = self.client.clone();
        let database = self.database.clone();
        tokio::spawn(async move {
            let _ = client.database(&database).drop().await;
        });
    }
}

struct Lab {
    _dir: tempfile::TempDir,
    service: Arc<ControlService>,
    admin: Client,
    database: String,
    collection: String,
    connection_id: String,
    key: StoreKey,
    path: std::path::PathBuf,
    endpoint: MongoEndpoint,
    cleanup: Cleanup,
}

impl Lab {
    async fn open() -> Option<Self> {
        let url = mongodb_url()?;
        let mut endpoint = MongoEndpoint::from_url(&url).expect("MONGODB_URL");
        let n = N.fetch_add(1, Ordering::Relaxed);
        endpoint.database = format!("s8_{}_{n}", std::process::id());
        let admin = connect(&endpoint).await.expect("connect");
        admin
            .database("admin")
            .run_command(doc! { "ping": 1 })
            .await
            .expect("ping");
        let collection = "docs".to_string();
        admin
            .database(&endpoint.database)
            .create_collection(&collection)
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
        service.install_source_factory(Arc::new(MongoFactory)).await;
        let connection_id = format!("mg{n}");
        service
            .create_connection(ConnectionCreateRequest {
                id: connection_id.clone(),
                kind: "mongodb".into(),
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
        let cleanup = Cleanup {
            client: admin.clone(),
            database: endpoint.database.clone(),
        };
        Some(Self {
            _dir: dir,
            service,
            admin,
            database: endpoint.database.clone(),
            collection,
            connection_id,
            key,
            path,
            endpoint,
            cleanup,
        })
    }

    fn coll(&self) -> Collection<Document> {
        self.admin
            .database(&self.database)
            .collection(&self.collection)
    }

    async fn unique_index(&self, keys: Document) {
        let options = IndexOptions::builder().unique(true).build();
        let model = IndexModel::builder().keys(keys).options(options).build();
        self.coll().create_index(model).await.unwrap();
    }

    fn spec(&self, extra: serde_json::Value) -> serde_json::Value {
        let mut spec = serde_json::json!({ "collection": self.collection });
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
                payload_size: 1,
                max_buffer_records: 256,
                max_buffer_bytes: 8 * 1024 * 1024,
                batch_max_records: batch,
                batch_timeout_ms: 5_000,
                ordering_contract: "mongodb-find-keyset".into(),
                connection_id: Some(self.connection_id.clone()),
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
            database,
            collection,
            connection_id,
            key,
            path,
            service,
            endpoint,
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
        service.install_source_factory(Arc::new(MongoFactory)).await;
        Self {
            _dir,
            service,
            admin,
            database,
            collection,
            connection_id,
            key,
            path,
            endpoint,
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

fn int_order(field: &str, ty: &str, direction: &str) -> serde_json::Value {
    serde_json::json!([{ "field": field, "type": ty, "direction": direction }])
}

async fn take_one(service: &ControlService, group: &str, consumer: &str) -> diavasi::core::Batch {
    let gid = GroupId::new(group).unwrap();
    let handle = service.supervisor().lock().await.get_handle(&gid).unwrap();
    let cid = ConsumerId::new(consumer).unwrap();
    handle.join(cid.clone()).await.unwrap();
    loop {
        match handle.assign(&cid).await {
            Ok(batch) => return batch,
            Err(RuntimeError::Core(_)) => tokio::time::sleep(Duration::from_millis(5)).await,
            Err(err) => panic!("{err}"),
        }
    }
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
    service.install_source_factory(Arc::new(MongoFactory)).await;
    service
        .create_connection(ConnectionCreateRequest {
            id: "r".into(),
            kind: "redis".into(),
            config_json: serde_json::json!({}),
            secret: "x".into(),
        })
        .unwrap();
    let err = service
        .create_group(GroupCreateRequest {
            group_id: "g".into(),
            total_records: 0,
            payload_size: 1,
            max_buffer_records: 8,
            max_buffer_bytes: 1024,
            batch_max_records: 1,
            batch_timeout_ms: 5_000,
            ordering_contract: "redis".into(),
            connection_id: Some("r".into()),
            source_spec: Some(serde_json::json!({"collection": "docs"})),
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("unsupported"), "{err}");
}

#[tokio::test]
async fn default_id_round_trip() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    let first = ObjectId::from_bytes([0; 12]);
    let second = ObjectId::from_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    lab.coll()
        .insert_many(vec![
            doc! { "_id": second, "body": "b" },
            doc! { "_id": first, "body": "a" },
        ])
        .await
        .unwrap();
    lab.group("g", lab.spec(serde_json::json!({})), 2).await;
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
    assert_eq!(keys, vec![first.bytes().to_vec(), second.bytes().to_vec()]);
}

#[tokio::test]
async fn int_key_round_trip() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.unique_index(doc! { "id": 1 }).await;
    lab.coll()
        .insert_many(vec![
            doc! { "id": 1i64, "body": "a" },
            doc! { "id": 2i64, "body": "b" },
            doc! { "id": 3i64, "body": "c" },
        ])
        .await
        .unwrap();
    let spec = lab.spec(serde_json::json!({
        "order_by": int_order("id", "int64", "asc"),
        "fields": ["body"],
    }));
    lab.group("g", spec, 2).await;
    let mut ids = drain_i64(&lab.service, "g", "c1").await;
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3]);
}

#[tokio::test]
async fn empty_collection_assigns_nothing() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.unique_index(doc! { "id": 1 }).await;
    let spec = lab.spec(serde_json::json!({
        "order_by": int_order("id", "int64", "asc"),
    }));
    lab.group("g", spec, 2).await;
    let ids = drain_i64(&lab.service, "g", "c1").await;
    assert!(ids.is_empty());
    let cursor = lab.service.checkpoint("g").await.unwrap().durable_cursor;
    assert!(cursor.is_none());
}

#[tokio::test]
async fn string_key_uses_byte_order() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.unique_index(doc! { "name": 1 }).await;
    lab.coll()
        .insert_many(vec![
            doc! { "name": "b", "body": "2" },
            doc! { "name": "a", "body": "1" },
            doc! { "name": "c", "body": "3" },
        ])
        .await
        .unwrap();
    let spec = lab.spec(serde_json::json!({
        "order_by": [{ "field": "name", "type": "string", "direction": "asc" }],
    }));
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
    let mut idle = 0u32;
    loop {
        match handle.assign(&cid).await {
            Ok(batch) => {
                idle = 0;
                for record in &batch.records {
                    match record.ordering.atoms() {
                        [OrderingAtom::Bytes(bytes)] => {
                            names.push(String::from_utf8(bytes.clone()).unwrap())
                        }
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
    assert_eq!(names, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn compound_asc_desc() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.unique_index(doc! { "k": 1, "name": -1 }).await;
    lab.coll()
        .insert_many(vec![
            doc! { "k": 1i64, "name": "a" },
            doc! { "k": 1i64, "name": "b" },
            doc! { "k": 2i64, "name": "a" },
        ])
        .await
        .unwrap();
    let spec = lab.spec(serde_json::json!({
        "order_by": [
            { "field": "k", "type": "int64", "direction": "asc" },
            { "field": "name", "type": "string", "direction": "desc" }
        ]
    }));
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
                    match record.ordering.atoms() {
                        [OrderingAtom::I64(k), OrderingAtom::Bytes(name)] => {
                            let name = crate::spec::atom_bytes_to_canonical(
                                name,
                                crate::spec::Direction::Desc,
                            )
                            .unwrap();
                            keys.push((*k, String::from_utf8(name).unwrap()));
                        }
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
    assert_eq!(
        keys,
        vec![(1, "b".into()), (1, "a".into()), (2, "a".into())]
    );
}

#[tokio::test]
async fn date_bool_and_binary_keys() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.unique_index(doc! { "when": 1 }).await;
    let early = DateTime::from_millis(1_700_000_000_000);
    let late = DateTime::from_millis(1_700_000_100_000);
    lab.coll()
        .insert_many(vec![
            doc! { "when": late, "kind": "date" },
            doc! { "when": early, "kind": "date" },
        ])
        .await
        .unwrap();
    let spec = lab.spec(serde_json::json!({
        "order_by": [{ "field": "when", "type": "date", "direction": "asc" }]
    }));
    lab.group("dates", spec, 10).await;
    assert_eq!(
        drain_i64(&lab.service, "dates", "c").await,
        vec![early.timestamp_millis(), late.timestamp_millis()]
    );

    lab.admin
        .database(&lab.database)
        .create_collection("flags")
        .await
        .unwrap();
    let flags = lab
        .admin
        .database(&lab.database)
        .collection::<Document>("flags");
    flags
        .create_index(
            IndexModel::builder()
                .keys(doc! { "on": 1 })
                .options(IndexOptions::builder().unique(true).build())
                .build(),
        )
        .await
        .unwrap();
    flags
        .insert_many(vec![doc! { "on": true }, doc! { "on": false }])
        .await
        .unwrap();
    lab.group(
        "flags",
        serde_json::json!({
            "collection": "flags",
            "order_by": [{ "field": "on", "type": "bool", "direction": "asc" }]
        }),
        10,
    )
    .await;
    assert_eq!(drain_i64(&lab.service, "flags", "c").await, vec![0, 1]);

    lab.admin
        .database(&lab.database)
        .create_collection("bins")
        .await
        .unwrap();
    let bins = lab
        .admin
        .database(&lab.database)
        .collection::<Document>("bins");
    bins.create_index(
        IndexModel::builder()
            .keys(doc! { "blob": 1 })
            .options(IndexOptions::builder().unique(true).build())
            .build(),
    )
    .await
    .unwrap();
    bins.insert_many(vec![
        doc! { "blob": Bson::Binary(Binary { subtype: BinarySubtype::Generic, bytes: vec![2] }) },
        doc! { "blob": Bson::Binary(Binary { subtype: BinarySubtype::Generic, bytes: vec![1] }) },
    ])
    .await
    .unwrap();
    lab.group(
        "bins",
        serde_json::json!({
            "collection": "bins",
            "order_by": [{ "field": "blob", "type": "binData", "direction": "asc" }]
        }),
        10,
    )
    .await;
    let gid = GroupId::new("bins").unwrap();
    let handle = lab
        .service
        .supervisor()
        .lock()
        .await
        .get_handle(&gid)
        .unwrap();
    let cid = ConsumerId::new("c").unwrap();
    handle.join(cid.clone()).await.unwrap();
    let mut blobs = Vec::new();
    let mut idle = 0u32;
    loop {
        match handle.assign(&cid).await {
            Ok(batch) => {
                idle = 0;
                for record in &batch.records {
                    match record.ordering.atoms() {
                        [OrderingAtom::Bytes(bytes)] => blobs.push(bytes.clone()),
                        other => panic!("{other:?}"),
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
    assert_eq!(blobs, vec![vec![0, 1], vec![0, 2]]);
}

#[tokio::test]
async fn inserts_ahead_appear_and_inserts_behind_do_not() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.unique_index(doc! { "id": 1 }).await;
    lab.coll()
        .insert_many(vec![
            doc! { "id": 1i64, "body": "a" },
            doc! { "id": 2i64, "body": "b" },
        ])
        .await
        .unwrap();
    let spec = lab.spec(serde_json::json!({
        "order_by": int_order("id", "int64", "asc")
    }));
    lab.group("g", spec, 1).await;
    assert_eq!(drain_i64(&lab.service, "g", "c1").await, vec![1, 2]);
    lab.coll()
        .insert_many(vec![
            doc! { "id": 0i64, "body": "behind" },
            doc! { "id": 3i64, "body": "ahead" },
        ])
        .await
        .unwrap();
    lab.service
        .supervisor()
        .lock()
        .await
        .get_handle(&GroupId::new("g").unwrap())
        .unwrap()
        .leave(&ConsumerId::new("c1").unwrap())
        .await
        .unwrap();
    assert_eq!(drain_i64(&lab.service, "g", "c2").await, vec![3]);
}

#[tokio::test]
async fn delete_and_field_update_do_not_redeliver() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.unique_index(doc! { "id": 1 }).await;
    lab.coll()
        .insert_many(vec![
            doc! { "id": 1i64, "body": "a" },
            doc! { "id": 2i64, "body": "b" },
            doc! { "id": 3i64, "body": "c" },
        ])
        .await
        .unwrap();
    let spec = lab.spec(serde_json::json!({
        "order_by": int_order("id", "int64", "asc"),
        "fields": ["body"],
    }));
    lab.group("g", spec, 1).await;
    let batch = take_one(&lab.service, "g", "c").await;
    assert_eq!(batch.records[0].ordering.atoms(), [OrderingAtom::I64(1)]);
    let payload = String::from_utf8(batch.records[0].payload.to_vec()).unwrap();
    assert!(payload.contains("\"body\""), "{payload}");
    assert!(!payload.contains("secret"), "{payload}");
    let gid = GroupId::new("g").unwrap();
    lab.service
        .supervisor()
        .lock()
        .await
        .get_handle(&gid)
        .unwrap()
        .ack(batch.id)
        .await
        .unwrap();
    lab.service
        .supervisor()
        .lock()
        .await
        .abort_group(&gid)
        .unwrap();
    lab.coll()
        .update_one(doc! { "id": 1i64 }, doc! { "$set": { "body": "changed" } })
        .await
        .unwrap();
    lab.coll().delete_one(doc! { "id": 3i64 }).await.unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    lab.service.supervise_once().await.unwrap();
    assert_eq!(drain_i64(&lab.service, "g", "c2").await, vec![2]);
}

#[tokio::test]
async fn ordering_field_update_breaks_the_contract() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.unique_index(doc! { "seq": 1 }).await;
    lab.coll()
        .insert_many(vec![
            doc! { "seq": 1i64, "body": "a" },
            doc! { "seq": 2i64, "body": "b" },
            doc! { "seq": 3i64, "body": "c" },
        ])
        .await
        .unwrap();
    let spec = lab.spec(serde_json::json!({
        "order_by": int_order("seq", "int64", "asc")
    }));
    lab.group("g", spec, 1).await;
    let batch = take_one(&lab.service, "g", "c").await;
    let gid = GroupId::new("g").unwrap();
    lab.service
        .supervisor()
        .lock()
        .await
        .get_handle(&gid)
        .unwrap()
        .ack(batch.id)
        .await
        .unwrap();
    lab.service
        .supervisor()
        .lock()
        .await
        .abort_group(&gid)
        .unwrap();
    lab.coll()
        .update_one(doc! { "seq": 2i64 }, doc! { "$set": { "seq": 0i64 } })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    lab.service.supervise_once().await.unwrap();
    let rest = drain_i64(&lab.service, "g", "c2").await;
    assert_eq!(
        rest,
        vec![3],
        "document moved behind the cursor is not delivered"
    );
}

#[tokio::test]
async fn filter_skips_non_matching_documents() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.unique_index(doc! { "id": 1 }).await;
    lab.coll()
        .insert_many(vec![
            doc! { "id": 1i64, "keep": true },
            doc! { "id": 2i64, "keep": false },
            doc! { "id": 3i64, "keep": true },
        ])
        .await
        .unwrap();
    let spec = lab.spec(serde_json::json!({
        "order_by": int_order("id", "int64", "asc"),
        "filter": { "keep": true }
    }));
    lab.group("g", spec, 10).await;
    assert_eq!(drain_i64(&lab.service, "g", "c").await, vec![1, 3]);
}

#[tokio::test]
async fn restart_resumes_from_committed_cursor() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.unique_index(doc! { "id": 1 }).await;
    let docs: Vec<_> = (1..=6)
        .map(|id| doc! { "id": id as i64, "body": "x" })
        .collect();
    lab.coll().insert_many(docs).await.unwrap();
    let spec = lab.spec(serde_json::json!({
        "order_by": int_order("id", "int64", "asc")
    }));
    lab.group("g", spec, 1).await;
    let batch = take_one(&lab.service, "g", "c").await;
    lab.service
        .supervisor()
        .lock()
        .await
        .get_handle(&GroupId::new("g").unwrap())
        .unwrap()
        .ack(batch.id)
        .await
        .unwrap();
    let lab = lab.reopen().await;
    lab.service.start_group("g").await.unwrap();
    assert_eq!(
        drain_i64(&lab.service, "g", "c2").await,
        vec![2, 3, 4, 5, 6]
    );
}

#[tokio::test]
async fn two_consumers_and_crash_cover_the_uncommitted_tail() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.unique_index(doc! { "id": 1 }).await;
    let docs: Vec<_> = (1..=6)
        .map(|id| doc! { "id": id as i64, "body": "x" })
        .collect();
    lab.coll().insert_many(docs).await.unwrap();
    let spec = lab.spec(serde_json::json!({
        "order_by": int_order("id", "int64", "asc")
    }));
    lab.group("g", spec.clone(), 2).await;
    let mut a = drain_i64(&lab.service, "g", "a").await;
    lab.service
        .supervisor()
        .lock()
        .await
        .get_handle(&GroupId::new("g").unwrap())
        .unwrap()
        .leave(&ConsumerId::new("a").unwrap())
        .await
        .unwrap();
    let mut b = drain_i64(&lab.service, "g", "b").await;
    a.append(&mut b);
    a.sort();
    a.dedup();
    assert_eq!(a, vec![1, 2, 3, 4, 5, 6]);

    lab.group("g2", spec, 1).await;
    let inflight = take_one(&lab.service, "g2", "drop").await;
    assert!(!inflight.records.is_empty());
    drop(inflight);
    lab.service
        .supervisor()
        .lock()
        .await
        .abort_group(&GroupId::new("g2").unwrap())
        .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    lab.service.supervise_once().await.unwrap();
    let mut ids = drain_i64(&lab.service, "g2", "resume").await;
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn connection_loss_reconnects() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.unique_index(doc! { "id": 1 }).await;
    lab.coll()
        .insert_many(vec![
            doc! { "id": 1i64, "body": "a" },
            doc! { "id": 2i64, "body": "b" },
            doc! { "id": 3i64, "body": "c" },
        ])
        .await
        .unwrap();
    let mut request_endpoint = lab.endpoint.clone();
    request_endpoint.app_name = "diavasi:docs".into();
    let secret = request_endpoint.secret().into_bytes();
    let mut source = MongoSource::open(diavasi::runtime::SourceOpen {
        connection: lab
            .service
            .store()
            .get_connection(&lab.connection_id)
            .unwrap()
            .unwrap(),
        source_spec: lab.spec(serde_json::json!({
            "order_by": int_order("id", "int64", "asc")
        })),
        secret,
    })
    .await
    .unwrap();
    let first = source.fetch_after(&None, 1).await.unwrap();
    assert_eq!(first[0].ordering.atoms(), [OrderingAtom::I64(1)]);
    source.poison().await.unwrap();
    let cursor = Some(first[0].ordering.clone());
    let rest = source.fetch_after(&cursor, 10).await.unwrap();
    let ids: Vec<_> = rest
        .iter()
        .map(|record| match record.ordering.atoms() {
            [OrderingAtom::I64(id)] => *id,
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(ids, vec![2, 3]);
}

#[tokio::test]
async fn unsafe_contract_requires_acknowledgement() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    let spec = lab.spec(serde_json::json!({
        "order_by": [{ "field": "note", "type": "string", "direction": "asc" }]
    }));
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
            ordering_contract: "mongodb-find-keyset".into(),
            connection_id: Some(lab.connection_id.clone()),
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
            ordering_contract: "mongodb-find-keyset".into(),
            connection_id: Some(lab.connection_id.clone()),
            source_spec: Some(ack),
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn data_plane_consumes_mongodb_documents() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.unique_index(doc! { "id": 1 }).await;
    lab.coll()
        .insert_many(vec![
            doc! { "id": 1i64, "body": "a" },
            doc! { "id": 2i64, "body": "b" },
            doc! { "id": 3i64, "body": "c" },
            doc! { "id": 4i64, "body": "d" },
        ])
        .await
        .unwrap();
    let spec = lab.spec(serde_json::json!({
        "order_by": int_order("id", "int64", "asc")
    }));
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
async fn tens_of_thousands_of_documents() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.unique_index(doc! { "id": 1 }).await;
    for start in (1..=20_000).step_by(1_000) {
        let docs: Vec<_> = (start..start + 1_000)
            .map(|id| doc! { "id": id as i64, "body": "x" })
            .collect();
        lab.coll().insert_many(docs).await.unwrap();
    }
    let spec = lab.spec(serde_json::json!({
        "order_by": int_order("id", "int64", "asc")
    }));
    lab.group("g", spec, 200).await;
    let mut ids = drain_i64(&lab.service, "g", "c").await;
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 20_000);
    assert_eq!(ids.first().copied(), Some(1));
    assert_eq!(ids.last().copied(), Some(20_000));
}

#[tokio::test]
#[ignore = "millions of documents; not the CI run"]
async fn millions_of_documents() {
    let Some(lab) = Lab::open().await else {
        return;
    };
    lab.unique_index(doc! { "id": 1 }).await;
    for start in (1..=1_000_000).step_by(5_000) {
        let docs: Vec<_> = (start..start + 5_000)
            .map(|id| doc! { "id": id as i64, "body": "x" })
            .collect();
        lab.coll().insert_many(docs).await.unwrap();
    }
    let spec = lab.spec(serde_json::json!({
        "order_by": int_order("id", "int64", "asc")
    }));
    lab.group("g", spec, 500).await;
    let ids = drain_i64(&lab.service, "g", "c").await;
    assert_eq!(ids.len(), 1_000_000);
}

#[tokio::test]
async fn missing_collection_is_rejected() {
    let Some(lab) = Lab::open().await else {
        return;
    };
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
            ordering_contract: "mongodb-find-keyset".into(),
            connection_id: Some(lab.connection_id.clone()),
            source_spec: Some(serde_json::json!({"collection": "nope"})),
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("was not found"), "{err}");
}
