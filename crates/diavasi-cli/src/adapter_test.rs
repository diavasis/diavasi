//! Seed one adapter backend and consume the rows through a short-lived server.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

use diavasi::control::{ServeConfig, serve};
use diavasi::dataplane::{ConsumerClient, ConsumerOptions, SharedProgress};
use diavasi::store::StoreKey;
use diavasi_adapter_mongodb::connect::{MongoEndpoint, connect as connect_mongo};
use diavasi_adapter_postgres::connect::{PgEndpoint, connect as connect_pg};
use diavasi_adapter_redis::connect::{RedisEndpoint, connect as connect_redis};
use mongodb::IndexModel;
use mongodb::bson::{Document, doc};
use mongodb::options::IndexOptions;
use redis::AsyncCommands;

use crate::sources::RoutingFactory;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdapterKind {
    Postgres,
    Mongodb,
    Redis,
}

impl AdapterKind {
    fn label(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Mongodb => "mongodb",
            Self::Redis => "redis",
        }
    }
}

pub struct AdapterTest {
    pub adapter: AdapterKind,
    pub records: u64,
    pub payload_bytes: usize,
    pub database_url: String,
    pub mongodb_url: String,
    pub redis_url: String,
    pub keep: bool,
    pub object: String,
    pub json: bool,
}

struct Report {
    adapter: &'static str,
    records: u64,
    payload_bytes: usize,
    seed_ms: u128,
    elapsed_ms: u128,
    records_per_sec: u64,
    mib_per_sec: f64,
    deliveries: u64,
}

pub async fn run(test: AdapterTest) -> Result<(), String> {
    if test.records == 0 || test.payload_bytes == 0 {
        return Err("records and payload bytes must be at least 1".into());
    }
    ident(&test.object)?;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let seeded = Instant::now();
    let prepared = prepare(&test).await?;
    let seed_ms = seeded.elapsed().as_millis();
    let outcome = consume(&test, &prepared).await;
    if !test.keep {
        cleanup(&test).await;
    }
    let report = outcome?;
    print_report(&Report { seed_ms, ..report }, test.json);
    Ok(())
}

struct Prepared {
    kind: String,
    config_json: serde_json::Value,
    secret: String,
    source_spec: serde_json::Value,
    ordering_contract: String,
}

async fn prepare(test: &AdapterTest) -> Result<Prepared, String> {
    match test.adapter {
        AdapterKind::Postgres => prepare_postgres(test).await,
        AdapterKind::Mongodb => prepare_mongodb(test).await,
        AdapterKind::Redis => prepare_redis(test).await,
    }
}

async fn prepare_postgres(test: &AdapterTest) -> Result<Prepared, String> {
    let (endpoint, password) = PgEndpoint::from_database_url(&test.database_url)?;
    let client = connect_pg(&endpoint).await?;
    let table = &test.object;
    let width =
        i32::try_from(test.payload_bytes).map_err(|_| "payload-bytes does not fit in i32")?;
    let n = i64::try_from(test.records).map_err(|_| "records does not fit in i64")?;
    client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {table};
             CREATE TABLE {table} (id int8 PRIMARY KEY, body text NOT NULL);
             INSERT INTO {table} (id, body)
             SELECT g, repeat('x', {width}) FROM generate_series(1, {n}) g"
        ))
        .await
        .map_err(|err| err.to_string())?;
    Ok(Prepared {
        kind: "postgres".into(),
        config_json: endpoint.config_json(),
        secret: password,
        source_spec: serde_json::json!({
            "table": table,
            "order_by": [{"column": "id", "type": "int8"}],
            "payload": ["body"],
        }),
        ordering_contract: "postgres-keyset".into(),
    })
}

async fn prepare_mongodb(test: &AdapterTest) -> Result<Prepared, String> {
    let endpoint = MongoEndpoint::from_url(&test.mongodb_url)?;
    let client = connect_mongo(&endpoint).await?;
    let database = client.database(&endpoint.database);
    let collection = database.collection::<Document>(&test.object);
    collection.drop().await.map_err(|err| err.to_string())?;
    database
        .create_collection(&test.object)
        .await
        .map_err(|err| err.to_string())?;
    let index = IndexModel::builder()
        .keys(doc! { "id": 1 })
        .options(IndexOptions::builder().unique(true).build())
        .build();
    collection
        .create_index(index)
        .await
        .map_err(|err| err.to_string())?;
    let payload = "x".repeat(test.payload_bytes);
    let mut id = 1i64;
    let n = i64::try_from(test.records).map_err(|_| "records does not fit in i64")?;
    while id <= n {
        let end = (id + 499).min(n);
        let mut docs = Vec::new();
        while id <= end {
            docs.push(doc! { "id": id, "body": &payload });
            id += 1;
        }
        collection
            .insert_many(docs)
            .await
            .map_err(|err| err.to_string())?;
    }
    let secret = endpoint.secret();
    Ok(Prepared {
        kind: "mongodb".into(),
        config_json: endpoint.config_json(),
        secret: if secret.is_empty() {
            "unused".into()
        } else {
            secret
        },
        source_spec: serde_json::json!({
            "collection": test.object,
            "order_by": [{"field": "id", "type": "int64", "direction": "asc"}],
            "fields": ["body"],
        }),
        ordering_contract: "mongodb-find-keyset".into(),
    })
}

async fn prepare_redis(test: &AdapterTest) -> Result<Prepared, String> {
    let endpoint = RedisEndpoint::from_url(&test.redis_url)?;
    let mut conn = connect_redis(&endpoint).await?;
    let payload = "x".repeat(test.payload_bytes);
    let _: () = conn
        .del(&test.object)
        .await
        .map_err(|err| err.to_string())?;
    let mut pipe = redis::pipe();
    for _ in 0..test.records {
        pipe.cmd("XADD")
            .arg(&test.object)
            .arg("*")
            .arg("body")
            .arg(&payload);
    }
    let _: Vec<redis::Value> = pipe
        .query_async(&mut conn)
        .await
        .map_err(|err| err.to_string())?;
    let secret = endpoint.secret();
    Ok(Prepared {
        kind: "redis".into(),
        config_json: endpoint.config_json(),
        secret: if secret.is_empty() {
            "unused".into()
        } else {
            secret
        },
        source_spec: serde_json::json!({
            "stream": test.object,
            "group": format!("{}-g", test.object),
        }),
        ordering_contract: "redis-stream".into(),
    })
}

async fn cleanup(test: &AdapterTest) {
    match test.adapter {
        AdapterKind::Postgres => {
            if let Ok((endpoint, _)) = PgEndpoint::from_database_url(&test.database_url) {
                if let Ok(client) = connect_pg(&endpoint).await {
                    let _ = client
                        .batch_execute(&format!("DROP TABLE IF EXISTS {}", test.object))
                        .await;
                }
            }
        }
        AdapterKind::Mongodb => {
            if let Ok(endpoint) = MongoEndpoint::from_url(&test.mongodb_url) {
                if let Ok(client) = connect_mongo(&endpoint).await {
                    let _ = client
                        .database(&endpoint.database)
                        .collection::<Document>(&test.object)
                        .drop()
                        .await;
                }
            }
        }
        AdapterKind::Redis => {
            if let Ok(endpoint) = RedisEndpoint::from_url(&test.redis_url) {
                if let Ok(mut conn) = connect_redis(&endpoint).await {
                    let _: redis::RedisResult<()> = conn.del(&test.object).await;
                }
            }
        }
    }
}

async fn consume(test: &AdapterTest, prepared: &Prepared) -> Result<Report, String> {
    let dir = std::env::temp_dir().join(format!(
        "diavasi-test-{}-{}",
        prepared.kind,
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).map_err(|err| err.to_string())?;
    let control = bind_addr()?;
    let data = bind_addr()?;
    let token = "adapter-test".to_string();
    let config = ServeConfig {
        bind: control,
        data_bind: data,
        store_path: dir.join("meta.redb"),
        api_token: token.clone(),
        store_key: Some(StoreKey::generate()),
        tls_cert: None,
        tls_key: None,
        source_factory: Some(Arc::new(RoutingFactory::installed())),
    };
    let server = tokio::spawn(async move {
        if let Err(err) = serve(config).await {
            tracing::error!("serve ended: {err}");
        }
    });
    let base = format!("http://{control}");
    let result = consume_group(test, prepared, &base, &data.to_string(), &token, &dir).await;
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
    result
}

async fn consume_group(
    test: &AdapterTest,
    prepared: &Prepared,
    base: &str,
    data_addr: &str,
    token: &str,
    dir: &std::path::Path,
) -> Result<Report, String> {
    wait_health(base).await?;
    let http = reqwest::Client::new();
    post(
        &http,
        token,
        &format!("{base}/v1/connections"),
        &serde_json::json!({
            "id": "src",
            "kind": prepared.kind,
            "config_json": prepared.config_json,
            "secret": prepared.secret,
        }),
    )
    .await?;
    let batch = 200.min(usize::try_from(test.records).unwrap_or(200)).max(1);
    let max_buffer_bytes = test
        .payload_bytes
        .saturating_mul(batch)
        .saturating_mul(4)
        .max(8 * 1024 * 1024);
    post(
        &http,
        token,
        &format!("{base}/v1/groups"),
        &serde_json::json!({
            "group_id": "t",
            "total_records": 0,
            "payload_size": test.payload_bytes,
            "max_buffer_records": 4_096,
            "max_buffer_bytes": max_buffer_bytes,
            "batch_max_records": batch,
            "batch_timeout_ms": 5_000,
            "ordering_contract": prepared.ordering_contract,
            "connection_id": "src",
            "source_spec": prepared.source_spec,
        }),
    )
    .await?;
    post(
        &http,
        token,
        &format!("{base}/v1/groups/t/start"),
        &serde_json::json!({}),
    )
    .await?;
    let ca = std::fs::read(dir.join("dataplane-ca.crt")).map_err(|err| err.to_string())?;
    let started = Instant::now();
    let report = ConsumerClient::run(ConsumerOptions {
        addr: data_addr.to_string(),
        ca_pem: ca,
        token: token.to_string(),
        group_id: "t".into(),
        consumer_id: "c".into(),
        max_in_flight: 2,
        stop_after_batches: None,
        expect_records: None,
        idle_after_join: None,
        leave_after_join: false,
        duplicate_first_ack: false,
        ack_delay: Duration::ZERO,
        shared_progress: Some(SharedProgress {
            acked: Arc::new(AtomicU64::new(0)),
            target: test.records,
        }),
        timeout: Duration::from_secs(30 + test.records / 50),
    })
    .await
    .map_err(|err| err.to_string())?;
    let elapsed = started.elapsed();
    let deliveries = report.record_ids.len() as u64;
    if deliveries != test.records {
        return Err(format!(
            "expected {} deliveries, got {deliveries}",
            test.records
        ));
    }
    if test.adapter != AdapterKind::Redis {
        let distinct = report
            .record_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>()
            .len() as u64;
        if distinct != test.records {
            return Err(format!(
                "expected {} distinct ids, got {distinct}",
                test.records
            ));
        }
    }
    let seconds = elapsed.as_secs_f64().max(0.000_001);
    let bytes = deliveries * test.payload_bytes as u64;
    let mib = (bytes as f64 / seconds) / (1024.0 * 1024.0);
    Ok(Report {
        adapter: test.adapter.label(),
        records: test.records,
        payload_bytes: test.payload_bytes,
        seed_ms: 0,
        elapsed_ms: elapsed.as_millis(),
        records_per_sec: (deliveries as f64 / seconds).round() as u64,
        mib_per_sec: (mib * 1000.0).round() / 1000.0,
        deliveries,
    })
}

fn print_report(report: &Report, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::json!({
                "adapter": report.adapter,
                "records": report.records,
                "payload_bytes": report.payload_bytes,
                "seed_ms": report.seed_ms,
                "elapsed_ms": report.elapsed_ms,
                "records_per_sec": report.records_per_sec,
                "mib_per_sec": report.mib_per_sec,
                "deliveries": report.deliveries,
            })
        );
        return;
    }
    println!(
        "{}  records={}  payload_bytes={}",
        report.adapter, report.records, report.payload_bytes
    );
    println!("seed     {} ms", report.seed_ms);
    println!(
        "consume  {} ms  {} records/s  {} MiB/s",
        report.elapsed_ms, report.records_per_sec, report.mib_per_sec
    );
}

fn ident(name: &str) -> Result<(), String> {
    if name.is_empty()
        || !name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err("object name must be letters, digits, and underscores".into());
    }
    Ok(())
}

fn bind_addr() -> Result<SocketAddr, String> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|err| err.to_string())?;
    listener.local_addr().map_err(|err| err.to_string())
}

async fn wait_health(base: &str) -> Result<(), String> {
    let http = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if http
            .get(format!("{base}/health"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err("control plane did not become healthy".into())
}

async fn post(
    http: &reqwest::Client,
    token: &str,
    url: &str,
    body: &serde_json::Value,
) -> Result<(), String> {
    let response = http
        .post(url)
        .bearer_auth(token)
        .json(body)
        .send()
        .await
        .map_err(|err| err.to_string())?;
    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(format!("{url} -> {status}: {text}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(adapter: AdapterKind, object: &str) -> AdapterTest {
        AdapterTest {
            adapter,
            records: 8,
            payload_bytes: 16,
            database_url: std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://diavasi:diavasi@127.0.0.1:5433/diavasi".into()),
            mongodb_url: std::env::var("MONGODB_URL")
                .unwrap_or_else(|_| "mongodb://127.0.0.1:27017".into()),
            redis_url: std::env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://127.0.0.1:6379".into()),
            keep: false,
            object: object.into(),
            json: true,
        }
    }

    #[tokio::test]
    async fn rejects_an_empty_seed() {
        let mut test = sample(AdapterKind::Redis, "diavasi_test");
        test.records = 0;
        let err = run(test).await.unwrap_err();
        assert!(err.contains("at least 1"), "{err}");
    }

    #[tokio::test]
    async fn postgres_seed_and_consume() {
        if std::env::var("DATABASE_URL")
            .ok()
            .filter(|url| !url.is_empty())
            .is_none()
        {
            return;
        }
        let object = format!("dt_pg_{}", std::process::id());
        run(sample(AdapterKind::Postgres, &object))
            .await
            .expect("postgres");
    }

    #[tokio::test]
    async fn mongodb_seed_and_consume() {
        if std::env::var("MONGODB_URL")
            .ok()
            .filter(|url| !url.is_empty())
            .is_none()
        {
            return;
        }
        let object = format!("dt_mg_{}", std::process::id());
        run(sample(AdapterKind::Mongodb, &object))
            .await
            .expect("mongodb");
    }

    #[tokio::test]
    async fn redis_seed_and_consume() {
        if std::env::var("REDIS_URL")
            .ok()
            .filter(|url| !url.is_empty())
            .is_none()
        {
            return;
        }
        let object = format!("dt_rd_{}", std::process::id());
        run(sample(AdapterKind::Redis, &object))
            .await
            .expect("redis");
    }
}
