//! End-to-end benchmark: PostgreSQL keyset fetch, group buffer, gRPC consumers, ack.
//!
//! Separate from `diavasi-transport-bench`. One invocation appends one JSONL row.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

use clap::Parser;
use diavasi::control::{ServeConfig, serve};
use diavasi::dataplane::{ConsumerClient, ConsumerOptions, SharedProgress};
use diavasi::store::StoreKey;
use diavasi_adapter_postgres::PostgresFactory;
use diavasi_adapter_postgres::connect::{PgEndpoint, connect};

#[derive(Parser)]
#[command(name = "diavasi-e2e-bench")]
struct Args {
    /// Postgres URL used both to load the table and as the group connection.
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
    /// Tens of thousands of rows, one consumer, one group.
    #[arg(long)]
    smoke: bool,
    #[arg(long, default_value_t = 20_000)]
    rows: u64,
    #[arg(long, default_value_t = 64)]
    payload_bytes: usize,
    #[arg(long, default_value_t = 200)]
    batch_max_records: usize,
    #[arg(long, default_value_t = 1)]
    consumers: usize,
    #[arg(long, default_value_t = 1)]
    groups: usize,
    #[arg(long, default_value_t = 0)]
    ack_delay_ms: u64,
    #[arg(long, default_value_t = 4_096)]
    max_buffer_records: usize,
    #[arg(long, default_value_t = 32 * 1024 * 1024)]
    max_buffer_bytes: usize,
    #[arg(long, default_value_t = 1)]
    max_in_flight: u32,
    #[arg(long, default_value = "docs/bench/stage-07.jsonl")]
    output: PathBuf,
    #[arg(long, default_value = "run")]
    label: String,
}

#[tokio::main]
async fn main() {
    let mut args = Args::parse();
    if args.smoke {
        args.rows = 20_000;
        args.payload_bytes = 64;
        args.batch_max_records = 200;
        args.consumers = 1;
        args.groups = 1;
        args.ack_delay_ms = 0;
        args.max_in_flight = 1;
        args.label = "smoke".into();
    }
    if let Err(err) = run(args).await {
        eprintln!("error: {err}");
        process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), String> {
    if args.rows == 0 || args.consumers == 0 || args.groups == 0 || args.max_in_flight == 0 {
        return Err("rows, consumers, groups, and max-in-flight must be at least 1".into());
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "error".into()),
        )
        .try_init();
    let (endpoint, password) = PgEndpoint::from_database_url(&args.database_url)?;
    let table = format!("s7_{}", process::id());
    prepare_table(&endpoint, &table, args.rows, args.payload_bytes).await?;
    let outcome = bench(&args, &endpoint, &password, &table).await;
    drop_table(&endpoint, &table).await;
    outcome
}

async fn prepare_table(
    endpoint: &PgEndpoint,
    table: &str,
    rows: u64,
    payload_bytes: usize,
) -> Result<(), String> {
    let client = connect(endpoint).await?;
    client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {table}; CREATE TABLE {table} (id int8 PRIMARY KEY, body text NOT NULL)"
        ))
        .await
        .map_err(|err| err.to_string())?;
    let width = i32::try_from(payload_bytes).map_err(|_| "payload-bytes does not fit in i32")?;
    let n = i64::try_from(rows).map_err(|_| "rows does not fit in i64")?;
    client
        .batch_execute(&format!(
            "INSERT INTO {table} (id, body) SELECT g, repeat('x', {width}) FROM generate_series(1, {n}) g"
        ))
        .await
        .map_err(|err| err.to_string())?;
    Ok(())
}

async fn drop_table(endpoint: &PgEndpoint, table: &str) {
    if let Ok(client) = connect(endpoint).await {
        let _ = client
            .batch_execute(&format!("DROP TABLE IF EXISTS {table}"))
            .await;
    }
}

async fn bench(
    args: &Args,
    endpoint: &PgEndpoint,
    password: &str,
    table: &str,
) -> Result<(), String> {
    let dir = std::env::temp_dir().join(format!("diavasi-e2e-{}", process::id()));
    std::fs::create_dir_all(&dir).map_err(|err| err.to_string())?;
    let control = bind_addr()?;
    let data = bind_addr()?;
    let token = "bench-token".to_string();
    let config = ServeConfig {
        bind: control,
        data_bind: data,
        store_path: dir.join("meta.redb"),
        api_token: token.clone(),
        store_key: Some(StoreKey::generate()),
        tls_cert: None,
        tls_key: None,
        source_factory: Some(Arc::new(PostgresFactory)),
        checkpoint_interval: std::time::Duration::ZERO,
    };
    let server = tokio::spawn(async move {
        if let Err(err) = serve(config).await {
            tracing::error!("serve ended: {err}");
        }
    });

    let base = format!("http://{control}");
    wait_health(&base).await?;
    let http = reqwest::Client::new();
    let spec = serde_json::json!({
        "table": table,
        "order_by": [{"column": "id", "type": "int8"}],
        "payload": ["body"],
    });
    post(
        &http,
        &token,
        &format!("{base}/v1/connections"),
        &serde_json::json!({
            "id": "pg",
            "kind": "postgres",
            "config_json": endpoint.config_json(),
            "secret": password,
        }),
    )
    .await?;

    for group in 0..args.groups {
        post(
            &http,
            &token,
            &format!("{base}/v1/groups"),
            &serde_json::json!({
                "group_id": format!("g{group}"),
                "total_records": 0,
                "payload_size": args.payload_bytes,
                "max_buffer_records": args.max_buffer_records,
                "max_buffer_bytes": args.max_buffer_bytes,
                "batch_max_records": args.batch_max_records,
                "batch_timeout_ms": 5_000,
                "ordering_contract": "postgres-keyset",
                "connection_id": "pg",
                "source_spec": spec,
            }),
        )
        .await?;
        post(
            &http,
            &token,
            &format!("{base}/v1/groups/g{group}/start"),
            &serde_json::json!({}),
        )
        .await?;
    }

    let ca = std::fs::read(dir.join("dataplane-ca.crt")).map_err(|err| err.to_string())?;
    let started = Instant::now();
    let mut tasks = Vec::new();
    for group in 0..args.groups {
        let progress = SharedProgress {
            acked: Arc::new(AtomicU64::new(0)),
            target: args.rows,
        };
        for consumer in 0..args.consumers {
            let opts = ConsumerOptions {
                addr: data.to_string(),
                ca_pem: ca.clone(),
                token: token.clone(),
                group_id: format!("g{group}"),
                consumer_id: format!("c{consumer}"),
                max_in_flight: args.max_in_flight,
                stop_after_batches: None,
                expect_records: None,
                idle_after_join: None,
                leave_after_join: false,
                duplicate_first_ack: false,
                ack_delay: Duration::from_millis(args.ack_delay_ms),
                shared_progress: Some(progress.clone()),
                timeout: Duration::from_secs(60 + args.rows / 200),
            };
            tasks.push(tokio::spawn(async move { ConsumerClient::run(opts).await }));
        }
    }
    let mut ids = HashSet::new();
    let mut deliveries = 0u64;
    let mut batches = 0usize;
    let mut ack_latency_us = Vec::new();
    for task in tasks {
        let report = task
            .await
            .map_err(|err| err.to_string())?
            .map_err(|err| err.to_string())?;
        deliveries += report.record_ids.len() as u64;
        batches += report.batches;
        ids.extend(report.record_ids);
        ack_latency_us.extend(report.ack_latency_us);
    }
    let elapsed = started.elapsed();
    server.abort();

    let expected = args.rows * args.groups as u64;
    if deliveries < expected || ids.len() as u64 != args.rows {
        return Err(format!(
            "expected at least {expected} deliveries of {} distinct ids, got {deliveries} deliveries and {} ids",
            args.rows,
            ids.len()
        ));
    }
    ack_latency_us.sort_unstable();
    let seconds = elapsed.as_secs_f64().max(0.000_001);
    let bytes = deliveries * args.payload_bytes as u64;
    let mib = (bytes as f64 / seconds) / (1024.0 * 1024.0);
    let row = serde_json::json!({
        "bench": "stage-07",
        "label": args.label,
        "rows": args.rows,
        "payload_bytes": args.payload_bytes,
        "batch_max_records": args.batch_max_records,
        "consumers": args.consumers,
        "groups": args.groups,
        "ack_delay_ms": args.ack_delay_ms,
        "max_buffer_records": args.max_buffer_records,
        "max_buffer_bytes": args.max_buffer_bytes,
        "max_in_flight": args.max_in_flight,
        "records": deliveries,
        "distinct_ids": ids.len(),
        "payload_bytes_total": bytes,
        "elapsed_ms": elapsed.as_millis() as u64,
        "records_per_sec": (deliveries as f64 / seconds).round() as u64,
        "mib_per_sec": (mib * 1000.0).round() / 1000.0,
        "ack_latency_us_p50": percentile(&ack_latency_us, 0.50),
        "ack_latency_us_p99": percentile(&ack_latency_us, 0.99),
        "batches": batches,
    });
    if let Some(parent) = args.output.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|err| err.to_string())?;
        }
    }
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&args.output)
        .map_err(|err| err.to_string())?;
    writeln!(file, "{row}").map_err(|err| err.to_string())?;
    println!("{row}");
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

fn bind_addr() -> Result<SocketAddr, String> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|err| err.to_string())?;
    let addr = listener.local_addr().map_err(|err| err.to_string())?;
    drop(listener);
    Ok(addr)
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

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}
