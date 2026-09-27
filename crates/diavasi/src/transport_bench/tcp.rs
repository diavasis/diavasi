use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use tracing::{info, warn};

use crate::bench_protocol::{Envelope, envelope};

use super::codec::EnvelopeCodec;
use super::config::{BenchConfig, Role};
use super::metrics::Metrics;
use super::report::BenchResult;
use super::workload::{
    SessionState, client_ack_delay, handle_client_message, next_outbound_batch, observe_delivery,
};

pub async fn run(cfg: BenchConfig) -> anyhow::Result<BenchResult> {
    match cfg.role {
        Role::Server => {
            run_server(cfg.clone()).await?;
            anyhow::bail!("server role exited")
        }
        Role::Client => run_client(cfg).await,
        Role::Both => {
            let server_cfg = {
                let mut c = cfg.clone();
                c.role = Role::Server;
                c
            };
            let client_cfg = {
                let mut c = cfg.clone();
                c.role = Role::Client;
                c
            };
            let listen = server_cfg.listen.clone();
            let server = tokio::spawn(async move { run_server(server_cfg).await });
            // Wait until the port accepts connections.
            for _ in 0..100 {
                if TcpStream::connect(&listen).await.is_ok() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            let result = run_client(client_cfg).await;
            server.abort();
            result
        }
    }
}

async fn run_server(cfg: BenchConfig) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&cfg.listen).await?;
    info!(addr = %cfg.listen, "tcp bench server listening");
    let state = Arc::new(SessionState::new(cfg));
    loop {
        let (stream, peer) = listener.accept().await?;
        info!(%peer, "tcp client connected");
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, state).await {
                warn!(error = %e, "tcp connection ended");
            }
        });
    }
}

async fn handle_connection(stream: TcpStream, state: Arc<SessionState>) -> anyhow::Result<()> {
    let mut framed = Framed::new(stream, EnvelopeCodec);
    // Wait for join.
    let Some(first) = framed.next().await else {
        return Ok(());
    };
    let first = first?;
    if let Some(reply) = handle_client_message(&state, first).await? {
        framed.send(reply).await?;
    }

    loop {
        tokio::select! {
            msg = framed.next() => {
                match msg {
                    Some(Ok(env)) => {
                        if let Some(reply) = handle_client_message(&state, env).await? {
                            framed.send(reply).await?;
                        }
                    }
                    Some(Err(e)) => return Err(e.into()),
                    None => return Ok(()),
                }
            }
            batch = next_outbound_batch(&state), if !state.producer.done() => {
                if let Some(batch) = batch {
                    framed.send(Envelope::record_batch(batch)).await?;
                } else if state.producer.done() {
                    // Keep serving ACKs until client disconnects.
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
        }
    }
}

async fn run_client(cfg: BenchConfig) -> anyhow::Result<BenchResult> {
    let stream = TcpStream::connect(&cfg.connect).await?;
    let mut framed = Framed::new(stream, EnvelopeCodec);
    let metrics = Arc::new(Metrics::new());
    let consumer_id = format!("rust-{}", std::process::id());

    framed
        .send(Envelope::flow_control(cfg.max_in_flight))
        .await?;
    framed
        .send(Envelope::join_group(&cfg.group_id, &consumer_id))
        .await?;

    let mut joined = false;
    let target = cfg.total_records;

    while metrics.records.load(std::sync::atomic::Ordering::Relaxed) < target {
        let Some(msg) = framed.next().await else {
            break;
        };
        let env = msg?;
        match env.body {
            Some(envelope::Body::Joined(_)) => {
                joined = true;
            }
            Some(envelope::Body::RecordBatch(batch)) => {
                observe_delivery(&metrics, &batch);
                client_ack_delay(&cfg).await;
                framed.send(Envelope::ack(batch.batch_id)).await?;
            }
            Some(envelope::Body::Error(e)) => anyhow::bail!("server error: {}", e.message),
            _ => {}
        }
    }

    let mut notes = vec!["transport=tcp".into()];
    if !joined {
        notes.push("warning: never received Joined".into());
    }
    Ok(BenchResult::from_config(&cfg, metrics.snapshot(), notes))
}
