use std::net::SocketAddr;
use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use tokio_util::codec::Framed;
use tracing::{info, warn};
use wtransport::tls::{Identity, Sha256Digest};
use wtransport::{ClientConfig, Endpoint, ServerConfig};

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
            let identity = Identity::self_signed(["localhost", "127.0.0.1", "::1"])?;
            run_server(cfg, identity).await?;
            anyhow::bail!("server role exited")
        }
        Role::Client => {
            // Standalone clients skip cert validation (bench only).
            run_client(cfg, None).await
        }
        Role::Both => {
            let identity = Identity::self_signed(["localhost", "127.0.0.1", "::1"])?;
            let hashes = leaf_hashes(&identity);
            let mut server_cfg = cfg.clone();
            server_cfg.role = Role::Server;
            let mut client_cfg = cfg.clone();
            client_cfg.role = Role::Client;
            let server_identity = identity.clone_identity();
            let server = tokio::spawn(async move { run_server(server_cfg, server_identity).await });
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            let result = run_client(client_cfg, Some(hashes)).await;
            server.abort();
            result
        }
    }
}

fn leaf_hashes(identity: &Identity) -> Vec<Sha256Digest> {
    identity
        .certificate_chain()
        .as_slice()
        .iter()
        .map(|c| c.hash())
        .collect()
}

async fn run_server(cfg: BenchConfig, identity: Identity) -> anyhow::Result<()> {
    let addr: SocketAddr = cfg.listen.parse()?;
    let server_config = ServerConfig::builder()
        .with_bind_address(addr)
        .with_identity(identity)
        .build();
    let endpoint = Endpoint::server(server_config)?;
    info!(%addr, "webtransport bench server listening");
    let state = Arc::new(SessionState::new(cfg));

    loop {
        let incoming = endpoint.accept().await;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(e) = handle_incoming(incoming, state).await {
                warn!(error = %e, "webtransport connection ended");
            }
        });
    }
}

async fn handle_incoming(
    incoming: wtransport::endpoint::IncomingSession,
    state: Arc<SessionState>,
) -> anyhow::Result<()> {
    let request = incoming.await?;
    info!(
        authority = %request.authority(),
        path = %request.path(),
        "webtransport session request"
    );
    let connection = request.accept().await?;
    let (send, recv) = connection.accept_bi().await?;
    let stream = tokio::io::join(recv, send);
    let mut framed = Framed::new(stream, EnvelopeCodec);

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
                }
            }
        }
    }
}

async fn run_client(
    cfg: BenchConfig,
    server_hashes: Option<Vec<Sha256Digest>>,
) -> anyhow::Result<BenchResult> {
    let builder = ClientConfig::builder().with_bind_default();
    let client_config = match server_hashes {
        Some(hashes) => builder.with_server_certificate_hashes(hashes).build(),
        None => builder.with_no_cert_validation().build(),
    };
    let endpoint = Endpoint::client(client_config)?;
    let url = format!("https://{}/", cfg.connect);
    let connection = endpoint.connect(url).await?;
    let (send, recv) = connection.open_bi().await?.await?;
    let stream = tokio::io::join(recv, send);
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
            Some(envelope::Body::Joined(_)) => joined = true,
            Some(envelope::Body::RecordBatch(batch)) => {
                observe_delivery(&metrics, &batch);
                client_ack_delay(&cfg).await;
                framed.send(Envelope::ack(batch.batch_id)).await?;
            }
            Some(envelope::Body::Error(e)) => anyhow::bail!("server error: {}", e.message),
            _ => {}
        }
    }

    let mut notes = vec![
        "transport=webtransport".into(),
        "library=wtransport".into(),
        "spec=draft; library not production-ready".into(),
    ];
    if !joined {
        notes.push("warning: never received Joined".into());
    }
    Ok(BenchResult::from_config(&cfg, metrics.snapshot(), notes))
}
