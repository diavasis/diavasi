use std::pin::Pin;
use std::sync::Arc;

use futures::{Stream, StreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming, transport::Server};
use tracing::{info, warn};

use crate::bench_protocol::pb::bench_stream_server::{BenchStream, BenchStreamServer};
use crate::bench_protocol::{Envelope, envelope};

use super::config::{BenchConfig, Role};
use super::metrics::Metrics;
use super::report::BenchResult;
use super::workload::{
    SessionState, client_ack_delay, handle_client_message, next_outbound_batch, observe_delivery,
};

pub async fn run(cfg: BenchConfig) -> anyhow::Result<BenchResult> {
    match cfg.role {
        Role::Server => {
            run_server(cfg).await?;
            anyhow::bail!("server role exited")
        }
        Role::Client => run_client(cfg).await,
        Role::Both => {
            let mut server_cfg = cfg.clone();
            server_cfg.role = Role::Server;
            let mut client_cfg = cfg.clone();
            client_cfg.role = Role::Client;
            let listen = server_cfg.listen.clone();
            let server = tokio::spawn(async move { run_server(server_cfg).await });
            for _ in 0..100 {
                if tokio::net::TcpStream::connect(&listen).await.is_ok() {
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

struct BenchSvc {
    state: Arc<SessionState>,
}

#[tonic::async_trait]
impl BenchStream for BenchSvc {
    type ConsumeStream = Pin<Box<dyn Stream<Item = Result<Envelope, Status>> + Send>>;

    async fn consume(
        &self,
        request: Request<Streaming<Envelope>>,
    ) -> Result<Response<Self::ConsumeStream>, Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel::<Result<Envelope, Status>>(64);
        let state = Arc::clone(&self.state);

        tokio::spawn(async move {
            let first = match inbound.next().await {
                Some(Ok(env)) => env,
                Some(Err(e)) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
                None => return,
            };
            match handle_client_message(&state, first).await {
                Ok(Some(reply)) => {
                    if tx.send(Ok(reply)).await.is_err() {
                        return;
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                    return;
                }
            }

            loop {
                tokio::select! {
                    msg = inbound.next() => {
                        match msg {
                            Some(Ok(env)) => {
                                match handle_client_message(&state, env).await {
                                    Ok(Some(reply)) => {
                                        if tx.send(Ok(reply)).await.is_err() {
                                            return;
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(e) => {
                                        let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                                        return;
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                let _ = tx.send(Err(e)).await;
                                return;
                            }
                            None => return,
                        }
                    }
                    batch = next_outbound_batch(&state), if !state.producer.done() => {
                        if let Some(batch) = batch {
                            if tx.send(Ok(Envelope::record_batch(batch))).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

async fn run_server(cfg: BenchConfig) -> anyhow::Result<()> {
    let addr = cfg.listen.parse()?;
    let state = Arc::new(SessionState::new(cfg));
    let svc = BenchSvc { state };
    info!(%addr, "grpc bench server listening");
    Server::builder()
        .add_service(BenchStreamServer::new(svc))
        .serve(addr)
        .await?;
    Ok(())
}

async fn run_client(cfg: BenchConfig) -> anyhow::Result<BenchResult> {
    use crate::bench_protocol::pb::bench_stream_client::BenchStreamClient;

    let endpoint = format!("http://{}", cfg.connect);
    let mut client = BenchStreamClient::connect(endpoint).await?;
    let metrics = Arc::new(Metrics::new());
    let consumer_id = format!("rust-{}", std::process::id());

    let (tx, rx) = mpsc::channel::<Envelope>(64);
    tx.send(Envelope::flow_control(cfg.max_in_flight)).await?;
    tx.send(Envelope::join_group(&cfg.group_id, &consumer_id))
        .await?;

    let outbound = ReceiverStream::new(rx);
    let response = client.consume(Request::new(outbound)).await?;
    let mut inbound = response.into_inner();

    let mut joined = false;
    let target = cfg.total_records;
    while metrics.records.load(std::sync::atomic::Ordering::Relaxed) < target {
        match inbound.next().await {
            Some(Ok(env)) => match env.body {
                Some(envelope::Body::Joined(_)) => joined = true,
                Some(envelope::Body::RecordBatch(batch)) => {
                    observe_delivery(&metrics, &batch);
                    client_ack_delay(&cfg).await;
                    if tx.send(Envelope::ack(batch.batch_id)).await.is_err() {
                        break;
                    }
                }
                Some(envelope::Body::Error(e)) => anyhow::bail!("server error: {}", e.message),
                _ => {}
            },
            Some(Err(e)) => return Err(e.into()),
            None => break,
        }
    }

    let mut notes = vec!["transport=grpc".into()];
    if !joined {
        notes.push("warning: never received Joined".into());
        warn!("never received Joined");
    }
    Ok(BenchResult::from_config(&cfg, metrics.snapshot(), notes))
}
