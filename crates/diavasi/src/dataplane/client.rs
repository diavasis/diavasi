use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};

use super::pb::data_plane_client::DataPlaneClient;
use super::pb::envelope::Body;
use super::{Envelope, PROTOCOL_VERSION, ack, flow_control, hello, join_group, leave};

pub struct ConsumerOptions {
    pub addr: String,
    pub ca_pem: Vec<u8>,
    pub token: String,
    pub group_id: String,
    pub consumer_id: String,
    pub max_in_flight: u32,
    /// Stop after this many batches without acking or leaving (abrupt disconnect).
    pub stop_after_batches: Option<usize>,
    /// When set, finish once this many distinct record ids have been acked.
    pub expect_records: Option<u64>,
    /// Stay connected after join without further client frames.
    pub idle_after_join: Option<Duration>,
    /// Send Leave immediately after Joined and wait for the server to close.
    pub leave_after_join: bool,
    /// Ack the first batch twice so the server rejects the duplicate.
    pub duplicate_first_ack: bool,
    /// Sleep this long after a batch arrives and before the ack is sent.
    pub ack_delay: Duration,
    /// When set, every client sharing this counter leaves once `target` records are acked.
    pub shared_progress: Option<SharedProgress>,
    pub timeout: Duration,
}

/// Ack counter shared by consumers of one group.
#[derive(Clone)]
pub struct SharedProgress {
    pub acked: Arc<AtomicU64>,
    pub target: u64,
}

#[derive(Debug)]
pub struct ConsumeReport {
    pub record_ids: Vec<u64>,
    pub batches: usize,
    pub acked: usize,
    /// Time from batch receipt through the optional ack delay until the ack is queued.
    pub ack_latency_us: Vec<u64>,
}

pub struct ConsumerClient;

impl ConsumerClient {
    pub async fn run(
        opts: ConsumerOptions,
    ) -> Result<ConsumeReport, Box<dyn std::error::Error + Send + Sync>> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&opts.ca_pem))
            .domain_name("localhost");
        let channel = Channel::from_shared(format!("https://{}", opts.addr))?
            .tls_config(tls)?
            .connect()
            .await?;
        let mut client = DataPlaneClient::new(channel);

        let (tx, rx) = mpsc::channel::<Envelope>(16);
        tx.send(hello(PROTOCOL_VERSION)).await?;
        let mut request = Request::new(ReceiverStream::new(rx));
        let token = format!("Bearer {}", opts.token);
        let mut header = MetadataValue::try_from(token.as_str())?;
        header.set_sensitive(true);
        request.metadata_mut().insert("authorization", header);
        let response = client.consume(request).await?;
        let mut inbound = response.into_inner();

        let mut record_ids = Vec::new();
        let mut seen = HashSet::new();
        let mut batches = 0usize;
        let mut acked = 0usize;
        let mut ack_latency_us = Vec::new();
        let mut sent_flow = false;
        let deadline = tokio::time::Instant::now() + opts.timeout;

        while tokio::time::Instant::now() < deadline {
            if let Some(expect) = opts.expect_records {
                if seen.len() as u64 >= expect {
                    let _ = tx.send(leave()).await;
                    break;
                }
            }
            if let Some(shared) = &opts.shared_progress {
                if shared.acked.load(Ordering::Relaxed) >= shared.target {
                    let _ = tx.send(leave()).await;
                    break;
                }
            }
            let env = match tokio::time::timeout_at(deadline, inbound.next()).await {
                Ok(Some(Ok(env))) => env,
                Ok(Some(Err(e))) => return Err(e.into()),
                Ok(None) => break,
                Err(_) => return Err("data plane client timed out".into()),
            };
            match env.body {
                Some(Body::HelloAck(_)) => {
                    tx.send(join_group(&opts.group_id, &opts.consumer_id))
                        .await?;
                }
                Some(Body::Joined(_)) => {
                    if opts.leave_after_join {
                        tx.send(leave()).await?;
                        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
                        while let Ok(Some(Ok(_))) =
                            tokio::time::timeout_at(deadline, inbound.next()).await
                        {
                        }
                        break;
                    }
                    if let Some(idle) = opts.idle_after_join {
                        tokio::time::sleep(idle).await;
                        break;
                    }
                    if !sent_flow {
                        tx.send(flow_control(opts.max_in_flight)).await?;
                        sent_flow = true;
                    }
                }
                Some(Body::RecordBatch(batch)) => {
                    batches += 1;
                    for record in &batch.records {
                        record_ids.push(record.record_id);
                        seen.insert(record.record_id);
                    }
                    if opts.stop_after_batches == Some(batches) {
                        drop(tx);
                        break;
                    }
                    let ack_started = tokio::time::Instant::now();
                    if !opts.ack_delay.is_zero() {
                        tokio::time::sleep(opts.ack_delay).await;
                    }
                    tx.send(ack(batch.batch_id)).await?;
                    ack_latency_us.push(ack_started.elapsed().as_micros() as u64);
                    acked += 1;
                    if let Some(shared) = &opts.shared_progress {
                        let now = shared
                            .acked
                            .fetch_add(batch.records.len() as u64, Ordering::Relaxed)
                            + batch.records.len() as u64;
                        if now >= shared.target {
                            let _ = tx.send(leave()).await;
                            break;
                        }
                    }
                    if opts.duplicate_first_ack && acked == 1 {
                        tx.send(ack(batch.batch_id)).await?;
                    }
                }
                Some(Body::Heartbeat(_)) => {}
                Some(Body::Error(err)) => {
                    return Err(format!("protocol error {}: {}", err.code, err.message).into());
                }
                other => {
                    return Err(format!("unexpected frame: {other:?}").into());
                }
            }
        }
        Ok(ConsumeReport {
            record_ids,
            batches,
            acked,
            ack_latency_us,
        })
    }
}
