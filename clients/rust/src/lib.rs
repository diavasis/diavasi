//! Thin client for `diavasi.data.v1`.
//!
//! The caller acks by `batch_id`. This crate does not store a cursor and does
//! not dedupe on `record_id`.

use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::{Code, Request, Status};

pub mod pb {
    tonic::include_proto!("diavasi.data.v1");
}

use pb::data_plane_client::DataPlaneClient;
use pb::envelope::Body;
use pb::{Ack, Envelope, FlowControl, Hello, JoinGroup, Leave};

#[derive(Debug)]
pub struct Record {
    pub record_id: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug)]
pub struct Batch {
    pub batch_id: u64,
    pub records: Vec<Record>,
}

#[derive(Debug)]
pub struct Report {
    pub record_ids: Vec<u64>,
    pub batch_ids: Vec<u64>,
}

#[derive(Debug)]
pub enum ClientError {
    Protocol { code: u32, message: String },
    Call { status: String, message: String },
    Transport(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Protocol { code, message } => write!(f, "protocol error {code}: {message}"),
            Self::Call { status, message } => write!(f, "grpc {status}: {message}"),
            Self::Transport(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for ClientError {}

pub struct Options {
    pub addr: String,
    pub ca_pem: Vec<u8>,
    pub token: String,
    pub group_id: String,
    pub consumer_id: String,
    pub max_in_flight: u32,
    /// Stop after this many acks without Leave.
    pub halt_after_acks: Option<u32>,
    /// Leave once this many records have been acked. `None` reads until the stream ends.
    pub expect_records: Option<u64>,
}

impl Options {
    pub fn new(
        addr: impl Into<String>,
        ca_pem: Vec<u8>,
        token: impl Into<String>,
        group_id: impl Into<String>,
        consumer_id: impl Into<String>,
    ) -> Self {
        Self {
            addr: addr.into(),
            ca_pem,
            token: token.into(),
            group_id: group_id.into(),
            consumer_id: consumer_id.into(),
            max_in_flight: 1,
            halt_after_acks: None,
            expect_records: None,
        }
    }
}

pub fn run(opts: Options) -> Result<Report, ClientError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| ClientError::Transport(err.to_string()))?;
    runtime.block_on(run_async(opts))
}

pub async fn run_async(opts: Options) -> Result<Report, ClientError> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(&opts.ca_pem))
        .domain_name("localhost");
    let channel = Channel::from_shared(format!("https://{}", opts.addr))
        .map_err(|err| ClientError::Transport(err.to_string()))?
        .tls_config(tls)
        .map_err(|err| ClientError::Transport(err.to_string()))?
        .connect()
        .await
        .map_err(status_or_transport)?;
    let mut client = DataPlaneClient::new(channel);
    let (tx, rx) = mpsc::channel(16);
    let mut request = Request::new(ReceiverStream::new(rx));
    let bearer = format!("Bearer {}", opts.token);
    let mut header = MetadataValue::try_from(bearer.as_str())
        .map_err(|err| ClientError::Transport(err.to_string()))?;
    header.set_sensitive(true);
    request.metadata_mut().insert("authorization", header);
    let response = client.consume(request).await.map_err(from_status)?;
    tx.send(hello())
        .await
        .map_err(|err| ClientError::Transport(err.to_string()))?;
    let mut inbound = response.into_inner();
    let mut sent_flow = false;
    let mut record_ids = Vec::new();
    let mut batch_ids = Vec::new();

    while let Some(message) = inbound.message().await.map_err(from_status)? {
        match message.body {
            Some(Body::HelloAck(_)) => {
                tx.send(join(&opts.group_id, &opts.consumer_id))
                    .await
                    .map_err(|err| ClientError::Transport(err.to_string()))?;
            }
            Some(Body::Joined(_)) => {
                if !sent_flow {
                    sent_flow = true;
                    tx.send(flow(opts.max_in_flight))
                        .await
                        .map_err(|err| ClientError::Transport(err.to_string()))?;
                }
            }
            Some(Body::RecordBatch(batch)) => {
                for record in &batch.records {
                    record_ids.push(record.record_id);
                }
                tx.send(ack(batch.batch_id))
                    .await
                    .map_err(|err| ClientError::Transport(err.to_string()))?;
                batch_ids.push(batch.batch_id);
                if opts
                    .halt_after_acks
                    .is_some_and(|limit| batch_ids.len() as u32 >= limit)
                {
                    let _ =
                        tokio::time::timeout(Duration::from_secs(5), inbound.message()).await;
                    drop(tx);
                    return Ok(Report {
                        record_ids,
                        batch_ids,
                    });
                }
                if opts
                    .expect_records
                    .is_some_and(|total| record_ids.len() as u64 >= total)
                {
                    tx.send(leave_frame())
                        .await
                        .map_err(|err| ClientError::Transport(err.to_string()))?;
                    let _ = tokio::time::timeout(Duration::from_secs(2), drain(&mut inbound)).await;
                    return Ok(Report {
                        record_ids,
                        batch_ids,
                    });
                }
            }
            Some(Body::Heartbeat(_)) => {
                tx.send(heartbeat())
                    .await
                    .map_err(|err| ClientError::Transport(err.to_string()))?;
            }
            Some(Body::Error(err)) => {
                return Err(ClientError::Protocol {
                    code: err.code,
                    message: err.message,
                });
            }
            Some(_) | None => {}
        }
    }
    if let Some(total) = opts.expect_records {
        if (record_ids.len() as u64) < total {
            return Err(ClientError::Transport(format!(
                "incomplete consume records={} batches={}",
                record_ids.len(),
                batch_ids.len()
            )));
        }
    }
    Ok(Report {
        record_ids,
        batch_ids,
    })
}

async fn drain(inbound: &mut tonic::Streaming<Envelope>) {
    while let Ok(Some(_)) = inbound.message().await {}
}

fn from_status(status: Status) -> ClientError {
    ClientError::Call {
        status: code_name(status.code()).to_string(),
        message: status.message().to_string(),
    }
}

fn status_or_transport(err: tonic::transport::Error) -> ClientError {
    ClientError::Transport(err.to_string())
}

fn code_name(code: Code) -> &'static str {
    match code {
        Code::Unauthenticated => "UNAUTHENTICATED",
        Code::Unavailable => "UNAVAILABLE",
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::Internal => "INTERNAL",
        Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        Code::Cancelled => "CANCELLED",
        _ => "UNKNOWN",
    }
}

fn envelope(body: Body) -> Envelope {
    Envelope {
        version: 1,
        body: Some(body),
    }
}

fn hello() -> Envelope {
    envelope(Body::Hello(Hello {
        protocol_version: 1,
    }))
}

fn join(group_id: &str, consumer_id: &str) -> Envelope {
    envelope(Body::JoinGroup(JoinGroup {
        group_id: group_id.to_string(),
        consumer_id: consumer_id.to_string(),
    }))
}

fn flow(max_in_flight: u32) -> Envelope {
    envelope(Body::FlowControl(FlowControl { max_in_flight }))
}

fn ack(batch_id: u64) -> Envelope {
    envelope(Body::Ack(Ack { batch_id }))
}

fn heartbeat() -> Envelope {
    envelope(Body::Heartbeat(pb::Heartbeat {}))
}

fn leave_frame() -> Envelope {
    envelope(Body::Leave(Leave {}))
}
