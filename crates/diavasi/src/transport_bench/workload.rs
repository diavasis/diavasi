use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::Semaphore;
use tracing::warn;

use crate::bench_protocol::{Envelope, RecordBatch, envelope};

use super::config::BenchConfig;
use super::metrics::Metrics;
use super::producer::SyntheticProducer;

pub struct SessionState {
    pub producer: Arc<SyntheticProducer>,
    pub metrics: Arc<Metrics>,
    pub in_flight: Arc<Semaphore>,
    pub cfg: BenchConfig,
}

impl SessionState {
    pub fn new(cfg: BenchConfig) -> Self {
        let producer = Arc::new(SyntheticProducer::new(
            cfg.record_size.bytes(),
            cfg.batch_size,
            cfg.total_records,
        ));
        Self {
            producer,
            metrics: Arc::new(Metrics::new()),
            in_flight: Arc::new(Semaphore::new(cfg.max_in_flight as usize)),
            cfg,
        }
    }
}

pub async fn handle_client_message(
    state: &SessionState,
    env: Envelope,
) -> anyhow::Result<Option<Envelope>> {
    match env.body {
        Some(envelope::Body::JoinGroup(j)) => Ok(Some(Envelope::joined(j.group_id, j.consumer_id))),
        Some(envelope::Body::Ack(ack)) => {
            state.in_flight.add_permits(1);
            state.metrics.record_ack(Duration::from_micros(1));
            let _ = ack.batch_id;
            Ok(None)
        }
        Some(envelope::Body::FlowControl(fc)) => {
            let _ = fc;
            Ok(None)
        }
        Some(envelope::Body::Heartbeat(_)) => Ok(Some(Envelope::heartbeat())),
        other => {
            warn!(?other, "unexpected client message");
            Ok(Some(Envelope::error("unexpected message")))
        }
    }
}

pub async fn next_outbound_batch(state: &SessionState) -> Option<RecordBatch> {
    let permit = state.in_flight.acquire().await.ok()?;
    permit.forget();
    match state.producer.next_batch() {
        Some(batch) => Some(batch),
        None => {
            state.in_flight.add_permits(1);
            None
        }
    }
}

pub fn observe_delivery(metrics: &Metrics, batch: &RecordBatch) {
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);
    let latency = if batch.sent_at_unix_ns > 0 && now_ns >= batch.sent_at_unix_ns {
        Duration::from_nanos((now_ns - batch.sent_at_unix_ns) as u64)
    } else {
        Duration::from_micros(0)
    };
    let bytes: u64 = batch.records.iter().map(|r| r.payload.len() as u64).sum();
    metrics.record_batch(batch.records.len() as u64, bytes, latency);
}

pub async fn client_ack_delay(cfg: &BenchConfig) {
    if cfg.ack_delay_ms > 0 {
        tokio::time::sleep(cfg.ack_delay()).await;
    }
}

pub fn deadline(cfg: &BenchConfig) -> Option<Instant> {
    if cfg.duration_secs > 0 {
        Some(Instant::now() + Duration::from_secs(cfg.duration_secs))
    } else {
        None
    }
}
