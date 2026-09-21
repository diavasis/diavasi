use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct Metrics {
    pub records: AtomicU64,
    pub bytes: AtomicU64,
    pub batches: AtomicU64,
    pub acks: AtomicU64,
    start: Instant,
    delivery_ns: std::sync::Mutex<Histogram<u64>>,
    ack_rtt_ns: std::sync::Mutex<Histogram<u64>>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            records: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            batches: AtomicU64::new(0),
            acks: AtomicU64::new(0),
            start: Instant::now(),
            delivery_ns: std::sync::Mutex::new(Histogram::new(3).expect("histogram")),
            ack_rtt_ns: std::sync::Mutex::new(Histogram::new(3).expect("histogram")),
        }
    }

    pub fn record_batch(&self, record_count: u64, byte_count: u64, delivery_latency: Duration) {
        self.records.fetch_add(record_count, Ordering::Relaxed);
        self.bytes.fetch_add(byte_count, Ordering::Relaxed);
        self.batches.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut h) = self.delivery_ns.lock() {
            let _ = h.record(delivery_latency.as_nanos() as u64);
        }
    }

    pub fn record_ack(&self, rtt: Duration) {
        self.acks.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut h) = self.ack_rtt_ns.lock() {
            let _ = h.record(rtt.as_nanos() as u64);
        }
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let elapsed = self.start.elapsed().as_secs_f64().max(1e-9);
        let records = self.records.load(Ordering::Relaxed);
        let bytes = self.bytes.load(Ordering::Relaxed);
        let delivery = self.delivery_ns.lock().ok();
        let ack = self.ack_rtt_ns.lock().ok();
        MetricsSnapshot {
            elapsed_secs: elapsed,
            records,
            bytes,
            batches: self.batches.load(Ordering::Relaxed),
            acks: self.acks.load(Ordering::Relaxed),
            records_per_sec: records as f64 / elapsed,
            mib_per_sec: (bytes as f64 / (1024.0 * 1024.0)) / elapsed,
            delivery_p50_us: percentile_us(delivery.as_deref(), 50.0),
            delivery_p95_us: percentile_us(delivery.as_deref(), 95.0),
            delivery_p99_us: percentile_us(delivery.as_deref(), 99.0),
            ack_rtt_p50_us: percentile_us(ack.as_deref(), 50.0),
            ack_rtt_p95_us: percentile_us(ack.as_deref(), 95.0),
            ack_rtt_p99_us: percentile_us(ack.as_deref(), 99.0),
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

fn percentile_us(hist: Option<&Histogram<u64>>, q: f64) -> f64 {
    hist.map(|h| {
        if h.is_empty() {
            0.0
        } else {
            h.value_at_quantile(q / 100.0) as f64 / 1000.0
        }
    })
    .unwrap_or(0.0)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub elapsed_secs: f64,
    pub records: u64,
    pub bytes: u64,
    pub batches: u64,
    pub acks: u64,
    pub records_per_sec: f64,
    pub mib_per_sec: f64,
    pub delivery_p50_us: f64,
    pub delivery_p95_us: f64,
    pub delivery_p99_us: f64,
    pub ack_rtt_p50_us: f64,
    pub ack_rtt_p95_us: f64,
    pub ack_rtt_p99_us: f64,
}
