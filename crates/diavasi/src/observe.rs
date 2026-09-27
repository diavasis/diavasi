//! Process-owned Prometheus registry.
//!
//! Counters move when a fetch, delivery, ack, replay, restart, disconnect, or
//! adapter error happens. Buffer and in-flight gauges are filled at scrape time
//! from live group handles, so a stopped group does not keep a stale series.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
    TextEncoder,
};

const LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

/// One running group's gauge values, collected when `/metrics` is scraped.
#[derive(Debug, Clone)]
pub struct GaugeSample {
    pub group_id: String,
    pub buffer_records: u64,
    pub buffer_bytes: u64,
    pub inflight_records: u64,
    pub checkpoint_lag: u64,
    pub consumer_count: u64,
}

/// Counter values for one group, for the diagnostics view.
#[derive(Debug, Clone, Copy, Default)]
pub struct CounterSnapshot {
    pub records_fetched: u64,
    pub records_delivered: u64,
    pub records_acked: u64,
    pub records_replayed: u64,
    pub bytes: u64,
    pub restarts: u64,
    pub consumer_disconnects: u64,
    pub adapter_errors: u64,
}

#[derive(Clone)]
pub struct Observe {
    inner: Arc<Inner>,
}

struct Inner {
    registry: Registry,
    groups_running: IntGauge,
    fetched: IntCounterVec,
    delivered: IntCounterVec,
    acked: IntCounterVec,
    replayed: IntCounterVec,
    bytes: IntCounterVec,
    buffer_records: IntGaugeVec,
    buffer_bytes: IntGaugeVec,
    inflight_records: IntGaugeVec,
    checkpoint_lag: IntGaugeVec,
    consumer_count: IntGaugeVec,
    fetch_latency: HistogramVec,
    ack_latency: HistogramVec,
    checkpoint_latency: HistogramVec,
    restarts: IntCounterVec,
    recovery_failures: IntCounterVec,
    stale_acks: IntCounterVec,
    disconnects: IntCounterVec,
    adapter_errors: IntCounterVec,
    scrape: Mutex<()>,
}

impl Observe {
    pub fn new() -> Self {
        let registry = Registry::new();
        let groups_running = gauge("diavasi_groups_running", "Group runtimes currently running");
        let fetched = counter_vec(
            "diavasi_group_records_fetched_total",
            "Records read from the source into a group",
            &["group_id", "adapter"],
        );
        let delivered = counter_vec(
            "diavasi_group_records_delivered_total",
            "Records assigned to a consumer",
            &["group_id", "adapter"],
        );
        let acked = counter_vec(
            "diavasi_group_records_acked_total",
            "Records acknowledged by a consumer",
            &["group_id", "adapter"],
        );
        let replayed = counter_vec(
            "diavasi_group_records_replayed_total",
            "Records returned to the buffer for another delivery",
            &["group_id", "adapter"],
        );
        let bytes = counter_vec(
            "diavasi_group_bytes_total",
            "Payload bytes read from the source into a group",
            &["group_id", "adapter"],
        );
        let buffer_records = gauge_vec(
            "diavasi_group_buffer_records",
            "Records waiting in the group buffer",
            &["group_id"],
        );
        let buffer_bytes = gauge_vec(
            "diavasi_group_buffer_bytes",
            "Payload bytes waiting in the group buffer",
            &["group_id"],
        );
        let inflight_records = gauge_vec(
            "diavasi_group_inflight_records",
            "Records assigned and not yet acknowledged",
            &["group_id"],
        );
        let checkpoint_lag = gauge_vec(
            "diavasi_group_checkpoint_lag",
            "Records fetched and not yet committed (buffer plus in flight)",
            &["group_id"],
        );
        let consumer_count = gauge_vec(
            "diavasi_consumer_count",
            "Consumers joined to the group",
            &["group_id"],
        );
        let fetch_latency = histogram_vec(
            "diavasi_group_fetch_latency_seconds",
            "Time to read one fetch from the source",
            &["group_id", "adapter"],
        );
        let ack_latency = histogram_vec(
            "diavasi_group_ack_latency_seconds",
            "Time to apply an acknowledgement",
            &["group_id", "adapter"],
        );
        let checkpoint_latency = histogram_vec(
            "diavasi_checkpoint_write_latency_seconds",
            "Time to persist a committed cursor",
            &["group_id"],
        );
        let restarts = counter_vec(
            "diavasi_group_restarts_total",
            "Unexpected group-task exits that the supervisor respawned",
            &["group_id"],
        );
        let recovery_failures = counter_vec(
            "diavasi_group_recovery_failures_total",
            "Restarts of a failed group that could not open its source",
            &["group_id"],
        );
        let stale_acks = counter_vec(
            "diavasi_group_stale_acks_total",
            "Acks for batches no longer in flight, usually timed out and redelivered",
            &["group_id"],
        );
        let disconnects = counter_vec(
            "diavasi_consumer_disconnects_total",
            "Consumer sessions that left or dropped",
            &["group_id"],
        );
        let adapter_errors = counter_vec(
            "diavasi_adapter_errors_total",
            "Source reads that failed",
            &["group_id", "adapter"],
        );

        for metric in [
            groups_running.clone().boxed(),
            fetched.clone().boxed(),
            delivered.clone().boxed(),
            acked.clone().boxed(),
            replayed.clone().boxed(),
            bytes.clone().boxed(),
            buffer_records.clone().boxed(),
            buffer_bytes.clone().boxed(),
            inflight_records.clone().boxed(),
            checkpoint_lag.clone().boxed(),
            consumer_count.clone().boxed(),
            fetch_latency.clone().boxed(),
            ack_latency.clone().boxed(),
            checkpoint_latency.clone().boxed(),
            restarts.clone().boxed(),
            recovery_failures.clone().boxed(),
            stale_acks.clone().boxed(),
            disconnects.clone().boxed(),
            adapter_errors.clone().boxed(),
        ] {
            registry.register(metric).expect("metric names are unique");
        }

        Self {
            inner: Arc::new(Inner {
                registry,
                groups_running,
                fetched,
                delivered,
                acked,
                replayed,
                bytes,
                buffer_records,
                buffer_bytes,
                inflight_records,
                checkpoint_lag,
                consumer_count,
                fetch_latency,
                ack_latency,
                checkpoint_latency,
                restarts,
                recovery_failures,
                stale_acks,
                disconnects,
                adapter_errors,
                scrape: Mutex::new(()),
            }),
        }
    }

    pub fn record_fetch(
        &self,
        group: &str,
        adapter: &str,
        records: u64,
        bytes: u64,
        latency: Duration,
    ) {
        if records > 0 {
            self.inner
                .fetched
                .with_label_values(&[group, adapter])
                .inc_by(records);
        }
        if bytes > 0 {
            self.inner
                .bytes
                .with_label_values(&[group, adapter])
                .inc_by(bytes);
        }
        self.inner
            .fetch_latency
            .with_label_values(&[group, adapter])
            .observe(latency.as_secs_f64());
    }

    pub fn record_deliver(&self, group: &str, adapter: &str, records: u64) {
        if records > 0 {
            self.inner
                .delivered
                .with_label_values(&[group, adapter])
                .inc_by(records);
        }
    }

    pub fn record_ack(&self, group: &str, adapter: &str, records: u64, latency: Duration) {
        if records > 0 {
            self.inner
                .acked
                .with_label_values(&[group, adapter])
                .inc_by(records);
        }
        self.inner
            .ack_latency
            .with_label_values(&[group, adapter])
            .observe(latency.as_secs_f64());
    }

    pub fn record_replay(&self, group: &str, adapter: &str, records: u64) {
        if records > 0 {
            self.inner
                .replayed
                .with_label_values(&[group, adapter])
                .inc_by(records);
        }
    }

    pub fn record_checkpoint(&self, group: &str, latency: Duration) {
        self.inner
            .checkpoint_latency
            .with_label_values(&[group])
            .observe(latency.as_secs_f64());
    }

    pub fn record_restart(&self, group: &str) {
        self.inner.restarts.with_label_values(&[group]).inc();
    }

    pub fn record_recovery_failure(&self, group: &str) {
        self.inner
            .recovery_failures
            .with_label_values(&[group])
            .inc();
    }

    pub fn record_stale_ack(&self, group: &str) {
        self.inner.stale_acks.with_label_values(&[group]).inc();
    }

    /// Count a consumer session that left or dropped. The consumer id is
    /// chosen by clients, so it is logged, not used as a label.
    pub fn record_disconnect(&self, group: &str) {
        self.inner.disconnects.with_label_values(&[group]).inc();
    }

    pub fn record_adapter_error(&self, group: &str, adapter: &str) {
        self.inner
            .adapter_errors
            .with_label_values(&[group, adapter])
            .inc();
    }

    /// Counter values for one group. Reading does not create series.
    pub fn counters(&self, group: &str, adapter: &str) -> CounterSnapshot {
        let both = [("group_id", group), ("adapter", adapter)];
        let only_group = [("group_id", group)];
        CounterSnapshot {
            records_fetched: counter_value(&self.inner.fetched, &both),
            records_delivered: counter_value(&self.inner.delivered, &both),
            records_acked: counter_value(&self.inner.acked, &both),
            records_replayed: counter_value(&self.inner.replayed, &both),
            bytes: counter_value(&self.inner.bytes, &both),
            restarts: counter_value(&self.inner.restarts, &only_group),
            consumer_disconnects: counter_value(&self.inner.disconnects, &only_group),
            adapter_errors: counter_value(&self.inner.adapter_errors, &both),
        }
    }

    /// Remove every series of a deleted group.
    pub fn forget_group(&self, group: &str, adapter: &str) {
        let inner = &self.inner;
        for vec in [
            &inner.fetched,
            &inner.delivered,
            &inner.acked,
            &inner.replayed,
            &inner.bytes,
            &inner.adapter_errors,
        ] {
            let _ = vec.remove_label_values(&[group, adapter]);
        }
        for vec in [
            &inner.restarts,
            &inner.recovery_failures,
            &inner.stale_acks,
            &inner.disconnects,
        ] {
            let _ = vec.remove_label_values(&[group]);
        }
        for vec in [&inner.fetch_latency, &inner.ack_latency] {
            let _ = vec.remove_label_values(&[group, adapter]);
        }
        let _ = inner.checkpoint_latency.remove_label_values(&[group]);
    }

    /// Replace gauge series with `samples` and encode the registry as Prometheus text.
    pub fn render(&self, running: usize, samples: &[GaugeSample]) -> String {
        let _scrape = self.inner.scrape.lock().expect("metrics scrape");
        self.inner.groups_running.set(running as i64);
        self.inner.buffer_records.reset();
        self.inner.buffer_bytes.reset();
        self.inner.inflight_records.reset();
        self.inner.checkpoint_lag.reset();
        self.inner.consumer_count.reset();
        for sample in samples {
            let group = sample.group_id.as_str();
            self.inner
                .buffer_records
                .with_label_values(&[group])
                .set(sample.buffer_records as i64);
            self.inner
                .buffer_bytes
                .with_label_values(&[group])
                .set(sample.buffer_bytes as i64);
            self.inner
                .inflight_records
                .with_label_values(&[group])
                .set(sample.inflight_records as i64);
            self.inner
                .checkpoint_lag
                .with_label_values(&[group])
                .set(sample.checkpoint_lag as i64);
            self.inner
                .consumer_count
                .with_label_values(&[group])
                .set(sample.consumer_count as i64);
        }
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&self.inner.registry.gather(), &mut buf)
            .expect("prometheus text encode");
        String::from_utf8(buf).expect("prometheus text is utf-8")
    }
}

impl Default for Observe {
    fn default() -> Self {
        Self::new()
    }
}

/// Install the `diavasi serve` subscriber.
///
/// `DIAVASI_LOG_FORMAT=json` selects JSON lines. Any other value, including an
/// unset variable, selects the text formatter. `RUST_LOG` selects the filter
/// and defaults to `info`, which leaves debug spans off.
pub fn init_serve_tracing() {
    let filter =
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    let format = std::env::var("DIAVASI_LOG_FORMAT").unwrap_or_else(|_| "text".into());
    match format.as_str() {
        "json" => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(filter)
                .init();
        }
        "text" => {
            tracing_subscriber::fmt().with_env_filter(filter).init();
        }
        other => {
            eprintln!("DIAVASI_LOG_FORMAT={other} is unknown; using text");
            tracing_subscriber::fmt().with_env_filter(filter).init();
        }
    }
}

fn gauge(name: &str, help: &str) -> IntGauge {
    IntGauge::with_opts(Opts::new(name, help)).expect("gauge")
}

fn counter_vec(name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    IntCounterVec::new(Opts::new(name, help), labels).expect("counter")
}

fn gauge_vec(name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
    IntGaugeVec::new(Opts::new(name, help), labels).expect("gauge vec")
}

fn histogram_vec(name: &str, help: &str, labels: &[&str]) -> HistogramVec {
    HistogramVec::new(
        HistogramOpts::new(name, help).buckets(LATENCY_BUCKETS.to_vec()),
        labels,
    )
    .expect("histogram")
}

/// The value of the series whose labels equal `labels`, or 0. Unlike
/// `get_metric_with_label_values`, this never creates a series.
fn counter_value(counter: &IntCounterVec, labels: &[(&str, &str)]) -> u64 {
    use prometheus::core::Collector;

    counter
        .collect()
        .iter()
        .flat_map(|family| family.get_metric())
        .find(|metric| {
            labels.iter().all(|(name, value)| {
                metric
                    .get_label()
                    .iter()
                    .any(|pair| pair.get_name() == *name && pair.get_value() == *value)
            })
        })
        .map(|metric| metric.get_counter().get_value() as u64)
        .unwrap_or(0)
}

trait BoxedCollector {
    fn boxed(self) -> Box<dyn prometheus::core::Collector>;
}

impl<T> BoxedCollector for T
where
    T: prometheus::core::Collector + 'static,
{
    fn boxed(self) -> Box<dyn prometheus::core::Collector> {
        Box::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// B24: reading counters for diagnostics must not create metric series.
    #[test]
    fn regress_b24_reading_counters_does_not_create_series() {
        let observe = Observe::new();
        let counters = observe.counters("ghost", "synthetic");
        assert_eq!(counters.records_fetched, 0);
        let text = observe.render(0, &[]);
        assert!(!text.contains("ghost"), "{text}");
    }

    /// B24: deleting a group removes its series.
    #[test]
    fn regress_b24_forget_group_removes_its_series() {
        let observe = Observe::new();
        observe.record_fetch("gone", "synthetic", 3, 30, Duration::from_millis(1));
        observe.record_restart("gone");
        assert!(observe.render(0, &[]).contains("gone"));
        observe.forget_group("gone", "synthetic");
        let text = observe.render(0, &[]);
        assert!(!text.contains("gone"), "{text}");
    }

    /// B24: consumer ids are chosen by clients, so they must not become
    /// labels. `record_disconnect` no longer takes one.
    #[test]
    fn regress_b24_disconnects_do_not_label_consumer_ids() {
        let observe = Observe::new();
        observe.record_disconnect("g1");
        let text = observe.render(0, &[]);
        assert!(!text.contains("consumer_id"), "{text}");
        assert_eq!(observe.counters("g1", "synthetic").consumer_disconnects, 1);
    }
}
