use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::config::{BenchConfig, TransportKind};
use super::metrics::MetricsSnapshot;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchResult {
    pub transport: TransportKind,
    pub client_lang: String,
    pub consumers: u32,
    pub record_size_bytes: usize,
    pub batch_size: u32,
    pub total_records: u64,
    pub max_in_flight: u32,
    pub ack_delay_ms: u64,
    pub metrics: MetricsSnapshot,
    pub notes: Vec<String>,
}

impl BenchResult {
    pub fn from_config(cfg: &BenchConfig, metrics: MetricsSnapshot, notes: Vec<String>) -> Self {
        Self {
            transport: cfg.transport,
            client_lang: cfg.client_lang.clone(),
            consumers: cfg.consumers,
            record_size_bytes: cfg.record_size.bytes(),
            batch_size: cfg.batch_size,
            total_records: cfg.total_records,
            max_in_flight: cfg.max_in_flight,
            ack_delay_ms: cfg.ack_delay_ms,
            metrics,
            notes,
        }
    }

    pub fn write_jsonl(&self, path: &Path) -> std::io::Result<()> {
        use std::io::Write;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        serde_json::to_writer(&mut f, self)?;
        f.write_all(b"\n")?;
        Ok(())
    }
}

pub fn process_rss_mib() -> Option<f64> {
    use sysinfo::{Pid, ProcessesToUpdate, System};
    let mut sys = System::new();
    let pid = Pid::from_u32(std::process::id());
    sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
    sys.process(pid)
        .map(|p| p.memory() as f64 / (1024.0 * 1024.0))
}

pub type SharedNotes = Arc<std::sync::Mutex<Vec<String>>>;
