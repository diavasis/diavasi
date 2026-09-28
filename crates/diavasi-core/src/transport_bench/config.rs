use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportKind {
    Tcp,
    Grpc,
    Quic,
    Webtransport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordSize {
    Small,
    Medium,
    Large,
}

impl RecordSize {
    pub fn bytes(self) -> usize {
        match self {
            Self::Small => 64,
            Self::Medium => 1024,
            Self::Large => 64 * 1024,
        }
    }
}

#[derive(Debug, Clone, Parser, Serialize, Deserialize)]
#[command(name = "diavasi-transport-bench")]
pub struct BenchConfig {
    /// Role: run as server, client, or both (in-process).
    #[arg(long, default_value = "both")]
    pub role: Role,

    #[arg(long, value_enum, default_value_t = TransportKind::Tcp)]
    pub transport: TransportKind,

    #[arg(long, default_value = "127.0.0.1:9800")]
    pub listen: String,

    #[arg(long, default_value = "127.0.0.1:9800")]
    pub connect: String,

    #[arg(long, default_value = "bench")]
    pub group_id: String,

    #[arg(long, default_value_t = 1)]
    pub consumers: u32,

    #[arg(long, value_enum, default_value_t = RecordSize::Small)]
    pub record_size: RecordSize,

    #[arg(long, default_value_t = 32)]
    pub batch_size: u32,

    #[arg(long, default_value_t = 10_000)]
    pub total_records: u64,

    #[arg(long, default_value_t = 8)]
    pub max_in_flight: u32,

    /// Artificial ACK delay in milliseconds.
    #[arg(long, default_value_t = 0)]
    pub ack_delay_ms: u64,

    /// Duration to run before stopping (0 = until total_records).
    #[arg(long, default_value_t = 0)]
    pub duration_secs: u64,

    #[arg(long)]
    pub output: Option<PathBuf>,

    #[arg(long, default_value = "rust")]
    pub client_lang: String,

    /// Smoke mode: tiny workload for CI.
    #[arg(long, default_value_t = false)]
    pub smoke: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Server,
    Client,
    Both,
}

impl BenchConfig {
    pub fn apply_smoke(&mut self) {
        if self.smoke {
            self.total_records = 200;
            self.batch_size = 8;
            self.consumers = 1;
            self.record_size = RecordSize::Small;
            self.max_in_flight = 2;
        }
    }

    pub fn ack_delay(&self) -> Duration {
        Duration::from_millis(self.ack_delay_ms)
    }
}
