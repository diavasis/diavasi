use clap::Parser;
use tracing_subscriber::EnvFilter;

use diavasi::transport_bench::config::{BenchConfig, TransportKind};
use diavasi::transport_bench::{grpc, quic, report, tcp, webtransport};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("install rustls crypto provider");

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let mut cfg = BenchConfig::parse();
    cfg.apply_smoke();

    let result = match cfg.transport {
        TransportKind::Tcp => tcp::run(cfg.clone()).await?,
        TransportKind::Grpc => grpc::run(cfg.clone()).await?,
        TransportKind::Quic => quic::run(cfg.clone()).await?,
        TransportKind::Webtransport => webtransport::run(cfg.clone()).await?,
    };

    println!("{}", serde_json::to_string_pretty(&result)?);
    if let Some(path) = &cfg.output {
        result.write_jsonl(path)?;
    }
    if let Some(rss) = report::process_rss_mib() {
        eprintln!("rss_mib={rss:.2}");
    }
    Ok(())
}
