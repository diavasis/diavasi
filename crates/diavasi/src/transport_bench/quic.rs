use std::net::SocketAddr;
use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use quinn::{ClientConfig, Endpoint, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio_util::codec::Framed;
use tracing::{info, warn};

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
            run_server(cfg).await?;
            anyhow::bail!("server role exited")
        }
        Role::Client => run_client(cfg).await,
        Role::Both => {
            let mut server_cfg = cfg.clone();
            server_cfg.role = Role::Server;
            let mut client_cfg = cfg.clone();
            client_cfg.role = Role::Client;
            let server = tokio::spawn(async move { run_server(server_cfg).await });
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            let result = run_client(client_cfg).await;
            server.abort();
            result
        }
    }
}

fn self_signed() -> anyhow::Result<(CertificateDer<'static>, PrivateKeyDer<'static>)> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = CertificateDer::from(cert.cert);
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()));
    Ok((cert_der, key_der))
}

fn make_server_config() -> anyhow::Result<ServerConfig> {
    let (cert_der, key_der) = self_signed()?;
    let mut crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)?;
    crypto.alpn_protocols = vec![b"diavasi-bench".to_vec()];
    let mut server = ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(crypto)?,
    ));
    let transport = Arc::get_mut(&mut server.transport).expect("unique transport");
    transport.max_concurrent_bidi_streams(1024_u32.into());
    Ok(server)
}

async fn run_server(cfg: BenchConfig) -> anyhow::Result<()> {
    let addr: SocketAddr = cfg.listen.parse()?;
    let endpoint = Endpoint::server(make_server_config()?, addr)?;
    info!(%addr, "quic bench server listening");
    let state = Arc::new(SessionState::new(cfg));

    while let Some(connecting) = endpoint.accept().await {
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            match connecting.await {
                Ok(conn) => {
                    if let Err(e) = handle_quic_connection(conn, state).await {
                        warn!(error = %e, "quic connection ended");
                    }
                }
                Err(e) => warn!(error = %e, "quic accept failed"),
            }
        });
    }
    Ok(())
}

async fn handle_quic_connection(
    conn: quinn::Connection,
    state: Arc<SessionState>,
) -> anyhow::Result<()> {
    let (send, recv) = conn.accept_bi().await?;
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

async fn run_client(cfg: BenchConfig) -> anyhow::Result<BenchResult> {
    let mut endpoint = Endpoint::client("0.0.0.0:0".parse()?)?;
    endpoint.set_default_client_config(insecure_client_config()?);

    let addr: SocketAddr = cfg.connect.parse()?;
    let conn = endpoint.connect(addr, "localhost")?.await?;
    let (send, recv) = conn.open_bi().await?;
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

    let mut notes = vec!["transport=quic".into()];
    if !joined {
        notes.push("warning: never received Joined".into());
    }
    Ok(BenchResult::from_config(&cfg, metrics.snapshot(), notes))
}

fn insecure_client_config() -> anyhow::Result<ClientConfig> {
    let mut crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();
    crypto.alpn_protocols = vec![b"diavasi-bench".to_vec()];
    Ok(ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto)?,
    )))
}

#[derive(Debug)]
struct SkipServerVerification;

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}
