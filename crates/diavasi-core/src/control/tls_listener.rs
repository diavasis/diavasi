//! HTTPS for the control plane: an axum listener that terminates TLS.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::server::TlsStream;

use super::server::ServeError;

/// How long a client has to finish the TLS handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Accepts TCP connections and hands axum the ones that completed a TLS
/// handshake. Handshakes run in their own tasks, so a slow client does not
/// hold up the others.
pub(crate) struct TlsListener {
    local_addr: SocketAddr,
    ready: mpsc::Receiver<(TlsStream<TcpStream>, SocketAddr)>,
}

impl TlsListener {
    /// A rustls server configuration from PEM certificate chain and key.
    pub(crate) fn server_config(
        cert_pem: &[u8],
        key_pem: &[u8],
    ) -> Result<Arc<ServerConfig>, ServeError> {
        let tls =
            |what: &str, err: &dyn std::fmt::Display| ServeError::Tls(format!("{what}: {err}"));
        let certs = CertificateDer::pem_slice_iter(cert_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| tls("http certificate", &err))?;
        if certs.is_empty() {
            return Err(ServeError::Tls(
                "http certificate: no certificate in the file".into(),
            ));
        }
        let key = PrivateKeyDer::from_pem_slice(key_pem).map_err(|err| tls("http key", &err))?;
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
        let mut config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|err| tls("http certificate", &err))?;
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Arc::new(config))
    }

    /// Start accepting on `listener` with `config`.
    pub(crate) fn new(listener: TcpListener, config: Arc<ServerConfig>) -> Self {
        let local_addr = listener
            .local_addr()
            .expect("a bound socket has an address");
        let acceptor = TlsAcceptor::from(config);
        let (ready_tx, ready) = mpsc::channel(64);
        tokio::spawn(async move {
            loop {
                let (stream, peer) = match listener.accept().await {
                    Ok(accepted) => accepted,
                    Err(err) => {
                        tracing::warn!(error = %err, "control plane accept failed");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                let acceptor = acceptor.clone();
                let ready = ready_tx.clone();
                tokio::spawn(async move {
                    match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                        Ok(Ok(tls)) => {
                            let _ = ready.send((tls, peer)).await;
                        }
                        Ok(Err(err)) => {
                            tracing::debug!(%peer, error = %err, "TLS handshake failed");
                        }
                        Err(_) => tracing::debug!(%peer, "TLS handshake timed out"),
                    }
                });
                if ready_tx.is_closed() {
                    break;
                }
            }
        });
        Self { local_addr, ready }
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.ready.recv().await {
            Some(accepted) => accepted,
            // The accept task only ends when this receiver is gone.
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        Ok(self.local_addr)
    }
}
