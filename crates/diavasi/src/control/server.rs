use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;

use crate::dataplane::{DataPlaneConfig, load_or_generate_pem, serve_dataplane};
use crate::store::{MASTER_KEY_ENV, RedbStore, StoreKey};

use super::auth::BearerTokenAuth;
use super::routes::{AppState, router};
use super::service::ControlService;

#[derive(Clone)]
pub struct ServeConfig {
    pub bind: SocketAddr,
    pub data_bind: SocketAddr,
    pub store_path: PathBuf,
    pub api_token: String,
    pub store_key: Option<StoreKey>,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
}

pub const API_TOKEN_ENV: &str = "DIAVASI_API_TOKEN";

/// Run the control plane and TLS gRPC data plane until one of them stops.
pub async fn serve(config: ServeConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let store = if config.store_path.exists() {
        Arc::new(RedbStore::open(&config.store_path)?)
    } else {
        if let Some(parent) = config.store_path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        Arc::new(RedbStore::create(&config.store_path)?)
    };

    let key = match config.store_key {
        Some(k) => k,
        None => {
            if std::env::var(MASTER_KEY_ENV).is_err() {
                tracing::warn!(
                    "{MASTER_KEY_ENV} unset; using ephemeral store key for this process"
                );
            }
            StoreKey::from_env_or_generate()?
        }
    };

    let service = Arc::new(ControlService::new(
        Arc::clone(&store),
        key,
        config.bind.to_string(),
    ));

    let supervise_svc = Arc::clone(&service);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;
            let _ = supervise_svc.supervise_once().await;
        }
    });

    let (cert_path, key_path) = match (&config.tls_cert, &config.tls_key) {
        (Some(cert), Some(key)) => (cert.clone(), key.clone()),
        (None, None) => {
            let dir = config
                .store_path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| PathBuf::from("."));
            (dir.join("dataplane.crt"), dir.join("dataplane.key"))
        }
        _ => {
            return Err("tls cert and key must be set together".into());
        }
    };
    let (tls_cert_pem, tls_key_pem) = load_or_generate_pem(&cert_path, &key_path)?;

    let mut data = tokio::spawn(serve_dataplane(DataPlaneConfig {
        bind: config.data_bind,
        tls_cert_pem,
        tls_key_pem,
        api_token: config.api_token.clone(),
        supervisor: service.supervisor(),
        heartbeat_interval: Duration::from_secs(5),
        heartbeat_timeout: Duration::from_secs(30),
    }));

    let state = AppState {
        service,
        auth: BearerTokenAuth::new(config.api_token),
    };
    let app = router(state);
    let listener = TcpListener::bind(config.bind).await?;
    tracing::info!("diavasi control plane listening on {}", config.bind);
    let data_abort = data.abort_handle();
    tokio::select! {
        result = axum::serve(listener, app) => {
            data_abort.abort();
            result?;
        }
        result = &mut data => {
            result??;
        }
    }
    Ok(())
}
