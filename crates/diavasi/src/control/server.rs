use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;

use crate::store::{MASTER_KEY_ENV, RedbStore, StoreKey};

use super::auth::BearerTokenAuth;
use super::routes::{AppState, router};
use super::service::ControlService;

#[derive(Clone)]
pub struct ServeConfig {
    pub bind: SocketAddr,
    pub store_path: PathBuf,
    pub api_token: String,
    pub store_key: Option<StoreKey>,
}

pub const API_TOKEN_ENV: &str = "DIAVASI_API_TOKEN";

/// Run the control-plane HTTP server until cancelled / error.
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

    let state = AppState {
        service,
        auth: BearerTokenAuth::new(config.api_token),
    };
    let app = router(state);
    let listener = TcpListener::bind(config.bind).await?;
    tracing::info!("diavasi control plane listening on {}", config.bind);
    axum::serve(listener, app).await?;
    Ok(())
}
