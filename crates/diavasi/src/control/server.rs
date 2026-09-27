use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;

use crate::dataplane::{DataPlaneConfig, load_or_generate_pem, load_pem, serve_dataplane};
use crate::store::{MASTER_KEY_ENV, RedbStore, StateStore, StoreKey, open_secret};

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
    pub source_factory: Option<Arc<dyn crate::runtime::SourceFactory>>,
    /// Zero writes each ack's checkpoint before answering it. A positive
    /// interval writes at most once per interval; see
    /// [`GroupRuntimeConfig::checkpoint_interval`](crate::runtime::GroupRuntimeConfig::checkpoint_interval).
    pub checkpoint_interval: Duration,
}

pub const API_TOKEN_ENV: &str = "DIAVASI_API_TOKEN";

/// Pick the store key: `configured`, else `DIAVASI_STORE_KEY`, else a
/// temporary key. Returns the key and whether it is temporary.
///
/// A temporary key is refused when the store already holds sealed secrets,
/// and a key that cannot open every stored secret is refused, so a restart
/// never runs with secrets it cannot read.
fn resolve_store_key(
    configured: Option<StoreKey>,
    store: &RedbStore,
) -> Result<(StoreKey, bool), Box<dyn std::error::Error + Send + Sync>> {
    let connections = store.list_connections()?;
    let (key, ephemeral) = match configured
        .map(Ok)
        .or_else(|| StoreKey::from_env().transpose())
    {
        Some(key) => (key?, false),
        None if connections.is_empty() => {
            tracing::warn!(
                "{MASTER_KEY_ENV} is unset; using a temporary store key. Connections cannot be created until a key is set."
            );
            (StoreKey::generate(), true)
        }
        None => {
            return Err(format!(
                "{MASTER_KEY_ENV} is required: this store holds {} sealed connection secret(s)",
                connections.len()
            )
            .into());
        }
    };
    for connection in &connections {
        if open_secret(&key, &connection.sealed_secret).is_err() {
            return Err(format!(
                "the store key cannot open the secret of connection {}; start with the key this store was created with",
                connection.id
            )
            .into());
        }
    }
    Ok((key, ephemeral))
}

/// Run the control plane and TLS gRPC data plane until SIGINT or SIGTERM,
/// or until one of them fails. See [`serve_until`].
pub async fn serve(config: ServeConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    serve_until(config, shutdown_signal()).await
}

/// Run the control plane and TLS gRPC data plane until `shutdown` completes,
/// or until one of them fails.
///
/// On start, groups whose stored lifecycle is `Running` or `Draining` resume.
/// On shutdown, the servers stop accepting requests and every running group
/// saves its progress without changing its lifecycle, so it resumes at the
/// next start.
pub async fn serve_until(
    config: ServeConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

    let (key, ephemeral) = resolve_store_key(config.store_key, store.as_ref())?;
    let (tls_cert_pem, tls_key_pem) = match (&config.tls_cert, &config.tls_key) {
        (Some(cert), Some(key)) => load_pem(cert, key)?,
        (None, None) => {
            let dir = config
                .store_path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| PathBuf::from("."));
            load_or_generate_pem(&dir.join("dataplane.crt"), &dir.join("dataplane.key"))?
        }
        _ => {
            return Err("tls cert and key must be set together".into());
        }
    };
    let listener = TcpListener::bind(config.bind).await?;

    let service = ControlService::new(Arc::clone(&store), key, config.bind.to_string());
    let service = Arc::new(if ephemeral {
        service.with_ephemeral_key()
    } else {
        service
    });
    if let Some(factory) = config.source_factory.clone() {
        service.install_source_factory(factory).await;
    }
    service
        .set_runtime_config(crate::runtime::GroupRuntimeConfig {
            checkpoint_interval: config.checkpoint_interval,
            ..Default::default()
        })
        .await;
    service.resume_groups().await;

    let supervise_svc = Arc::clone(&service);
    let supervise = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;
            let _ = supervise_svc.supervise_once().await;
        }
    });

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
        service: Arc::clone(&service),
        auth: Arc::new(BearerTokenAuth::new(config.api_token)),
    };
    let app = router(state);
    tracing::info!("diavasi control plane listening on {}", config.bind);
    let result: Result<(), Box<dyn std::error::Error + Send + Sync>> = tokio::select! {
        result = axum::serve(listener, app).with_graceful_shutdown(shutdown) => {
            result.map_err(Into::into)
        }
        result = &mut data => match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(err),
            Err(err) => Err(err.into()),
        },
    };

    supervise.abort();
    let _ = supervise.await;
    // `select!` may already have taken the data plane's result.
    if !data.is_finished() {
        data.abort();
        let _ = data.await;
    }
    service.shutdown_groups().await;
    tracing::info!("diavasi stopped");
    result
}

/// Completes on SIGINT (Ctrl-C) or, on Unix, SIGTERM.
async fn shutdown_signal() {
    let interrupt = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = interrupt => {}
        () = terminate => {}
    }
    tracing::info!("shutdown requested");
}
