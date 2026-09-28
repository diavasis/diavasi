use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::dataplane::{DataPlaneConfig, load_or_generate_pem, load_pem, serve_dataplane_on};
use crate::store::{MASTER_KEY_ENV, RedbStore, StateStore, StoreError, StoreKey, open_secret};

use super::auth::BearerTokenAuth;
use super::routes::{AppState, router};
use super::service::ControlService;
use super::tls_listener::TlsListener;

/// Settings for [`serve`], [`serve_until`], and [`serve_on`].
///
/// ```no_run
/// use diavasi::control::{ServeConfig, serve};
/// use diavasi::store::StoreKey;
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// let mut config = ServeConfig::new(
///     "127.0.0.1:7700".parse()?,
///     "127.0.0.1:7710".parse()?,
///     "/var/lib/diavasi/meta.redb",
///     std::env::var("DIAVASI_API_TOKEN")?,
/// );
/// config.store_key = Some(StoreKey::from_hex(&std::env::var("DIAVASI_STORE_KEY")?)?);
/// serve(config).await?;
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct ServeConfig {
    /// Control-plane HTTP address.
    pub bind: SocketAddr,
    /// Data-plane gRPC address.
    pub data_bind: SocketAddr,
    /// The redb store file. Created, with its directory, when missing.
    pub store_path: PathBuf,
    /// Bearer token for `/v1`, `/metrics`, and the data plane.
    pub api_token: String,
    /// Key for connection secrets. `None` reads `DIAVASI_STORE_KEY`; without
    /// either, a temporary key is used and connections cannot be created.
    pub store_key: Option<StoreKey>,
    /// Data-plane certificate. Set with `tls_key` or not at all; when both are
    /// unset, a local CA and certificate are generated next to the store.
    pub tls_cert: Option<PathBuf>,
    /// Private key for `tls_cert`.
    pub tls_key: Option<PathBuf>,
    /// Extra DNS names or IP addresses for a generated data-plane certificate,
    /// besides `localhost` and `127.0.0.1`. Used only when the certificate is
    /// generated; an existing one is kept.
    pub tls_san: Vec<String>,
    /// Control-plane certificate. When set with `http_tls_key`, the control
    /// plane serves HTTPS instead of HTTP.
    pub http_tls_cert: Option<PathBuf>,
    /// Private key for `http_tls_cert`.
    pub http_tls_key: Option<PathBuf>,
    /// Opens adapter sources. `None` allows only synthetic groups.
    pub source_factory: Option<Arc<dyn crate::runtime::SourceFactory>>,
    /// Zero writes each ack's checkpoint before answering it. A positive
    /// interval writes at most once per interval; see
    /// [`GroupRuntimeConfig::checkpoint_interval`](crate::runtime::GroupRuntimeConfig::checkpoint_interval).
    pub checkpoint_interval: Duration,
}

impl ServeConfig {
    /// A configuration with the given addresses, store, and token, and every
    /// option at its default: store key from the environment, generated
    /// data-plane TLS, plain HTTP, synthetic groups only, a checkpoint per ack.
    pub fn new(
        bind: SocketAddr,
        data_bind: SocketAddr,
        store_path: impl Into<PathBuf>,
        api_token: impl Into<String>,
    ) -> Self {
        Self {
            bind,
            data_bind,
            store_path: store_path.into(),
            api_token: api_token.into(),
            store_key: None,
            tls_cert: None,
            tls_key: None,
            tls_san: Vec::new(),
            http_tls_cert: None,
            http_tls_key: None,
            source_factory: None,
            checkpoint_interval: Duration::ZERO,
        }
    }
}

/// Environment variable the CLI reads the bearer token from.
pub const API_TOKEN_ENV: &str = "DIAVASI_API_TOKEN";

/// Why the server could not start or stopped with an error.
#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    /// The store could not be opened or read.
    #[error("store: {0}")]
    Store(#[from] StoreError),
    /// The store key is missing or cannot open the stored secrets.
    #[error("store key: {0}")]
    StoreKey(String),
    /// A certificate or key could not be read, generated, or used.
    #[error("TLS: {0}")]
    Tls(String),
    /// An address could not be bound.
    #[error("listen on {addr}: {source}")]
    Bind {
        /// The address.
        addr: SocketAddr,
        /// Why binding failed.
        source: std::io::Error,
    },
    /// The configuration is inconsistent.
    #[error("{0}")]
    Config(String),
    /// Creating the store directory or serving HTTP failed.
    #[error("control plane: {0}")]
    Io(#[from] std::io::Error),
    /// The data plane stopped with an error.
    #[error("data plane: {0}")]
    DataPlane(String),
}

/// The sockets of the two planes, bound before the server starts. Bind port 0
/// and read [`Listeners::control_addr`] and [`Listeners::data_addr`] to get
/// free ports that nothing else can take before the server uses them.
///
/// ```
/// use diavasi::control::Listeners;
/// let listeners = Listeners::bind("127.0.0.1:0".parse()?, "127.0.0.1:0".parse()?)?;
/// assert_ne!(listeners.control_addr().port(), 0);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct Listeners {
    control: std::net::TcpListener,
    data: std::net::TcpListener,
}

impl Listeners {
    /// Bind both addresses.
    pub fn bind(control: SocketAddr, data: SocketAddr) -> Result<Self, ServeError> {
        let bind = |addr: SocketAddr| {
            let listener = std::net::TcpListener::bind(addr)
                .map_err(|source| ServeError::Bind { addr, source })?;
            listener
                .set_nonblocking(true)
                .map_err(|source| ServeError::Bind { addr, source })?;
            Ok::<_, ServeError>(listener)
        };
        Ok(Self {
            control: bind(control)?,
            data: bind(data)?,
        })
    }

    /// The control-plane address, with the port the system chose for port 0.
    pub fn control_addr(&self) -> SocketAddr {
        self.control
            .local_addr()
            .expect("a bound socket has an address")
    }

    /// The data-plane address, with the port the system chose for port 0.
    pub fn data_addr(&self) -> SocketAddr {
        self.data
            .local_addr()
            .expect("a bound socket has an address")
    }
}

/// Pick the store key: `configured`, else `DIAVASI_STORE_KEY`, else a
/// temporary key. Returns the key and whether it is temporary.
///
/// A temporary key is refused when the store already holds sealed secrets,
/// and a key that cannot open every stored secret is refused, so a restart
/// never runs with secrets it cannot read.
fn resolve_store_key(
    configured: Option<StoreKey>,
    store: &RedbStore,
) -> Result<(StoreKey, bool), ServeError> {
    let connections = store.list_connections()?;
    let from_env = || StoreKey::from_env().map_err(|err| ServeError::StoreKey(err.to_string()));
    let key = match configured {
        Some(key) => Some(key),
        None => from_env()?,
    };
    let (key, ephemeral) = match key {
        Some(key) => (key, false),
        None if connections.is_empty() => {
            tracing::warn!(
                "{MASTER_KEY_ENV} is unset; using a temporary store key. Connections cannot be created until a key is set."
            );
            (StoreKey::generate(), true)
        }
        None => {
            return Err(ServeError::StoreKey(format!(
                "{MASTER_KEY_ENV} is required: this store holds {} sealed connection secret(s)",
                connections.len()
            )));
        }
    };
    for connection in &connections {
        if open_secret(&key, &connection.sealed_secret).is_err() {
            return Err(ServeError::StoreKey(format!(
                "the store key cannot open the secret of connection {}; start with the key this store was created with",
                connection.id
            )));
        }
    }
    Ok((key, ephemeral))
}

/// Run the control plane and TLS gRPC data plane until SIGINT or SIGTERM,
/// or until one of them fails. See [`serve_on`].
pub async fn serve(config: ServeConfig) -> Result<(), ServeError> {
    serve_until(config, shutdown_signal()).await
}

/// Like [`serve`], until `shutdown` completes instead of a signal.
pub async fn serve_until(
    config: ServeConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), ServeError> {
    let listeners = Listeners::bind(config.bind, config.data_bind)?;
    serve_on(config, listeners, shutdown).await
}

/// Run the control plane and TLS gRPC data plane on `listeners` until
/// `shutdown` completes, or until one of them fails. `config.bind` and
/// `config.data_bind` are only reported; the listeners are used.
///
/// On start, groups whose stored lifecycle is `Running` or `Draining` resume.
/// On shutdown, the servers stop accepting requests and every running group
/// saves its progress without changing its lifecycle, so it resumes at the
/// next start.
pub async fn serve_on(
    config: ServeConfig,
    listeners: Listeners,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), ServeError> {
    let store = open_store(&config.store_path)?;
    let (key, ephemeral) = resolve_store_key(config.store_key.clone(), store.as_ref())?;
    let (tls_cert_pem, tls_key_pem) = data_plane_tls(&config)?;
    let http_tls = match (&config.http_tls_cert, &config.http_tls_key) {
        (Some(cert), Some(key)) => {
            let (cert, key) =
                load_pem(cert, key).map_err(|err| ServeError::Tls(err.to_string()))?;
            Some(TlsListener::server_config(&cert, &key)?)
        }
        (None, None) => None,
        _ => {
            return Err(ServeError::Config(
                "http tls cert and key must be set together".into(),
            ));
        }
    };
    let control_addr = listeners.control_addr();
    let data_addr = listeners.data_addr();
    let control = tokio::net::TcpListener::from_std(listeners.control)?;
    let data_listener = tokio::net::TcpListener::from_std(listeners.data)?;

    let service = ControlService::new(Arc::clone(&store), key, control_addr.to_string());
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

    let mut data = tokio::spawn(serve_dataplane_on(
        DataPlaneConfig {
            bind: data_addr,
            tls_cert_pem,
            tls_key_pem,
            api_token: config.api_token.clone(),
            supervisor: service.supervisor(),
            heartbeat_interval: Duration::from_secs(5),
            heartbeat_timeout: Duration::from_secs(30),
        },
        data_listener,
    ));

    let state = AppState {
        service: Arc::clone(&service),
        auth: Arc::new(BearerTokenAuth::new(config.api_token)),
    };
    let app = router(state);
    let scheme = if http_tls.is_some() { "https" } else { "http" };
    tracing::info!(control = %format!("{scheme}://{control_addr}"), data = %data_addr, "diavasi listening");
    let http = async move {
        match http_tls {
            Some(tls) => {
                axum::serve(TlsListener::new(control, tls), app)
                    .with_graceful_shutdown(shutdown)
                    .await
            }
            None => {
                axum::serve(control, app)
                    .with_graceful_shutdown(shutdown)
                    .await
            }
        }
    };
    let result: Result<(), ServeError> = tokio::select! {
        result = http => result.map_err(ServeError::Io),
        result = &mut data => match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(ServeError::DataPlane(err.to_string())),
            Err(err) => Err(ServeError::DataPlane(err.to_string())),
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

fn open_store(path: &Path) -> Result<Arc<RedbStore>, ServeError> {
    if path.exists() {
        return Ok(Arc::new(RedbStore::open(path)?));
    }
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    Ok(Arc::new(RedbStore::create(path)?))
}

/// The data-plane certificate and key: the configured files, or a pair
/// generated next to the store.
fn data_plane_tls(config: &ServeConfig) -> Result<(Vec<u8>, Vec<u8>), ServeError> {
    let tls = |err: std::io::Error| ServeError::Tls(err.to_string());
    match (&config.tls_cert, &config.tls_key) {
        (Some(cert), Some(key)) => load_pem(cert, key).map_err(tls),
        (None, None) => {
            let dir = config
                .store_path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| PathBuf::from("."));
            load_or_generate_pem(
                &dir.join("dataplane.crt"),
                &dir.join("dataplane.key"),
                &config.tls_san,
            )
            .map_err(tls)
        }
        _ => Err(ServeError::Config(
            "tls cert and key must be set together".into(),
        )),
    }
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
