use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand, ValueEnum};
use diavasi::control::{
    API_TOKEN_ENV, BackupRequest, ConnectionCreateRequest, GroupCreateRequest, ServeConfig, serve,
};
use diavasi::store::StoreKey;

#[derive(Parser, Debug)]
#[command(name = "diavasi", version, about = "Diavasi administration CLI")]
struct Cli {
    /// Control plane base URL (client commands).
    #[arg(
        long,
        global = true,
        default_value = "http://127.0.0.1:7700",
        env = "DIAVASI_URL"
    )]
    url: String,

    /// Bearer token (client commands).
    #[arg(long, global = true, env = "DIAVASI_API_TOKEN", hide_env_values = true)]
    token: Option<String>,

    /// Output format.
    #[arg(long, global = true, default_value = "text", value_enum)]
    output: OutputFormat,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum OutputFormat {
    Text,
    Json,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum AdapterName {
    Postgres,
    #[value(alias = "mongo")]
    Mongodb,
    Redis,
    Scylla,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the server: the control plane (HTTP) and the data plane (TLS gRPC).
    Serve {
        #[arg(long, default_value = "127.0.0.1:7700")]
        bind: SocketAddr,
        #[arg(long)]
        store: PathBuf,
        #[arg(long, env = "DIAVASI_API_TOKEN", hide_env_values = true)]
        token: String,
        /// Optional 64-char hex store master key (else DIAVASI_STORE_KEY / ephemeral).
        #[arg(long)]
        store_key: Option<String>,
        /// TLS gRPC data plane bind address.
        #[arg(long, default_value = "127.0.0.1:7710")]
        data_bind: SocketAddr,
        /// PEM certificate for the data plane. Generated next to the store when omitted.
        #[arg(long)]
        tls_cert: Option<PathBuf>,
        /// PEM private key for the data plane.
        #[arg(long)]
        tls_key: Option<PathBuf>,
        /// Extra DNS name or IP address for the generated data-plane
        /// certificate, besides localhost and 127.0.0.1. Repeat for several.
        #[arg(long = "tls-san")]
        tls_san: Vec<String>,
        /// PEM certificate for the control plane. With --http-tls-key, the
        /// control plane serves HTTPS.
        #[arg(long)]
        http_tls_cert: Option<PathBuf>,
        /// PEM private key for the control plane.
        #[arg(long)]
        http_tls_key: Option<PathBuf>,
        /// Write checkpoints at most once per this many milliseconds. 0 (the
        /// default) writes before each ack is answered. A larger value raises
        /// throughput; a crash can replay up to one interval of acked records.
        #[arg(long, default_value_t = 0, env = "DIAVASI_CHECKPOINT_INTERVAL_MS")]
        checkpoint_interval_ms: u64,
    },
    /// Print library version.
    Version,
    /// Seed Postgres, MongoDB, Redis, or ScyllaDB, then consume the rows and print throughput.
    ///
    /// Creates a table, collection, or stream named `diavasi_test_<pid>` in the
    /// target database and drops it at the end unless `--keep` is set.
    Test {
        /// `postgres`, `mongo` / `mongodb`, `redis`, or `scylla`.
        adapter: AdapterName,
        /// Records to insert.
        #[arg(short = 'n', long, default_value_t = 10_000)]
        records: u64,
        /// Bytes stored in each payload field.
        #[arg(short = 'b', long, default_value_t = 1024)]
        payload_bytes: usize,
        #[arg(
            long,
            env = "DATABASE_URL",
            default_value = "postgres://diavasi:diavasi@127.0.0.1:5433/diavasi"
        )]
        database_url: String,
        #[arg(long, env = "MONGODB_URL", default_value = "mongodb://127.0.0.1:27017")]
        mongodb_url: String,
        #[arg(long, env = "REDIS_URL", default_value = "redis://127.0.0.1:6379")]
        redis_url: String,
        #[arg(long, env = "SCYLLA_URL", default_value = "127.0.0.1:9042")]
        scylla_url: String,
        /// Leave the seeded table, collection, or stream in place.
        #[arg(long)]
        keep: bool,
    },
    /// Commands that call a running server's control plane.
    #[command(flatten)]
    Remote(RemoteCmd),
}

/// Commands sent to the control plane at `--url` with `--token`.
#[derive(Subcommand, Debug)]
enum RemoteCmd {
    /// Server status.
    Status,
    /// Live dashboard of the control plane.
    Tui,
    /// Add, list, show, and delete database connections.
    #[command(subcommand)]
    Connection(ConnectionCmd),
    /// Create, run, and inspect groups.
    #[command(subcommand)]
    Group(GroupCmd),
    /// List the consumers joined to a group.
    #[command(subcommand)]
    Consumer(ConsumerCmd),
    /// Show a group's stored and live cursors.
    #[command(subcommand)]
    Checkpoint(CheckpointCmd),
    /// Back up the metadata store.
    #[command(subcommand)]
    Store(StoreCmd),
}

/// Environment variable read for the connection secret when no secret flag
/// is given.
const SECRET_ENV: &str = "DIAVASI_CONNECTION_SECRET";

/// Where the connection secret comes from. At most one flag; with none, the
/// secret is read from `DIAVASI_CONNECTION_SECRET`.
#[derive(Args, Debug)]
#[group(multiple = false)]
struct SecretSource {
    /// The secret itself. Visible in the process list and shell history;
    /// prefer --secret-file, --secret-stdin, or DIAVASI_CONNECTION_SECRET.
    #[arg(long)]
    secret: Option<String>,
    /// Read the secret from this file. One trailing newline is removed.
    #[arg(long)]
    secret_file: Option<PathBuf>,
    /// Read the secret from standard input. One trailing newline is removed.
    #[arg(long)]
    secret_stdin: bool,
}

impl SecretSource {
    fn resolve(self) -> Result<String, String> {
        let raw = if let Some(secret) = self.secret {
            secret
        } else if let Some(path) = self.secret_file {
            std::fs::read_to_string(&path)
                .map_err(|err| format!("reading {}: {err}", path.display()))?
        } else if self.secret_stdin {
            let mut buf = String::new();
            std::io::Read::read_to_string(&mut std::io::stdin(), &mut buf)
                .map_err(|err| format!("reading stdin: {err}"))?;
            buf
        } else {
            std::env::var(SECRET_ENV).map_err(|_| {
                format!("give --secret, --secret-file, --secret-stdin, or set {SECRET_ENV}")
            })?
        };
        let secret = raw
            .strip_suffix('\n')
            .map(|rest| rest.strip_suffix('\r').unwrap_or(rest))
            .unwrap_or(&raw);
        if secret.is_empty() {
            return Err("the secret is empty".into());
        }
        Ok(secret.to_string())
    }
}

#[derive(Subcommand, Debug)]
enum ConnectionCmd {
    Add {
        #[arg(long)]
        id: String,
        #[arg(long)]
        kind: String,
        #[arg(long, default_value = "{}")]
        config_json: String,
        #[command(flatten)]
        secret: SecretSource,
    },
    List,
    Show {
        id: String,
    },
    Delete {
        id: String,
    },
}

#[derive(Subcommand, Debug)]
enum GroupCmd {
    Create {
        #[arg(long)]
        group_id: String,
        /// Synthetic groups only: records to generate (default 100).
        #[arg(long, conflicts_with = "connection_id")]
        total_records: Option<u64>,
        /// Synthetic groups only: payload bytes per record (default 64).
        #[arg(long, conflicts_with = "connection_id")]
        payload_size: Option<usize>,
        #[arg(long, default_value_t = 1024)]
        max_buffer_records: usize,
        #[arg(long, default_value_t = 1024 * 1024)]
        max_buffer_bytes: usize,
        #[arg(long, default_value_t = 32)]
        batch_max_records: usize,
        #[arg(long, default_value_t = 100)]
        batch_timeout_ms: u64,
        /// A label stored with the group. Defaults to `synthetic-u64` or the
        /// connection kind.
        #[arg(long)]
        ordering_contract: Option<String>,
        #[arg(long)]
        connection_id: Option<String>,
        /// The adapter source contract (`source_spec`) as JSON.
        #[arg(long)]
        source_json: Option<String>,
    },
    List,
    Show {
        id: String,
    },
    Delete {
        id: String,
    },
    Start {
        id: String,
    },
    Pause {
        id: String,
    },
    Resume {
        id: String,
    },
    Drain {
        id: String,
    },
    Diagnostics {
        id: String,
    },
}

#[derive(Subcommand, Debug)]
enum ConsumerCmd {
    List { group: String },
}

#[derive(Subcommand, Debug)]
enum CheckpointCmd {
    Show { group: String },
}

#[derive(Subcommand, Debug)]
enum StoreCmd {
    /// Copy the server's metadata store to a new file on the server host.
    Backup {
        /// Absolute path on the server host. Must not exist yet.
        path: String,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(code) => code,
    }
}

mod adapter_test;
mod sources;
mod tui;

async fn run(cli: Cli) -> Result<(), ExitCode> {
    match cli.command {
        Commands::Version => {
            println!("diavasi {}", diavasi::VERSION);
            Ok(())
        }
        Commands::Serve {
            bind,
            store,
            token,
            store_key,
            data_bind,
            tls_cert,
            tls_key,
            tls_san,
            http_tls_cert,
            http_tls_key,
            checkpoint_interval_ms,
        } => {
            diavasi::observe::init_serve_tracing();
            let store_key = match store_key {
                Some(hex) => Some(StoreKey::from_hex(&hex).map_err(|e| {
                    eprintln!("error: {e}");
                    ExitCode::from(2)
                })?),
                None => None,
            };
            if let Err(e) = serve(ServeConfig {
                bind,
                data_bind,
                store_path: store,
                api_token: token,
                store_key,
                tls_cert,
                tls_key,
                source_factory: Some(std::sync::Arc::new(sources::RoutingFactory::installed())),
                checkpoint_interval: std::time::Duration::from_millis(checkpoint_interval_ms),
                tls_san,
                http_tls_cert,
                http_tls_key,
            })
            .await
            {
                eprintln!("error: {e}");
                return Err(ExitCode::FAILURE);
            }
            Ok(())
        }
        Commands::Test {
            adapter,
            records,
            payload_bytes,
            database_url,
            mongodb_url,
            redis_url,
            scylla_url,
            keep,
        } => {
            if let Err(err) = adapter_test::run(adapter_test::AdapterTest {
                adapter: match adapter {
                    AdapterName::Postgres => adapter_test::AdapterKind::Postgres,
                    AdapterName::Mongodb => adapter_test::AdapterKind::Mongodb,
                    AdapterName::Redis => adapter_test::AdapterKind::Redis,
                    AdapterName::Scylla => adapter_test::AdapterKind::Scylla,
                },
                records,
                payload_bytes,
                database_url,
                mongodb_url,
                redis_url,
                scylla_url,
                keep,
                // One name per run, so an object of the same name that
                // someone else created is never dropped.
                object: format!("diavasi_test_{}", std::process::id()),
                json: matches!(cli.output, OutputFormat::Json),
            })
            .await
            {
                eprintln!("error: {err}");
                return Err(ExitCode::FAILURE);
            }
            Ok(())
        }
        Commands::Remote(cmd) => {
            let token = cli.token.ok_or_else(|| {
                eprintln!("error: --token or {API_TOKEN_ENV} required");
                ExitCode::from(2)
            })?;
            let client = Client {
                base: cli.url.trim_end_matches('/').to_string(),
                token,
                output: cli.output,
                http: reqwest::Client::new(),
            };
            client.dispatch(cmd).await
        }
    }
}

struct Client {
    base: String,
    token: String,
    output: OutputFormat,
    http: reqwest::Client,
}

impl Client {
    async fn dispatch(&self, cmd: RemoteCmd) -> Result<(), ExitCode> {
        match cmd {
            RemoteCmd::Tui => tui::run(&self.base, &self.token).await,
            RemoteCmd::Status => {
                let v = self.get_json("/v1/status").await?;
                self.print_value(&v, |v| {
                    println!(
                        "version={} schema={} bind={} running={}",
                        v["version"].as_str().unwrap_or("?"),
                        v["schema_version"],
                        v["bind"].as_str().unwrap_or("?"),
                        text(&v["running_groups"])
                    );
                });
                Ok(())
            }
            RemoteCmd::Connection(ConnectionCmd::Add {
                id,
                kind,
                config_json,
                secret,
            }) => {
                let config: serde_json::Value =
                    serde_json::from_str(&config_json).map_err(|e| {
                        eprintln!("error: invalid --config-json: {e}");
                        ExitCode::from(2)
                    })?;
                let body = ConnectionCreateRequest {
                    id,
                    kind,
                    config_json: config,
                    secret: secret.resolve().map_err(|err| {
                        eprintln!("error: {err}");
                        ExitCode::from(2)
                    })?,
                };
                let v = self.post_json("/v1/connections", &body).await?;
                self.print_value(&v, |v| {
                    println!("connection {} created (secret sealed)", text(&v["id"]));
                });
                Ok(())
            }
            RemoteCmd::Connection(ConnectionCmd::List) => {
                let v = self.get_json("/v1/connections").await?;
                self.print_value(&v, |v| {
                    if let Some(arr) = v.as_array() {
                        for c in arr {
                            println!(
                                "{}\t{}\tsealed={}",
                                c["id"].as_str().unwrap_or("?"),
                                c["kind"].as_str().unwrap_or("?"),
                                c["secret_sealed"]
                            );
                        }
                    }
                });
                Ok(())
            }
            RemoteCmd::Connection(ConnectionCmd::Show { id }) => {
                let v = self.get_json(&format!("/v1/connections/{id}")).await?;
                self.print_value(&v, |v| {
                    println!(
                        "id={} kind={} sealed={}",
                        v["id"].as_str().unwrap_or("?"),
                        v["kind"].as_str().unwrap_or("?"),
                        v["secret_sealed"]
                    );
                });
                Ok(())
            }
            RemoteCmd::Connection(ConnectionCmd::Delete { id }) => {
                self.delete(&format!("/v1/connections/{id}")).await?;
                if matches!(self.output, OutputFormat::Json) {
                    println!("{{\"ok\":true}}");
                } else {
                    println!("deleted connection {id}");
                }
                Ok(())
            }
            RemoteCmd::Group(GroupCmd::Create {
                group_id,
                total_records,
                payload_size,
                max_buffer_records,
                max_buffer_bytes,
                batch_max_records,
                batch_timeout_ms,
                ordering_contract,
                connection_id,
                source_json,
            }) => {
                let source_spec = match source_json {
                    Some(raw) => Some(serde_json::from_str::<serde_json::Value>(&raw).map_err(
                        |e| {
                            eprintln!("error: invalid --source-json: {e}");
                            ExitCode::from(2)
                        },
                    )?),
                    None => None,
                };
                // Adapter groups take neither synthetic field; the server
                // rejects non-zero values for them.
                let synthetic = connection_id.is_none();
                let body = GroupCreateRequest {
                    group_id,
                    total_records: total_records.unwrap_or(if synthetic { 100 } else { 0 }),
                    payload_size: payload_size.unwrap_or(if synthetic { 64 } else { 0 }),
                    max_buffer_records,
                    max_buffer_bytes,
                    batch_max_records,
                    batch_timeout_ms,
                    ordering_contract: ordering_contract.unwrap_or_default(),
                    connection_id,
                    source_spec,
                };
                let v = self.post_json("/v1/groups", &body).await?;
                self.print_value(&v, |v| {
                    println!(
                        "group {} created running={}",
                        v["group_id"].as_str().unwrap_or("?"),
                        v["running"]
                    );
                });
                Ok(())
            }
            RemoteCmd::Group(GroupCmd::List) => {
                let v = self.get_json("/v1/groups").await?;
                self.print_value(&v, |v| {
                    if let Some(arr) = v.as_array() {
                        for g in arr {
                            println!(
                                "{}\trunning={}\tlifecycle={}",
                                g["group_id"].as_str().unwrap_or("?"),
                                g["running"],
                                text(&g["lifecycle"])
                            );
                        }
                    }
                });
                Ok(())
            }
            RemoteCmd::Group(GroupCmd::Show { id }) => {
                let v = self.get_json(&format!("/v1/groups/{id}")).await?;
                self.print_value(&v, |v| {
                    println!(
                        "group={} running={} lifecycle={} records={}",
                        v["group_id"].as_str().unwrap_or("?"),
                        v["running"],
                        text(&v["lifecycle"]),
                        v["total_records"]
                    );
                });
                Ok(())
            }
            RemoteCmd::Group(GroupCmd::Delete { id }) => {
                self.delete(&format!("/v1/groups/{id}")).await?;
                if matches!(self.output, OutputFormat::Json) {
                    println!("{{\"ok\":true}}");
                } else {
                    println!("deleted group {id}");
                }
                Ok(())
            }
            RemoteCmd::Group(GroupCmd::Start { id }) => self.group_action(&id, "start").await,
            RemoteCmd::Group(GroupCmd::Pause { id }) => self.group_action(&id, "pause").await,
            RemoteCmd::Group(GroupCmd::Resume { id }) => self.group_action(&id, "resume").await,
            RemoteCmd::Group(GroupCmd::Drain { id }) => self.group_action(&id, "drain").await,
            RemoteCmd::Group(GroupCmd::Diagnostics { id }) => {
                let v = self
                    .get_json(&format!("/v1/groups/{id}/diagnostics"))
                    .await?;
                self.print_value(&v, |v| {
                    let reason = v["last_stop_reason"].as_str().unwrap_or("");
                    println!(
                        "group={} running={} lifecycle={} lag={} fetched={} acked={} replayed={} disconnects={} restarts={} recovered={} reason={}",
                        v["group_id"].as_str().unwrap_or(&id),
                        v["running"],
                        text(&v["lifecycle"]),
                        v["checkpoint_lag"],
                        v["records_fetched"],
                        v["records_acked"],
                        v["records_replayed"],
                        v["consumer_disconnects"],
                        v["restarts"],
                        v["recovered"],
                        reason,
                    );
                });
                Ok(())
            }
            RemoteCmd::Consumer(ConsumerCmd::List { group }) => {
                let v = self
                    .get_json(&format!("/v1/groups/{group}/consumers"))
                    .await?;
                self.print_value(&v, |v| {
                    if let Some(arr) = v["consumers"].as_array() {
                        for c in arr {
                            println!("{}", c.as_str().unwrap_or("?"));
                        }
                    }
                });
                Ok(())
            }
            RemoteCmd::Checkpoint(CheckpointCmd::Show { group }) => {
                let v = self
                    .get_json(&format!("/v1/groups/{group}/checkpoint"))
                    .await?;
                self.print_value(&v, |v| {
                    println!(
                        "group={} durable={} live={}",
                        v["group_id"].as_str().unwrap_or("?"),
                        text(&v["durable_cursor"]),
                        text(&v["live_cursor"])
                    );
                });
                Ok(())
            }
            RemoteCmd::Store(StoreCmd::Backup { path }) => {
                let v = self
                    .post_json("/v1/store/backup", &BackupRequest { path })
                    .await?;
                self.print_value(&v, |v| {
                    println!(
                        "backup written to {} ({} connections, {} groups)",
                        text(&v["path"]),
                        v["connections"],
                        v["groups"]
                    );
                });
                Ok(())
            }
        }
    }

    async fn group_action(&self, id: &str, action: &str) -> Result<(), ExitCode> {
        let v = self
            .post_json(&format!("/v1/groups/{id}/{action}"), &serde_json::json!({}))
            .await?;
        self.print_value(&v, |v| {
            println!(
                "group {} {} running={}",
                v["group_id"].as_str().unwrap_or(id),
                action,
                v["running"]
            );
        });
        Ok(())
    }

    fn print_value(&self, v: &serde_json::Value, text: impl FnOnce(&serde_json::Value)) {
        match self.output {
            OutputFormat::Json => println!("{}", serde_json::to_string_pretty(v).unwrap()),
            OutputFormat::Text => text(v),
        }
    }

    async fn get_json(&self, path: &str) -> Result<serde_json::Value, ExitCode> {
        let resp = self
            .http
            .get(format!("{}{path}", self.base))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| {
                eprintln!("error: {e}");
                ExitCode::FAILURE
            })?;
        self.json_or_err(resp).await
    }

    async fn post_json(
        &self,
        path: &str,
        body: &impl serde::Serialize,
    ) -> Result<serde_json::Value, ExitCode> {
        let resp = self
            .http
            .post(format!("{}{path}", self.base))
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await
            .map_err(|e| {
                eprintln!("error: {e}");
                ExitCode::FAILURE
            })?;
        self.json_or_err(resp).await
    }

    async fn delete(&self, path: &str) -> Result<(), ExitCode> {
        let resp = self
            .http
            .delete(format!("{}{path}", self.base))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| {
                eprintln!("error: {e}");
                ExitCode::FAILURE
            })?;
        if resp.status().is_success() {
            Ok(())
        } else {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            eprintln!("error: HTTP {status}: {text}");
            Err(ExitCode::from(1))
        }
    }

    async fn json_or_err(&self, resp: reqwest::Response) -> Result<serde_json::Value, ExitCode> {
        let status = resp.status();
        let text = resp.text().await.map_err(|e| {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        })?;
        if !status.is_success() {
            eprintln!("error: HTTP {status}: {text}");
            return Err(ExitCode::from(1));
        }
        if text.is_empty() {
            return Ok(serde_json::json!({"ok": true}));
        }
        serde_json::from_str(&text).map_err(|e| {
            eprintln!("error: invalid json: {e}: {text}");
            ExitCode::FAILURE
        })
    }
}

/// A JSON value for text output: strings without quotes, other values as JSON.
fn text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.clone(),
        serde_json::Value::Null => "-".into(),
        other => other.to_string(),
    }
}
