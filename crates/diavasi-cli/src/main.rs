use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use diavasi::control::{API_TOKEN_ENV, ServeConfig, serve};
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
    #[arg(long, global = true, env = "DIAVASI_API_TOKEN")]
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

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the local control-plane HTTP server.
    Serve {
        #[arg(long, default_value = "127.0.0.1:7700")]
        bind: SocketAddr,
        #[arg(long)]
        store: PathBuf,
        #[arg(long, env = "DIAVASI_API_TOKEN")]
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
    },
    /// Print library version.
    Version,
    /// Server status.
    Status,
    #[command(subcommand)]
    Connection(ConnectionCmd),
    #[command(subcommand)]
    Group(GroupCmd),
    #[command(subcommand)]
    Consumer(ConsumerCmd),
    #[command(subcommand)]
    Checkpoint(CheckpointCmd),
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
        #[arg(long)]
        secret: String,
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
        #[arg(long, default_value_t = 100)]
        total_records: u64,
        #[arg(long, default_value_t = 64)]
        payload_size: usize,
        #[arg(long, default_value_t = 1024)]
        max_buffer_records: usize,
        #[arg(long, default_value_t = 1024 * 1024)]
        max_buffer_bytes: usize,
        #[arg(long, default_value_t = 32)]
        batch_max_records: usize,
        #[arg(long, default_value_t = 100)]
        batch_timeout_ms: u64,
        #[arg(long, default_value = "synthetic-u64")]
        ordering_contract: String,
        #[arg(long)]
        connection_id: Option<String>,
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
}

#[derive(Subcommand, Debug)]
enum ConsumerCmd {
    List { group: String },
}

#[derive(Subcommand, Debug)]
enum CheckpointCmd {
    Show { group: String },
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(code) => code,
    }
}

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
        } => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "info".into()),
                )
                .init();
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
            })
            .await
            {
                eprintln!("error: {e}");
                return Err(ExitCode::FAILURE);
            }
            Ok(())
        }
        other => {
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
            client.dispatch(other).await
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
    async fn dispatch(&self, cmd: Commands) -> Result<(), ExitCode> {
        match cmd {
            Commands::Status => {
                let v = self.get_json("/v1/status").await?;
                self.print_value(&v, |v| {
                    println!(
                        "version={} schema={} bind={} running={:?}",
                        v["version"].as_str().unwrap_or("?"),
                        v["schema_version"],
                        v["bind"].as_str().unwrap_or("?"),
                        v["running_groups"]
                    );
                });
                Ok(())
            }
            Commands::Connection(ConnectionCmd::Add {
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
                let body = serde_json::json!({
                    "id": id,
                    "kind": kind,
                    "config_json": config,
                    "secret": secret,
                });
                let v = self.post_json("/v1/connections", &body).await?;
                self.print_value(&v, |v| {
                    println!("connection {} created (secret sealed)", v["id"]);
                });
                Ok(())
            }
            Commands::Connection(ConnectionCmd::List) => {
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
            Commands::Connection(ConnectionCmd::Show { id }) => {
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
            Commands::Connection(ConnectionCmd::Delete { id }) => {
                self.delete(&format!("/v1/connections/{id}")).await?;
                if matches!(self.output, OutputFormat::Json) {
                    println!("{{\"ok\":true}}");
                } else {
                    println!("deleted connection {id}");
                }
                Ok(())
            }
            Commands::Group(GroupCmd::Create {
                group_id,
                total_records,
                payload_size,
                max_buffer_records,
                max_buffer_bytes,
                batch_max_records,
                batch_timeout_ms,
                ordering_contract,
                connection_id,
            }) => {
                let body = serde_json::json!({
                    "group_id": group_id,
                    "total_records": total_records,
                    "payload_size": payload_size,
                    "max_buffer_records": max_buffer_records,
                    "max_buffer_bytes": max_buffer_bytes,
                    "batch_max_records": batch_max_records,
                    "batch_timeout_ms": batch_timeout_ms,
                    "ordering_contract": ordering_contract,
                    "connection_id": connection_id,
                });
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
            Commands::Group(GroupCmd::List) => {
                let v = self.get_json("/v1/groups").await?;
                self.print_value(&v, |v| {
                    if let Some(arr) = v.as_array() {
                        for g in arr {
                            println!(
                                "{}\trunning={}\tlifecycle={:?}",
                                g["group_id"].as_str().unwrap_or("?"),
                                g["running"],
                                g["lifecycle"]
                            );
                        }
                    }
                });
                Ok(())
            }
            Commands::Group(GroupCmd::Show { id }) => {
                let v = self.get_json(&format!("/v1/groups/{id}")).await?;
                self.print_value(&v, |v| {
                    println!(
                        "group={} running={} lifecycle={:?} records={}",
                        v["group_id"].as_str().unwrap_or("?"),
                        v["running"],
                        v["lifecycle"],
                        v["total_records"]
                    );
                });
                Ok(())
            }
            Commands::Group(GroupCmd::Delete { id }) => {
                self.delete(&format!("/v1/groups/{id}")).await?;
                if matches!(self.output, OutputFormat::Json) {
                    println!("{{\"ok\":true}}");
                } else {
                    println!("deleted group {id}");
                }
                Ok(())
            }
            Commands::Group(GroupCmd::Start { id }) => self.group_action(&id, "start").await,
            Commands::Group(GroupCmd::Pause { id }) => self.group_action(&id, "pause").await,
            Commands::Group(GroupCmd::Resume { id }) => self.group_action(&id, "resume").await,
            Commands::Group(GroupCmd::Drain { id }) => self.group_action(&id, "drain").await,
            Commands::Consumer(ConsumerCmd::List { group }) => {
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
            Commands::Checkpoint(CheckpointCmd::Show { group }) => {
                let v = self
                    .get_json(&format!("/v1/groups/{group}/checkpoint"))
                    .await?;
                self.print_value(&v, |v| {
                    println!(
                        "group={} durable={:?} live={:?}",
                        v["group_id"].as_str().unwrap_or("?"),
                        v["durable_cursor"],
                        v["live_cursor"]
                    );
                });
                Ok(())
            }
            Commands::Version | Commands::Serve { .. } => unreachable!(),
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
        body: &serde_json::Value,
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
