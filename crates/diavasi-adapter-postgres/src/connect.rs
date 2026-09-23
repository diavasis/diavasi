use std::time::Duration;

use tokio_postgres::NoTls;
use tokio_postgres::{Client, Config};

use diavasi::runtime::SourceOpen;

#[derive(Clone)]
pub struct PgEndpoint {
    pub config: Config,
    pub tls: bool,
}

impl PgEndpoint {
    pub fn from_request(request: &SourceOpen) -> Result<Self, String> {
        let cfg = &request.connection.config_json;
        let host = cfg
            .get("host")
            .and_then(|v| v.as_str())
            .unwrap_or("localhost");
        let port = cfg.get("port").and_then(|v| v.as_u64()).unwrap_or(5432);
        let port = u16::try_from(port).map_err(|_| "port out of range")?;
        let dbname = cfg
            .get("dbname")
            .and_then(|v| v.as_str())
            .ok_or("config_json.dbname is required")?;
        let user = cfg
            .get("user")
            .and_then(|v| v.as_str())
            .ok_or("config_json.user is required")?;
        let sslmode = cfg
            .get("sslmode")
            .and_then(|v| v.as_str())
            .unwrap_or("disable");
        let tls = match sslmode {
            "disable" => false,
            "require" => true,
            other => return Err(format!("sslmode {other} is not supported")),
        };
        let password = String::from_utf8(request.secret.clone())
            .map_err(|_| "secret must be a utf-8 password")?;
        let mut config = Config::new();
        config.host(host);
        config.port(port);
        config.dbname(dbname);
        config.user(user);
        config.password(password);
        config.connect_timeout(Duration::from_secs(5));
        Ok(Self { config, tls })
    }

    pub fn from_database_url(url: &str) -> Result<(Self, String), String> {
        let config: Config = url.parse().map_err(|err| format!("DATABASE_URL: {err}"))?;
        let password = config
            .get_password()
            .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
            .filter(|password| !password.is_empty())
            .ok_or("DATABASE_URL must include a password")?;
        Ok((Self { config, tls: false }, password))
    }

    pub fn config_json(&self) -> serde_json::Value {
        let host = match self.config.get_hosts().first() {
            Some(tokio_postgres::config::Host::Tcp(host)) => host.clone(),
            _ => "localhost".into(),
        };
        serde_json::json!({
            "host": host,
            "port": self.config.get_ports().first().copied().unwrap_or(5432),
            "dbname": self.config.get_dbname().unwrap_or("postgres"),
            "user": self.config.get_user().unwrap_or("postgres"),
            "sslmode": if self.tls { "require" } else { "disable" },
        })
    }
}

fn connect_err(err: tokio_postgres::Error) -> String {
    let mut message = err.to_string();
    let mut source = std::error::Error::source(&err);
    while let Some(inner) = source {
        message.push_str(": ");
        message.push_str(&inner.to_string());
        source = std::error::Error::source(inner);
    }
    message
}

pub async fn connect(endpoint: &PgEndpoint) -> Result<Client, String> {
    if endpoint.tls {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = tokio_postgres_rustls::MakeRustlsConnect::new(tls);
        let (client, connection) = endpoint
            .config
            .connect(connector)
            .await
            .map_err(connect_err)?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::debug!("postgres tls connection ended: {err}");
            }
        });
        Ok(client)
    } else {
        let (client, connection) = endpoint.config.connect(NoTls).await.map_err(connect_err)?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::debug!("postgres connection ended: {err}");
            }
        });
        Ok(client)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use diavasi::runtime::SourceOpen;
    use diavasi::store::{ConnectionRecord, SealedSecret};

    fn expect_err(result: Result<PgEndpoint, String>) -> String {
        match result {
            Ok(_) => panic!("expected an error"),
            Err(err) => err,
        }
    }

    fn request(config: serde_json::Value, secret: Vec<u8>) -> SourceOpen {
        SourceOpen {
            connection: ConnectionRecord {
                id: "pg".into(),
                kind: "postgres".into(),
                config_json: config,
                sealed_secret: SealedSecret {
                    nonce: Vec::new(),
                    ciphertext: Vec::new(),
                },
            },
            source_spec: serde_json::json!({}),
            secret,
        }
    }

    #[test]
    fn from_request_accepts_disable_and_require() {
        let plain = PgEndpoint::from_request(&request(
            serde_json::json!({
                "dbname": "diavasi",
                "user": "diavasi",
            }),
            b"secret".to_vec(),
        ))
        .unwrap();
        assert!(!plain.tls);
        assert_eq!(plain.config.get_hosts().len(), 1);
        assert_eq!(plain.config.get_ports(), &[5432]);

        let tls = PgEndpoint::from_request(&request(
            serde_json::json!({
                "host": "db.example",
                "port": 5433,
                "dbname": "diavasi",
                "user": "diavasi",
                "sslmode": "require",
            }),
            b"secret".to_vec(),
        ))
        .unwrap();
        assert!(tls.tls);
        let json = tls.config_json();
        assert_eq!(json["host"], "db.example");
        assert_eq!(json["port"], 5433);
        assert_eq!(json["sslmode"], "require");
    }

    #[test]
    fn from_request_rejects_bad_config() {
        let base = serde_json::json!({"dbname": "diavasi", "user": "diavasi"});
        assert!(expect_err(PgEndpoint::from_request(&request(base, vec![0xff]))).contains("utf-8"));
        assert!(
            expect_err(PgEndpoint::from_request(&request(
                serde_json::json!({"user": "diavasi"}),
                b"secret".to_vec()
            )))
            .contains("dbname")
        );
        assert!(
            expect_err(PgEndpoint::from_request(&request(
                serde_json::json!({"dbname": "diavasi"}),
                b"secret".to_vec()
            )))
            .contains("user")
        );
        assert!(
            expect_err(PgEndpoint::from_request(&request(
                serde_json::json!({
                    "dbname": "diavasi",
                    "user": "diavasi",
                    "port": 70_000,
                }),
                b"secret".to_vec()
            )))
            .contains("port")
        );
        assert!(
            expect_err(PgEndpoint::from_request(&request(
                serde_json::json!({
                    "dbname": "diavasi",
                    "user": "diavasi",
                    "sslmode": "verify-full",
                }),
                b"secret".to_vec()
            )))
            .contains("sslmode")
        );
    }

    #[test]
    fn database_url_and_config_json_cover_defaults() {
        assert!(PgEndpoint::from_database_url("not a url").is_err());
        match PgEndpoint::from_database_url("postgres://diavasi@127.0.0.1/diavasi") {
            Ok(_) => panic!("expected a missing password"),
            Err(err) => assert!(err.contains("password")),
        }
        let (endpoint, password) =
            PgEndpoint::from_database_url("postgres://diavasi:secret@127.0.0.1:5432/diavasi")
                .unwrap();
        assert_eq!(password, "secret");
        let json = endpoint.config_json();
        assert_eq!(json["sslmode"], "disable");
        assert_eq!(json["dbname"], "diavasi");

        let mut config = Config::new();
        config.user("diavasi");
        config.password("secret");
        let fallback = PgEndpoint { config, tls: false }.config_json();
        assert_eq!(fallback["host"], "localhost");
        assert_eq!(fallback["port"], 5432);
        assert_eq!(fallback["dbname"], "postgres");
    }

    #[tokio::test]
    async fn tls_connect_to_a_closed_port_fails() {
        let mut config = Config::new();
        config.host("127.0.0.1");
        config.port(1);
        config.dbname("diavasi");
        config.user("diavasi");
        config.password("diavasi");
        config.connect_timeout(Duration::from_millis(200));
        let err = connect(&PgEndpoint { config, tls: true })
            .await
            .unwrap_err();
        assert!(!err.is_empty());
    }
}
