use std::time::Duration;

use tokio_postgres::NoTls;
use tokio_postgres::{Client, Config};

use diavasi::runtime::SourceOpen;

/// How to reach one database. The sealed secret is the password.
#[derive(Clone)]
pub struct PgEndpoint {
    /// Host, port, database, user, and password.
    pub config: Config,
    /// Connect with TLS and verify the server certificate.
    pub tls: bool,
    /// PEM CA certificates that sign the server certificate. `None` trusts
    /// the public web roots.
    pub ca_pem: Option<String>,
}

impl PgEndpoint {
    /// The endpoint of a stored connection: `config_json` plus the opened secret as password. Unknown `config_json` keys are an error.
    pub fn from_request(request: &SourceOpen) -> Result<Self, String> {
        let cfg = &request.connection.config_json;
        diavasi::runtime::check_keys(
            cfg,
            &["host", "port", "dbname", "user", "sslmode", "ca_pem"],
            "config_json",
        )?;
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
        // `require` verifies the certificate, as `verify-full` does. That is
        // stricter than libpq, whose `require` skips verification.
        let tls = match sslmode {
            "disable" => false,
            "require" | "verify-full" => true,
            other => return Err(format!("sslmode {other} is not supported")),
        };
        let ca_pem = match cfg.get("ca_pem") {
            None | Some(serde_json::Value::Null) => None,
            Some(serde_json::Value::String(pem)) if tls => Some(pem.clone()),
            Some(serde_json::Value::String(_)) => {
                return Err("config_json.ca_pem needs sslmode require or verify-full".into());
            }
            Some(_) => return Err("config_json.ca_pem must be a PEM string".into()),
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
        Ok(Self {
            config,
            tls,
            ca_pem,
        })
    }

    /// The endpoint and password of a `postgres://` URL. The URL must carry a
    /// password; `sslmode=require` turns on TLS.
    ///
    /// ```
    /// use diavasi_adapter_postgres::connect::PgEndpoint;
    /// let (endpoint, password) =
    ///     PgEndpoint::from_database_url("postgres://app:s3cret@db.internal:5432/app?sslmode=require")?;
    /// assert!(endpoint.tls);
    /// assert_eq!(password, "s3cret");
    /// assert_eq!(endpoint.config_json()["dbname"], "app");
    /// # Ok::<(), String>(())
    /// ```
    pub fn from_database_url(url: &str) -> Result<(Self, String), String> {
        let config: Config = url.parse().map_err(|err| format!("DATABASE_URL: {err}"))?;
        let password = config
            .get_password()
            .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
            .filter(|password| !password.is_empty())
            .ok_or("DATABASE_URL must include a password")?;
        let tls = matches!(
            config.get_ssl_mode(),
            tokio_postgres::config::SslMode::Require
        );
        Ok((
            Self {
                config,
                tls,
                ca_pem: None,
            },
            password,
        ))
    }

    /// The `config_json` of a connection to this endpoint, without the password.
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

/// The error and every error it wraps, joined with `: `.
pub(crate) fn error_chain(err: &tokio_postgres::Error) -> String {
    let mut message = err.to_string();
    let mut source = std::error::Error::source(err);
    while let Some(inner) = source {
        message.push_str(": ");
        message.push_str(&inner.to_string());
        source = std::error::Error::source(inner);
    }
    message
}

/// Open a connection. With TLS, the server certificate is verified against `ca_pem` or the public roots.
pub async fn connect(endpoint: &PgEndpoint) -> Result<Client, String> {
    if endpoint.tls {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut roots = rustls::RootCertStore::empty();
        match &endpoint.ca_pem {
            Some(pem) => {
                use rustls::pki_types::CertificateDer;
                use rustls::pki_types::pem::PemObject;

                let mut added = 0;
                for cert in CertificateDer::pem_slice_iter(pem.as_bytes()) {
                    let cert = cert.map_err(|err| format!("config_json.ca_pem: {err}"))?;
                    roots
                        .add(cert)
                        .map_err(|err| format!("config_json.ca_pem: {err}"))?;
                    added += 1;
                }
                if added == 0 {
                    return Err("config_json.ca_pem holds no certificate".into());
                }
            }
            None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
        }
        let tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = tokio_postgres_rustls::MakeRustlsConnect::new(tls);
        let (client, connection) = endpoint
            .config
            .connect(connector)
            .await
            .map_err(|err| error_chain(&err))?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::debug!("postgres tls connection ended: {err}");
            }
        });
        Ok(client)
    } else {
        let (client, connection) = endpoint
            .config
            .connect(NoTls)
            .await
            .map_err(|err| error_chain(&err))?;
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
                    "sslmode": "verify-ca",
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
        let fallback = PgEndpoint {
            config,
            tls: false,
            ca_pem: None,
        }
        .config_json();
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
        let err = connect(&PgEndpoint {
            config,
            tls: true,
            ca_pem: None,
        })
        .await
        .unwrap_err();
        let lower = err.to_lowercase();
        assert!(
            lower.contains("connection refused") || lower.contains("error connecting"),
            "{err}"
        );
    }

    /// B22: `verify-full` is accepted, `ca_pem` needs TLS, and a CA that
    /// holds no certificate fails the connect.
    #[tokio::test]
    async fn tls_modes_and_ca_pem() {
        let base = serde_json::json!({"dbname": "diavasi", "user": "diavasi"});
        let with = |extra: serde_json::Value| {
            let mut cfg = base.clone();
            for (key, value) in extra.as_object().unwrap() {
                cfg[key] = value.clone();
            }
            PgEndpoint::from_request(&request(cfg, b"secret".to_vec()))
        };
        assert!(
            with(serde_json::json!({"sslmode": "verify-full"}))
                .unwrap()
                .tls
        );
        assert!(
            expect_err(with(serde_json::json!({"ca_pem": "x"}))).contains("sslmode"),
            "ca_pem without TLS"
        );
        assert!(expect_err(with(serde_json::json!({"passwrd": "x"}))).contains("passwrd"));
        let endpoint = with(serde_json::json!({
            "host": "127.0.0.1",
            "port": 1,
            "sslmode": "require",
            "ca_pem": "not a certificate",
        }))
        .unwrap();
        let err = connect(&endpoint).await.unwrap_err();
        assert!(err.contains("no certificate"), "{err}");
    }

    #[test]
    fn database_url_sslmode_require_turns_on_tls() {
        let (endpoint, _) = PgEndpoint::from_database_url(
            "postgres://diavasi:secret@127.0.0.1:5432/diavasi?sslmode=require",
        )
        .unwrap();
        assert!(endpoint.tls);
    }
}
