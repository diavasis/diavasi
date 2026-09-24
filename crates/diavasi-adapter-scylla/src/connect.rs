//! One Scylla session per running group.

use std::sync::Arc;
use std::time::Duration;

use diavasi::runtime::SourceOpen;
use rustls::RootCertStore;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::batch::{Batch, BatchType};

#[derive(Clone, Debug)]
pub struct ScyllaEndpoint {
    pub host: String,
    pub port: u16,
    pub keyspace: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub tls: bool,
    pub connect_timeout: Duration,
}

impl ScyllaEndpoint {
    pub fn from_request(request: &SourceOpen) -> Result<Self, String> {
        let cfg = &request.connection.config_json;
        let host = cfg
            .get("host")
            .and_then(|v| v.as_str())
            .unwrap_or("127.0.0.1");
        let port = cfg.get("port").and_then(|v| v.as_u64()).unwrap_or(9042);
        let port = u16::try_from(port).map_err(|_| "port out of range")?;
        let keyspace = match cfg.get("keyspace") {
            None => None,
            Some(value) => Some(
                value
                    .as_str()
                    .ok_or("config_json.keyspace must be a string")?
                    .to_string(),
            ),
        };
        let username = match cfg.get("username") {
            None => None,
            Some(value) => Some(
                value
                    .as_str()
                    .ok_or("config_json.username must be a string")?
                    .to_string(),
            ),
        };
        let tls = match cfg.get("tls").and_then(|v| v.as_str()).unwrap_or("disable") {
            "disable" => false,
            "require" => true,
            other => return Err(format!("tls {other} is not supported")),
        };
        let secret =
            String::from_utf8(request.secret.clone()).map_err(|_| "secret must be utf-8")?;
        let password = if username.is_some() {
            if secret.is_empty() {
                return Err("username requires a password secret".into());
            }
            Some(secret)
        } else {
            None
        };
        Ok(Self {
            host: host.to_string(),
            port,
            keyspace,
            username,
            password,
            tls,
            connect_timeout: Duration::from_secs(5),
        })
    }

    pub fn from_url(url: &str) -> Result<Self, String> {
        let raw = url.strip_prefix("scylla://").unwrap_or(url);
        let (hostport, keyspace) = match raw.split_once('/') {
            Some((hostport, keyspace)) if !keyspace.is_empty() => {
                (hostport, Some(keyspace.to_string()))
            }
            _ => (raw, None),
        };
        let (host, port) = split_host_port(hostport)?;
        Ok(Self {
            host,
            port,
            keyspace,
            username: None,
            password: None,
            tls: false,
            connect_timeout: Duration::from_secs(5),
        })
    }

    pub fn config_json(&self) -> serde_json::Value {
        let mut cfg = serde_json::json!({
            "host": self.host,
            "port": self.port,
            "tls": if self.tls { "require" } else { "disable" },
        });
        if let Some(keyspace) = &self.keyspace {
            cfg["keyspace"] = serde_json::Value::from(keyspace.clone());
        }
        if let Some(username) = &self.username {
            cfg["username"] = serde_json::Value::from(username.clone());
        }
        cfg
    }

    pub fn secret(&self) -> String {
        self.password.clone().unwrap_or_default()
    }
}

fn split_host_port(hostport: &str) -> Result<(String, u16), String> {
    let (host, port) = hostport
        .rsplit_once(':')
        .ok_or("SCYLLA_URL must be host:port")?;
    if host.is_empty() {
        return Err("SCYLLA_URL must be host:port".into());
    }
    let port = port
        .parse::<u16>()
        .map_err(|_| "SCYLLA_URL port is not a number")?;
    Ok((host.to_string(), port))
}

pub async fn connect(endpoint: &ScyllaEndpoint) -> Result<Session, String> {
    let mut builder = SessionBuilder::new()
        .known_node(format!("{}:{}", endpoint.host, endpoint.port))
        .connection_timeout(endpoint.connect_timeout);
    if let (Some(username), Some(password)) = (&endpoint.username, &endpoint.password) {
        builder = builder.user(username, password);
    }
    if endpoint.tls {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        builder = builder.tls_context(Some(Arc::new(config)));
    }
    builder.build().await.map_err(|err| err.to_string())
}

pub async fn execute(session: &Session, cql: &str) -> Result<(), String> {
    session
        .query_unpaged(cql, ())
        .await
        .map(|_| ())
        .map_err(|err| err.to_string())
}

/// Insert `records` rows into one partition (`bucket = 0`), clustering `id` from 1, payload `body`.
pub async fn seed_bucket(
    session: &Session,
    keyspace: &str,
    table: &str,
    records: u64,
    payload: &str,
) -> Result<(), String> {
    let statement = session
        .prepare(format!(
            "INSERT INTO \"{keyspace}\".\"{table}\" (bucket, id, body) VALUES (?, ?, ?)"
        ))
        .await
        .map_err(|err| err.to_string())?;
    let total = i64::try_from(records).map_err(|_| "records does not fit in i64")?;
    let mut id = 1i64;
    while id <= total {
        let end = (id + 49).min(total);
        let mut batch = Batch::new(BatchType::Unlogged);
        let mut values = Vec::new();
        while id <= end {
            batch.append_statement(statement.clone());
            values.push((0i32, id, payload));
            id += 1;
        }
        session
            .batch(&batch, values)
            .await
            .map_err(|err| err.to_string())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_keeps_the_host_port() {
        let endpoint = ScyllaEndpoint::from_url("127.0.0.1:9042").unwrap();
        assert_eq!(endpoint.host, "127.0.0.1");
        assert_eq!(endpoint.port, 9042);
        assert!(endpoint.secret().is_empty());
        assert!(endpoint.config_json().get("username").is_none());
    }

    #[test]
    fn username_makes_the_secret_the_password() {
        let request = SourceOpen {
            connection: diavasi::store::ConnectionRecord {
                id: "s".into(),
                kind: "scylla".into(),
                config_json: serde_json::json!({
                    "host": "db.example",
                    "username": "app",
                    "tls": "require",
                }),
                sealed_secret: diavasi::store::SealedSecret {
                    nonce: Vec::new(),
                    ciphertext: Vec::new(),
                },
            },
            source_spec: serde_json::json!({}),
            secret: b"pw".to_vec(),
        };
        let endpoint = ScyllaEndpoint::from_request(&request).unwrap();
        assert_eq!(endpoint.secret(), "pw");
        assert!(endpoint.tls);
        assert_eq!(endpoint.config_json()["username"], "app");
    }
}
