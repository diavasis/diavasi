use std::time::Duration;

use diavasi::runtime::SourceOpen;
use mongodb::Client;
use mongodb::options::{ClientOptions, Credential, Tls, TlsOptions};

/// How to reach one database. The sealed secret is the password when `user` is set.
#[derive(Clone, Debug)]
pub struct MongoEndpoint {
    /// Server host.
    pub host: String,
    /// Server port. Default 27017.
    pub port: u16,
    /// Database that holds the collection.
    pub database: String,
    /// User to authenticate as. `None` connects without credentials.
    pub user: Option<String>,
    /// Password, when `user` is set.
    pub password: Option<String>,
    /// Database that holds the user. Default `admin`.
    pub auth_source: String,
    /// Connect with TLS.
    pub tls: bool,
    /// Name the server shows for this client.
    pub app_name: String,
    /// How long to wait for the server.
    pub server_selection_timeout: Duration,
}

impl MongoEndpoint {
    /// The endpoint of a stored connection. Unknown `config_json` keys are an error.
    pub fn from_request(request: &SourceOpen) -> Result<Self, String> {
        let cfg = &request.connection.config_json;
        diavasi::runtime::check_keys(
            cfg,
            &["host", "port", "database", "user", "auth_source", "tls"],
            "config_json",
        )?;
        let host = cfg
            .get("host")
            .and_then(|v| v.as_str())
            .filter(|host| !host.is_empty())
            .ok_or("config_json.host is required")?
            .to_string();
        let port = cfg.get("port").and_then(|v| v.as_u64()).unwrap_or(27017);
        let port = u16::try_from(port).map_err(|_| "port out of range")?;
        let database = cfg
            .get("database")
            .and_then(|v| v.as_str())
            .filter(|name| !name.is_empty())
            .ok_or("config_json.database is required")?
            .to_string();
        let auth_source = cfg
            .get("auth_source")
            .and_then(|v| v.as_str())
            .filter(|name| !name.is_empty())
            .unwrap_or("admin")
            .to_string();
        let tls = match cfg.get("tls").and_then(|v| v.as_str()).unwrap_or("disable") {
            "disable" => false,
            "require" => true,
            other => return Err(format!("tls {other} is not supported")),
        };
        let user = cfg
            .get("user")
            .and_then(|v| v.as_str())
            .filter(|user| !user.is_empty())
            .map(str::to_string);
        let password = if user.is_some() {
            Some(
                String::from_utf8(request.secret.clone())
                    .map_err(|_| "secret must be a utf-8 password")?,
            )
        } else {
            None
        };
        Ok(Self {
            host,
            port,
            database,
            user,
            password,
            auth_source,
            tls,
            app_name: "diavasi".into(),
            server_selection_timeout: Duration::from_secs(5),
        })
    }

    /// The endpoint of a `mongodb://` URL with one host.
    ///
    /// ```
    /// use diavasi_adapter_mongodb::connect::MongoEndpoint;
    /// let endpoint = MongoEndpoint::from_url("mongodb://app:s3cret@db.internal:27018/shop?tls=true")?;
    /// assert_eq!((endpoint.port, endpoint.database.as_str()), (27018, "shop"));
    /// assert!(endpoint.tls);
    /// # Ok::<(), String>(())
    /// ```
    pub fn from_url(url: &str) -> Result<Self, String> {
        let rest = url
            .strip_prefix("mongodb://")
            .ok_or("MONGODB_URL must start with mongodb://")?;
        let (rest, query) = rest
            .split_once('?')
            .map(|(left, right)| (left, Some(right)))
            .unwrap_or((rest, None));
        let (auth, host_and_db) = match rest.split_once('@') {
            Some((auth, rest)) => (Some(auth), rest),
            None => (None, rest),
        };
        let (hostport, database) = match host_and_db.split_once('/') {
            Some((hostport, database)) => {
                let database = database.split('/').next().unwrap_or("");
                (
                    hostport,
                    if database.is_empty() {
                        None
                    } else {
                        Some(database.to_string())
                    },
                )
            }
            None => (host_and_db, None),
        };
        let (host, port) = split_host_port(hostport)?;
        let (user, password) = match auth {
            Some(auth) => {
                let (user, password) = auth
                    .split_once(':')
                    .ok_or("MONGODB_URL credentials must be user:password")?;
                (Some(percent_decode(user)?), Some(percent_decode(password)?))
            }
            None => (None, None),
        };
        let mut auth_source = "admin".to_string();
        let mut tls = false;
        if let Some(query) = query {
            for pair in query.split('&') {
                if pair.is_empty() {
                    continue;
                }
                let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
                match key {
                    "authSource" => auth_source = percent_decode(value)?,
                    "tls" | "ssl" if value == "true" => tls = true,
                    _ => {}
                }
            }
        }
        Ok(Self {
            host,
            port,
            database: database.unwrap_or_else(|| "diavasi".into()),
            user,
            password,
            auth_source,
            tls,
            app_name: "diavasi".into(),
            server_selection_timeout: Duration::from_secs(5),
        })
    }

    /// The `config_json` of a connection to this endpoint, without the password.
    pub fn config_json(&self) -> serde_json::Value {
        let mut json = serde_json::json!({
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "auth_source": self.auth_source,
            "tls": if self.tls { "require" } else { "disable" },
        });
        if let Some(user) = &self.user {
            json["user"] = serde_json::Value::String(user.clone());
        }
        json
    }

    /// The password, or an empty string.
    pub fn secret(&self) -> String {
        self.password.clone().unwrap_or_default()
    }
}

fn split_host_port(hostport: &str) -> Result<(String, u16), String> {
    if hostport.is_empty() {
        return Err("MONGODB_URL is missing a host".into());
    }
    if let Some(rest) = hostport.strip_prefix('[') {
        let (host, after) = rest.split_once(']').ok_or("bad ipv6 host")?;
        let port = after.strip_prefix(':').filter(|port| !port.is_empty());
        let port = match port {
            Some(port) => parse_port(port)?,
            None => 27017,
        };
        return Ok((host.to_string(), port));
    }
    if let Some((host, port)) = hostport.rsplit_once(':') {
        if !host.is_empty() && port.chars().all(|ch| ch.is_ascii_digit()) {
            return Ok((host.to_string(), parse_port(port)?));
        }
    }
    Ok((hostport.to_string(), 27017))
}

fn parse_port(raw: &str) -> Result<u16, String> {
    raw.parse::<u16>().map_err(|_| format!("bad port {raw}"))
}

fn percent_decode(raw: &str) -> Result<String, String> {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err("bad percent-encoding".into());
            }
            let hex = std::str::from_utf8(&bytes[index + 1..index + 3])
                .map_err(|_| "bad percent-encoding")?;
            let byte = u8::from_str_radix(hex, 16).map_err(|_| "bad percent-encoding")?;
            out.push(byte);
            index += 3;
        } else {
            out.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(out).map_err(|_| "percent-decoded text is not utf-8".into())
}

/// A client for the endpoint. The driver connects on first use.
pub async fn connect(endpoint: &MongoEndpoint) -> Result<Client, String> {
    let uri = format!("mongodb://{}:{}", endpoint.host, endpoint.port);
    let mut options = ClientOptions::parse(uri)
        .await
        .map_err(|err| err.to_string())?;
    if let Some(user) = &endpoint.user {
        options.credential = Some(
            Credential::builder()
                .username(user.clone())
                .password(endpoint.password.clone().unwrap_or_default())
                .source(endpoint.auth_source.clone())
                .build(),
        );
    }
    options.direct_connection = Some(true);
    options.server_selection_timeout = Some(endpoint.server_selection_timeout);
    options.connect_timeout = Some(endpoint.server_selection_timeout);
    options.app_name = Some(endpoint.app_name.clone());
    options.tls = Some(if endpoint.tls {
        Tls::Enabled(TlsOptions::builder().build())
    } else {
        Tls::Disabled
    });
    Client::with_options(options).map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_without_credentials() {
        let endpoint = MongoEndpoint::from_url("mongodb://127.0.0.1:27017").unwrap();
        assert_eq!(endpoint.host, "127.0.0.1");
        assert_eq!(endpoint.port, 27017);
        assert!(endpoint.user.is_none());
        assert!(!endpoint.tls);
    }

    #[test]
    fn url_with_user_database_and_tls() {
        let endpoint =
            MongoEndpoint::from_url("mongodb://diavasi:s3cret@db.example:27018/app?tls=true")
                .unwrap();
        assert_eq!(endpoint.user.as_deref(), Some("diavasi"));
        assert_eq!(endpoint.password.as_deref(), Some("s3cret"));
        assert_eq!(endpoint.database, "app");
        assert_eq!(endpoint.port, 27018);
        assert!(endpoint.tls);
        let json = endpoint.config_json();
        assert_eq!(json["tls"], "require");
        assert!(json.get("password").is_none());
    }
}
