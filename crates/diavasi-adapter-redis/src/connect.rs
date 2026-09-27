use std::time::Duration;

use diavasi::runtime::SourceOpen;
use redis::aio::ConnectionManager;
use redis::aio::ConnectionManagerConfig;

/// How to reach one Redis server. The sealed secret is the password when `username` is set.
#[derive(Clone, Debug)]
pub struct RedisEndpoint {
    /// Server host.
    pub host: String,
    /// Server port. Default 6379.
    pub port: u16,
    /// Database number. Default 0.
    pub db: i64,
    /// User to authenticate as (`user` in `config_json`). `None` does not authenticate.
    pub username: Option<String>,
    /// Password, when a user is set.
    pub password: Option<String>,
    /// Connect with TLS.
    pub tls: bool,
    /// How long to wait to connect.
    pub connect_timeout: Duration,
    /// How long to wait for a reply.
    pub response_timeout: Duration,
    /// `None` keeps the client default. `Some(0)` tries once.
    pub retries: Option<usize>,
}

impl RedisEndpoint {
    /// The endpoint of a stored connection. Unknown `config_json` keys are an error.
    pub fn from_request(request: &SourceOpen) -> Result<Self, String> {
        let cfg = &request.connection.config_json;
        diavasi::runtime::check_keys(
            cfg,
            &["host", "port", "db", "user", "username", "tls"],
            "config_json",
        )?;
        let host = cfg
            .get("host")
            .and_then(|value| value.as_str())
            .filter(|host| !host.is_empty())
            .ok_or("config_json.host is required")?
            .to_string();
        let port = cfg
            .get("port")
            .and_then(|value| value.as_u64())
            .unwrap_or(6379);
        let port = u16::try_from(port).map_err(|_| "port out of range")?;
        let db = cfg.get("db").and_then(|value| value.as_i64()).unwrap_or(0);
        if db < 0 {
            return Err("db must be >= 0".into());
        }
        let tls = match cfg
            .get("tls")
            .and_then(|value| value.as_str())
            .unwrap_or("disable")
        {
            "disable" => false,
            "require" => true,
            other => return Err(format!("tls {other} is not supported")),
        };
        // `user` is the name every adapter uses; `username` is accepted from
        // connections created before v0.13.
        let username = cfg
            .get("user")
            .or_else(|| cfg.get("username"))
            .and_then(|value| value.as_str())
            .filter(|user| !user.is_empty())
            .map(str::to_string);
        let password = if username.is_some() {
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
            db,
            username,
            password,
            tls,
            connect_timeout: Duration::from_secs(5),
            response_timeout: Duration::from_secs(5),
            retries: None,
        })
    }

    /// The endpoint of a `redis://` or `rediss://` URL.
    ///
    /// ```
    /// use diavasi_adapter_redis::connect::RedisEndpoint;
    /// let endpoint = RedisEndpoint::from_url("redis://127.0.0.1:6380/2")?;
    /// assert_eq!((endpoint.port, endpoint.db), (6380, 2));
    /// # Ok::<(), String>(())
    /// ```
    pub fn from_url(url: &str) -> Result<Self, String> {
        let (tls, rest) = if let Some(rest) = url.strip_prefix("rediss://") {
            (true, rest)
        } else if let Some(rest) = url.strip_prefix("redis://") {
            (false, rest)
        } else {
            return Err("REDIS_URL must start with redis:// or rediss://".into());
        };
        let (rest, _query) = rest
            .split_once('?')
            .map(|(left, right)| (left, Some(right)))
            .unwrap_or((rest, None));
        let (auth, host_and_db) = match rest.split_once('@') {
            Some((auth, rest)) => (Some(auth), rest),
            None => (None, rest),
        };
        let (hostport, db) = match host_and_db.split_once('/') {
            Some((hostport, db)) => {
                let db = db.split('/').next().unwrap_or("");
                let db = if db.is_empty() {
                    0
                } else {
                    db.parse::<i64>().map_err(|_| format!("bad db {db}"))?
                };
                (hostport, db)
            }
            None => (host_and_db, 0),
        };
        if db < 0 {
            return Err("db must be >= 0".into());
        }
        let (host, port) = split_host_port(hostport)?;
        let (username, password) = match auth {
            Some(auth) => {
                let (user, password) = auth
                    .split_once(':')
                    .ok_or("REDIS_URL credentials must be username:password")?;
                let username = if user.is_empty() {
                    None
                } else {
                    Some(percent_decode(user)?)
                };
                (username, Some(percent_decode(password)?))
            }
            None => (None, None),
        };
        Ok(Self {
            host,
            port,
            db,
            username,
            password,
            tls,
            connect_timeout: Duration::from_secs(5),
            response_timeout: Duration::from_secs(5),
            retries: None,
        })
    }

    /// The `config_json` of a connection to this endpoint, without the password.
    pub fn config_json(&self) -> serde_json::Value {
        let mut json = serde_json::json!({
            "host": self.host,
            "port": self.port,
            "db": self.db,
            "tls": if self.tls { "require" } else { "disable" },
        });
        if let Some(username) = &self.username {
            json["user"] = serde_json::Value::String(username.clone());
        }
        json
    }

    /// The password, or an empty string.
    pub fn secret(&self) -> String {
        self.password.clone().unwrap_or_default()
    }

    /// The endpoint as a URL the redis client accepts.
    pub fn redis_url(&self) -> String {
        let scheme = if self.tls { "rediss" } else { "redis" };
        let host = if self.host.contains(':') && !self.host.starts_with('[') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        let auth = match (&self.username, &self.password) {
            (Some(user), Some(password)) => {
                format!("{}:{}@", encode_userinfo(user), encode_userinfo(password))
            }
            (Some(user), None) => format!("{}@", encode_userinfo(user)),
            (None, Some(password)) => format!(":{}@", encode_userinfo(password)),
            (None, None) => String::new(),
        };
        format!("{scheme}://{auth}{host}:{}/{}", self.port, self.db)
    }
}

/// A connection that reconnects by itself.
pub async fn connect(endpoint: &RedisEndpoint) -> Result<ConnectionManager, String> {
    let client = redis::Client::open(endpoint.redis_url()).map_err(|err| err.to_string())?;
    let mut config = ConnectionManagerConfig::new()
        .set_connection_timeout(endpoint.connect_timeout)
        .set_response_timeout(endpoint.response_timeout);
    if let Some(retries) = endpoint.retries {
        config = config.set_number_of_retries(retries);
    }
    ConnectionManager::new_with_config(client, config)
        .await
        .map_err(|err| err.to_string())
}

fn split_host_port(hostport: &str) -> Result<(String, u16), String> {
    if hostport.is_empty() {
        return Err("REDIS_URL is missing a host".into());
    }
    if let Some(rest) = hostport.strip_prefix('[') {
        let (host, after) = rest.split_once(']').ok_or("bad ipv6 host")?;
        let port = after.strip_prefix(':').filter(|port| !port.is_empty());
        let port = match port {
            Some(port) => parse_port(port)?,
            None => 6379,
        };
        return Ok((host.to_string(), port));
    }
    if let Some((host, port)) = hostport.rsplit_once(':') {
        if !host.is_empty() && port.chars().all(|ch| ch.is_ascii_digit()) {
            return Ok((host.to_string(), parse_port(port)?));
        }
    }
    Ok((hostport.to_string(), 6379))
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

fn encode_userinfo(raw: &str) -> String {
    let mut out = String::new();
    for byte in raw.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use diavasi::runtime::SourceOpen;
    use diavasi::store::{ConnectionRecord, SealedSecret};

    use super::*;

    fn request(config: serde_json::Value, secret: &str) -> SourceOpen {
        SourceOpen {
            connection: ConnectionRecord {
                id: "c".into(),
                kind: "redis".into(),
                config_json: config,
                sealed_secret: SealedSecret {
                    nonce: Vec::new(),
                    ciphertext: Vec::new(),
                },
            },
            source_spec: serde_json::json!({}),
            secret: secret.as_bytes().to_vec(),
        }
    }

    #[test]
    fn url_without_credentials_skips_auth() {
        let endpoint = RedisEndpoint::from_url("redis://127.0.0.1:6379").unwrap();
        assert!(endpoint.username.is_none());
        assert!(endpoint.password.is_none());
        assert_eq!(endpoint.db, 0);
        assert!(!endpoint.tls);
        assert_eq!(endpoint.redis_url(), "redis://127.0.0.1:6379/0");
    }

    #[test]
    fn url_with_user_db_and_tls() {
        let endpoint = RedisEndpoint::from_url("rediss://ada:s%20ecret@[::1]:6380/2").unwrap();
        assert_eq!(endpoint.username.as_deref(), Some("ada"));
        assert_eq!(endpoint.password.as_deref(), Some("s ecret"));
        assert_eq!(endpoint.host, "::1");
        assert_eq!(endpoint.port, 6380);
        assert_eq!(endpoint.db, 2);
        assert!(endpoint.tls);
        assert!(
            endpoint
                .redis_url()
                .starts_with("rediss://ada:s%20ecret@[::1]:6380/2")
        );
    }

    #[test]
    fn request_uses_the_secret_only_when_a_username_is_set() {
        let open = RedisEndpoint::from_request(&request(
            serde_json::json!({"host": "127.0.0.1", "username": "ada"}),
            "secret",
        ))
        .unwrap();
        assert_eq!(open.password.as_deref(), Some("secret"));
        let anon = RedisEndpoint::from_request(&request(
            serde_json::json!({"host": "127.0.0.1"}),
            "unused",
        ))
        .unwrap();
        assert!(anon.password.is_none());
        assert!(RedisEndpoint::from_request(&request(serde_json::json!({}), "x")).is_err());
    }
}
