use std::time::Duration;

use diavasi::runtime::SourceOpen;
use mongodb::Client;
use mongodb::options::{ClientOptions, Credential, Tls, TlsOptions};

/// How to reach one database. The sealed secret is the password when `user` is set.
#[derive(Clone, Debug)]
pub struct MongoEndpoint {
    /// A `mongodb://` or `mongodb+srv://` connection string for a replica
    /// set, several mongos routers, or a DNS seed list. When set, `host` and
    /// `port` are unused and the driver discovers the topology. It carries
    /// no credentials; those come from `user` and the sealed secret.
    pub uri: Option<String>,
    /// Server host, for a single server reached directly.
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
    /// Connect with TLS. `None` leaves it to `uri` (TLS is on for
    /// `mongodb+srv://` and when the string sets `tls=true`).
    pub tls: Option<bool>,
    /// Name the server shows for this client.
    pub app_name: String,
    /// How long to wait for the server.
    pub server_selection_timeout: Duration,
}

impl MongoEndpoint {
    /// The endpoint of a stored connection. Unknown `config_json` keys are an error.
    ///
    /// `config_json` has either `host` (and optional `port`) for one server,
    /// or `uri` for a replica set, several routers, or a `mongodb+srv://`
    /// seed list.
    pub fn from_request(request: &SourceOpen) -> Result<Self, String> {
        let raw: RawConfig =
            diavasi::runtime::parse_json(&request.connection.config_json, "config_json")?;
        let database = raw
            .database
            .filter(|name| !name.is_empty())
            .ok_or("config_json.database is required")?;
        let (uri, host, port) = match (raw.uri, raw.host) {
            (Some(_), Some(_)) => {
                return Err("config_json takes uri or host, not both".into());
            }
            (Some(uri), None) => {
                if raw.port.is_some() {
                    return Err("config_json.port goes in the uri when uri is set".into());
                }
                check_uri(&uri)?;
                (Some(uri), String::new(), 27017)
            }
            (None, Some(host)) if !host.is_empty() => (None, host, raw.port.unwrap_or(27017)),
            (None, _) => return Err("config_json.host or config_json.uri is required".into()),
        };
        let tls = match raw.tls.as_deref() {
            None if uri.is_some() => None,
            None | Some("disable") => Some(false),
            Some("require") => Some(true),
            Some(other) => return Err(format!("tls {other} is not supported")),
        };
        let user = raw.user.filter(|user| !user.is_empty());
        let password = if user.is_some() {
            Some(
                String::from_utf8(request.secret.clone())
                    .map_err(|_| "secret must be a utf-8 password")?,
            )
        } else {
            None
        };
        Ok(Self {
            uri,
            host,
            port,
            database,
            user,
            password,
            auth_source: raw
                .auth_source
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| "admin".into()),
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
    /// assert_eq!(endpoint.tls, Some(true));
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
            uri: None,
            host,
            port,
            database: database.unwrap_or_else(|| "diavasi".into()),
            user,
            password,
            auth_source,
            tls: Some(tls),
            app_name: "diavasi".into(),
            server_selection_timeout: Duration::from_secs(5),
        })
    }

    /// The `config_json` of a connection to this endpoint, without the password.
    pub fn config_json(&self) -> serde_json::Value {
        let mut json = serde_json::json!({
            "database": self.database,
            "auth_source": self.auth_source,
        });
        match &self.uri {
            Some(uri) => json["uri"] = serde_json::Value::String(uri.clone()),
            None => {
                json["host"] = serde_json::Value::String(self.host.clone());
                json["port"] = serde_json::Value::from(self.port);
            }
        }
        if let Some(tls) = self.tls {
            json["tls"] = serde_json::Value::from(if tls { "require" } else { "disable" });
        }
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

/// `config_json` as written. Checked into a [`MongoEndpoint`].
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default)]
    uri: Option<String>,
    #[serde(default)]
    host: Option<String>,
    #[serde(default)]
    port: Option<u16>,
    #[serde(default)]
    database: Option<String>,
    #[serde(default)]
    user: Option<String>,
    #[serde(default)]
    auth_source: Option<String>,
    #[serde(default)]
    tls: Option<String>,
}

/// A connection string for `config_json.uri`: the right scheme, and no
/// password, which belongs in the sealed secret.
fn check_uri(uri: &str) -> Result<(), String> {
    let rest = uri
        .strip_prefix("mongodb+srv://")
        .or_else(|| uri.strip_prefix("mongodb://"))
        .ok_or("config_json.uri must start with mongodb:// or mongodb+srv://")?;
    let authority = rest.split(['/', '?']).next().unwrap_or("");
    if authority.contains('@') {
        return Err(
            "config_json.uri must not hold credentials; set user and put the password in the secret"
                .into(),
        );
    }
    if authority.is_empty() {
        return Err("config_json.uri is missing a host".into());
    }
    Ok(())
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
///
/// A `host` endpoint connects directly to that one server. A `uri` endpoint
/// lets the driver discover the replica set or routers the string names.
pub async fn connect(endpoint: &MongoEndpoint) -> Result<Client, String> {
    let uri = match &endpoint.uri {
        Some(uri) => uri.clone(),
        None => format!("mongodb://{}:{}", endpoint.host, endpoint.port),
    };
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
    if endpoint.uri.is_none() {
        options.direct_connection = Some(true);
    }
    options.server_selection_timeout = Some(endpoint.server_selection_timeout);
    options.connect_timeout = Some(endpoint.server_selection_timeout);
    options.app_name = Some(endpoint.app_name.clone());
    match endpoint.tls {
        Some(true) => options.tls = Some(Tls::Enabled(TlsOptions::builder().build())),
        Some(false) => options.tls = Some(Tls::Disabled),
        None => {}
    }
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
        assert_eq!(endpoint.tls, Some(false));
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
        assert_eq!(endpoint.tls, Some(true));
        let json = endpoint.config_json();
        assert_eq!(json["tls"], "require");
        assert!(json.get("password").is_none());
    }

    fn open(config: serde_json::Value, secret: &str) -> Result<MongoEndpoint, String> {
        MongoEndpoint::from_request(&SourceOpen {
            connection: diavasi::store::ConnectionRecord {
                id: "m".into(),
                kind: "mongodb".into(),
                config_json: config,
                sealed_secret: diavasi::store::SealedSecret {
                    nonce: Vec::new(),
                    ciphertext: Vec::new(),
                },
            },
            source_spec: serde_json::json!({}),
            secret: secret.as_bytes().to_vec(),
        })
    }

    /// B22: a replica set or `mongodb+srv://` seed list through `uri`.
    #[test]
    fn config_uri_for_replica_sets_and_srv() {
        let endpoint = open(
            serde_json::json!({
                "uri": "mongodb://a.internal:27017,b.internal:27017/?replicaSet=rs0",
                "database": "app", "user": "diavasi"
            }),
            "s3cret",
        )
        .unwrap();
        assert!(endpoint.uri.is_some());
        assert_eq!(endpoint.tls, None, "the uri decides TLS");
        assert_eq!(endpoint.password.as_deref(), Some("s3cret"));
        assert_eq!(endpoint.config_json()["uri"], endpoint.uri.clone().unwrap());
        assert!(endpoint.config_json().get("host").is_none());

        let srv = open(
            serde_json::json!({"uri": "mongodb+srv://cluster0.example.net", "database": "app", "tls": "require"}),
            "x",
        )
        .unwrap();
        assert_eq!(srv.tls, Some(true));

        let host = open(serde_json::json!({"host": "db", "database": "app"}), "x").unwrap();
        assert_eq!((host.port, host.tls), (27017, Some(false)));

        for (config, needle) in [
            (
                serde_json::json!({"uri": "mongodb://db", "host": "db", "database": "a"}),
                "not both",
            ),
            (
                serde_json::json!({"uri": "mongodb://db", "port": 1, "database": "a"}),
                "port",
            ),
            (
                serde_json::json!({"uri": "http://db", "database": "a"}),
                "mongodb://",
            ),
            (
                serde_json::json!({"uri": "mongodb://u:p@db", "database": "a"}),
                "credentials",
            ),
            (
                serde_json::json!({"uri": "mongodb:///app", "database": "a"}),
                "host",
            ),
            (serde_json::json!({"database": "a"}), "required"),
            (serde_json::json!({"host": "db"}), "database"),
            (
                serde_json::json!({"host": "db", "database": "a", "hots": 1}),
                "hots",
            ),
            (
                serde_json::json!({"host": "db", "database": "a", "port": 70000}),
                "port",
            ),
            (
                serde_json::json!({"host": "db", "database": "a", "tls": "prefer"}),
                "prefer",
            ),
        ] {
            let err = open(config.clone(), "x").unwrap_err();
            assert!(err.contains(needle), "{config}: {err}");
        }
    }

    /// B22: the driver accepts both URI forms. An SRV lookup needs DNS, so
    /// only the plain multi-host form is built into a client here.
    #[tokio::test]
    async fn multi_host_uri_builds_a_client() {
        let endpoint = open(
            serde_json::json!({
                "uri": "mongodb://127.0.0.1:1,127.0.0.1:2/?replicaSet=rs0",
                "database": "app"
            }),
            "x",
        )
        .unwrap();
        connect(&endpoint).await.unwrap();
    }
}
