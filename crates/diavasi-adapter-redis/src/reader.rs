use crate::connect::{RedisEndpoint, connect};
use crate::spec::{SourceSpec, cursor_to_pair, id_to_ordering, id_to_pair};
use bytes::Bytes;
use diavasi::core::{LogicalCursor, Record, RecordSource, SourceError};
use diavasi::runtime::SourceOpen;
use futures::future::BoxFuture;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use redis::streams::{StreamId, StreamRangeReply};

pub struct RedisSource {
    conn: ConnectionManager,
    endpoint: RedisEndpoint,
    spec: SourceSpec,
}

impl RedisSource {
    pub async fn open(request: SourceOpen) -> Result<Self, String> {
        let spec = SourceSpec::parse(&request.source_spec)?;
        let endpoint = RedisEndpoint::from_request(&request)?;
        let mut conn = connect(&endpoint).await?;
        ensure_stream(&mut conn, &spec.stream).await?;
        Ok(Self {
            conn,
            endpoint,
            spec,
        })
    }

    /// Accept TCP and answer every command with `+OK`, which is not a stream
    /// read, so the next fetch fails once and opens a new connection.
    #[cfg(test)]
    pub async fn poison(&mut self) -> Result<(), String> {
        use std::time::Duration;

        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|err| err.to_string())?;
        let port = listener.local_addr().map_err(|err| err.to_string())?.port();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    loop {
                        let n = match socket.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => n,
                        };
                        let commands = buf[..n].iter().filter(|byte| **byte == b'*').count();
                        for _ in 0..commands {
                            if socket.write_all(b"+OK\r\n").await.is_err() {
                                return;
                            }
                        }
                    }
                });
            }
        });
        let endpoint = RedisEndpoint {
            host: "127.0.0.1".into(),
            port,
            db: 0,
            username: None,
            password: None,
            tls: false,
            connect_timeout: Duration::from_millis(500),
            response_timeout: Duration::from_millis(500),
            retries: Some(0),
        };
        self.conn = connect(&endpoint).await?;
        Ok(())
    }

    async fn fetch(
        &mut self,
        cursor: &LogicalCursor,
        limit: usize,
    ) -> Result<Vec<Record>, SourceError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut conn = self.conn.clone();
        match read_after(&mut conn, &self.spec, cursor, limit).await {
            Err(SourceError::Transient(err)) => {
                tracing::warn!("redis fetch failed, reconnecting: {err}");
                self.conn = connect(&self.endpoint)
                    .await
                    .map_err(SourceError::Transient)?;
                read_after(&mut self.conn, &self.spec, cursor, limit).await
            }
            other => other,
        }
    }
}

impl RecordSource for RedisSource {
    fn fetch_after<'a>(
        &'a mut self,
        cursor: &'a LogicalCursor,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Record>, SourceError>> {
        Box::pin(async move { self.fetch(cursor, limit).await })
    }
}

async fn ensure_stream(conn: &mut ConnectionManager, stream: &str) -> Result<(), String> {
    let kind: String = conn.key_type(stream).await.map_err(|err| err.to_string())?;
    match kind.as_str() {
        "stream" => Ok(()),
        "none" => Err(format!("stream {stream} does not exist")),
        other => Err(format!("key {stream} has type {other}")),
    }
}

/// Read up to `limit` entries strictly after `cursor` with
/// `XRANGE key (<ms>-<seq> + COUNT limit`. The read does not write to Redis:
/// no consumer group is created or moved, so readers of one stream cannot
/// disturb each other.
async fn read_after(
    conn: &mut ConnectionManager,
    spec: &SourceSpec,
    cursor: &LogicalCursor,
    limit: usize,
) -> Result<Vec<Record>, SourceError> {
    let start = match cursor_to_pair(cursor).map_err(SourceError::Contract)? {
        None => "-".to_string(),
        Some(after) => {
            check_no_gap(conn, &spec.stream, after).await?;
            format!("({}-{}", after.0, after.1)
        }
    };
    let reply: StreamRangeReply = conn
        .xrange_count(&spec.stream, start, "+", limit)
        .await
        .map_err(|err| SourceError::Transient(err.to_string()))?;
    reply
        .ids
        .iter()
        .map(|entry| record_from_entry(spec, entry).map_err(SourceError::Contract))
        .collect()
}

/// Fail when trimming removed entries after `after` before they were read.
///
/// Redis does not report which ids a trim removed, so the check infers it
/// from `XINFO STREAM` (Redis 7.0 or later; skipped on older servers):
///
/// - every entry up to the cursor is gone (the first entry is after it),
/// - entries were removed (`entries-added` exceeds `length`), and
/// - no `XDEL` reached the cursor (`max-deleted-entry-id` is before it).
///
/// The last condition keeps applications that `XDEL` processed entries from
/// being reported. A trim that stops exactly at the cursor is also reported,
/// because it cannot be told apart from one that went further.
async fn check_no_gap(
    conn: &mut ConnectionManager,
    stream: &str,
    after: (u64, u64),
) -> Result<(), SourceError> {
    let info: std::collections::HashMap<String, redis::Value> = redis::cmd("XINFO")
        .arg("STREAM")
        .arg(stream)
        .query_async(conn)
        .await
        .map_err(|err| SourceError::Transient(err.to_string()))?;
    stream_gap(stream, after, &info).map_err(SourceError::Contract)
}

/// The gap rule of [`check_no_gap`] applied to an `XINFO STREAM` reply.
fn stream_gap(
    stream: &str,
    after: (u64, u64),
    info: &std::collections::HashMap<String, redis::Value>,
) -> Result<(), String> {
    let text = |key: &str| -> Result<Option<String>, String> {
        info.get(key)
            .map(|value| redis::from_redis_value::<String>(value).map_err(|err| err.to_string()))
            .transpose()
    };
    let number = |key: &str| -> Result<Option<u64>, String> {
        info.get(key)
            .map(|value| redis::from_redis_value::<u64>(value).map_err(|err| err.to_string()))
            .transpose()
    };
    let (Some(max_deleted), Some(added), Some(length)) = (
        text("max-deleted-entry-id")?,
        number("entries-added")?,
        number("length")?,
    ) else {
        return Ok(());
    };
    let max_deleted = id_to_pair(&max_deleted)?;
    let first = match info.get("first-entry") {
        Some(redis::Value::Array(entry)) => match entry.first() {
            Some(id) => Some(id_to_pair(
                &redis::from_redis_value::<String>(id).map_err(|err| err.to_string())?,
            )?),
            None => None,
        },
        _ => None,
    };
    let head_passed_cursor = first.is_none_or(|first| first > after);
    if head_passed_cursor && added > length && max_deleted < after {
        return Err(format!(
            "stream {stream} was trimmed past the committed cursor {}-{}; entries after it were removed before delivery",
            after.0, after.1
        ));
    }
    Ok(())
}

fn record_from_entry(spec: &SourceSpec, entry: &StreamId) -> Result<Record, String> {
    let ordering = id_to_ordering(&entry.id)?;
    let mut pairs: Vec<(&str, &redis::Value)> = entry
        .map
        .iter()
        .map(|(key, value)| (key.as_str(), value))
        .collect();
    if let Some(fields) = &spec.fields {
        pairs.retain(|(key, _)| fields.iter().any(|field| field == key));
    }
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    let mut object = serde_json::Map::new();
    for (key, value) in pairs {
        object.insert(
            key.to_string(),
            serde_json::Value::String(field_text(value)?),
        );
    }
    let payload = Bytes::from(serde_json::Value::Object(object).to_string().into_bytes());
    Ok(Record { ordering, payload })
}

fn field_text(value: &redis::Value) -> Result<String, String> {
    redis::from_redis_value(value).map_err(|_| "stream field is not utf-8".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_utf8_field_fails() {
        let err = field_text(&redis::Value::BulkString(vec![0xff])).unwrap_err();
        assert!(err.contains("utf-8"), "{err}");
    }
}
