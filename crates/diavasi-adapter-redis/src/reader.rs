use crate::connect::{RedisEndpoint, connect};
use crate::spec::{SourceSpec, cursor_to_id, id_to_ordering};
use bytes::Bytes;
use diavasi::core::{LogicalCursor, Record, RecordSource, SourceError};
use diavasi::runtime::SourceOpen;
use futures::future::BoxFuture;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use redis::streams::{StreamId, StreamReadOptions, StreamReadReply};

const CONSUMER: &str = "diavasi";

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
        ensure_group(&mut conn, &spec).await?;
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
            Ok(records) => Ok(records),
            Err(err) => {
                tracing::warn!("redis fetch failed, reconnecting: {err}");
                self.conn = connect(&self.endpoint).await.map_err(SourceError)?;
                read_after(&mut self.conn, &self.spec, cursor, limit)
                    .await
                    .map_err(SourceError)
            }
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

async fn ensure_group(conn: &mut ConnectionManager, spec: &SourceSpec) -> Result<(), String> {
    match conn
        .xgroup_create::<_, _, _, ()>(&spec.stream, &spec.group, "0-0")
        .await
    {
        Ok(()) => Ok(()),
        Err(err) if is_busy_group(&err) => Ok(()),
        Err(err) => Err(err.to_string()),
    }
}

fn is_busy_group(err: &redis::RedisError) -> bool {
    err.code() == Some("BUSYGROUP") || err.to_string().contains("BUSYGROUP")
}

async fn read_after(
    conn: &mut ConnectionManager,
    spec: &SourceSpec,
    cursor: &LogicalCursor,
    limit: usize,
) -> Result<Vec<Record>, String> {
    ensure_group(conn, spec).await?;
    let id = cursor_to_id(cursor)?;
    let _: () = conn
        .xgroup_setid(&spec.stream, &spec.group, &id)
        .await
        .map_err(|err| err.to_string())?;
    let options = StreamReadOptions::default()
        .group(&spec.group, CONSUMER)
        .count(limit);
    let reply: Option<StreamReadReply> = conn
        .xread_options(&[spec.stream.as_str()], &[">"], &options)
        .await
        .map_err(|err| err.to_string())?;
    let entries = reply.map(flatten).unwrap_or_default();
    let mut records = Vec::with_capacity(entries.len());
    for entry in &entries {
        records.push(record_from_entry(spec, entry)?);
    }
    if !entries.is_empty() {
        let ids: Vec<&str> = entries.iter().map(|entry| entry.id.as_str()).collect();
        let _: i64 = conn
            .xack(&spec.stream, &spec.group, &ids)
            .await
            .map_err(|err| err.to_string())?;
    }
    Ok(records)
}

fn flatten(reply: StreamReadReply) -> Vec<StreamId> {
    reply.keys.into_iter().flat_map(|key| key.ids).collect()
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
