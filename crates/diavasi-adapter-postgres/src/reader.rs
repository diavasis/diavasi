use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use diavasi::core::{
    LogicalCursor, OrderingAtom, OrderingValue, Record, RecordSource, SourceError,
};
use diavasi::runtime::SourceOpen;
use futures::future::BoxFuture;
use tokio_postgres::Client;
use tokio_postgres::types::ToSql;

use crate::catalog::{ColumnInfo, describe};
use crate::connect::{PgEndpoint, connect};
use crate::spec::{ColType, SourceSpec, quote_ident};

pub struct PostgresSource {
    client: Client,
    endpoint: PgEndpoint,
    spec: SourceSpec,
    columns: HashMap<String, ColumnInfo>,
}

impl PostgresSource {
    pub async fn open(request: SourceOpen) -> Result<Self, String> {
        let spec = SourceSpec::parse(&request.source_spec)?;
        let mut endpoint = PgEndpoint::from_request(&request)?;
        endpoint
            .config
            .application_name(format!("diavasi:{}", spec.table));
        let client = connect(&endpoint).await?;
        let columns = describe(&client, &spec).await?;
        Ok(Self {
            client,
            endpoint,
            spec,
            columns,
        })
    }

    async fn query_rows(
        &mut self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<tokio_postgres::Row>, String> {
        match self.client.query(sql, params).await {
            Ok(rows) => Ok(rows),
            Err(err) => {
                tracing::warn!("postgres fetch failed, reconnecting: {err}");
                self.client = connect(&self.endpoint).await?;
                self.client
                    .query(sql, params)
                    .await
                    .map_err(|err| err.to_string())
            }
        }
    }

    async fn fetch(
        &mut self,
        cursor: &LogicalCursor,
        limit: usize,
    ) -> Result<Vec<Record>, SourceError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let (sql, owned) = build_query(&self.spec, cursor, limit).map_err(SourceError)?;
        let refs: Vec<&(dyn ToSql + Sync)> = owned
            .iter()
            .map(|p| p.as_ref() as &(dyn ToSql + Sync))
            .collect();
        let rows = self.query_rows(&sql, &refs).await.map_err(SourceError)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(decode_row(&self.spec, &self.columns, &row).map_err(SourceError)?);
        }
        Ok(out)
    }
}

impl RecordSource for PostgresSource {
    fn fetch_after<'a>(
        &'a mut self,
        cursor: &'a LogicalCursor,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Record>, SourceError>> {
        Box::pin(async move { self.fetch(cursor, limit).await })
    }
}

fn build_query(
    spec: &SourceSpec,
    cursor: &LogicalCursor,
    limit: usize,
) -> Result<(String, Vec<Box<dyn ToSql + Sync + Send>>), String> {
    let mut select = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for col in &spec.order_by {
        if seen.insert(col.name.clone()) {
            select.push(quote_ident(&col.name));
        }
    }
    for name in &spec.payload {
        if seen.insert(name.clone()) {
            select.push(quote_ident(name));
        }
    }
    let order_exprs: Vec<String> = spec
        .order_by
        .iter()
        .map(|col| {
            let quoted = quote_ident(&col.name);
            if col.ty.collated() {
                format!("{quoted} COLLATE \"C\"")
            } else {
                quoted
            }
        })
        .collect();
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    let mut sql = format!("SELECT {} FROM {} ", select.join(", "), spec.quoted_table());
    let mut predicates = Vec::new();
    if let Some(filter) = &spec.filter {
        predicates.push(format!("({filter})"));
    }
    if let Some(cursor) = cursor {
        if cursor.atoms().len() != spec.order_by.len() {
            return Err("cursor width does not match order_by".into());
        }
        let placeholders: Vec<String> = spec
            .order_by
            .iter()
            .zip(cursor.atoms())
            .map(|(col, atom)| {
                params.push(atom_param(col.ty, atom)?);
                Ok(format!("${}", params.len()))
            })
            .collect::<Result<Vec<_>, String>>()?;
        predicates.push(format!(
            "({}) > ({})",
            order_exprs.join(", "),
            placeholders.join(", ")
        ));
    }
    if !predicates.is_empty() {
        sql.push_str("WHERE ");
        sql.push_str(&predicates.join(" AND "));
        sql.push(' ');
    }
    params.push(Box::new(
        i64::try_from(limit).map_err(|_| "limit overflow")?,
    ));
    sql.push_str(&format!(
        "ORDER BY {} LIMIT ${}",
        order_exprs.join(", "),
        params.len()
    ));
    Ok((sql, params))
}

fn atom_param(ty: ColType, atom: &OrderingAtom) -> Result<Box<dyn ToSql + Sync + Send>, String> {
    match (ty, atom) {
        (ColType::Int2, OrderingAtom::I64(v)) => {
            let v = i16::try_from(*v).map_err(|_| "int2 cursor out of range")?;
            Ok(Box::new(v))
        }
        (ColType::Int4, OrderingAtom::I64(v)) => {
            let v = i32::try_from(*v).map_err(|_| "int4 cursor out of range")?;
            Ok(Box::new(v))
        }
        (ColType::Int8, OrderingAtom::I64(v)) => Ok(Box::new(*v)),
        (ColType::Text | ColType::Varchar, OrderingAtom::Bytes(bytes)) => {
            let text = String::from_utf8(bytes.clone()).map_err(|_| "text cursor is not utf-8")?;
            Ok(Box::new(text))
        }
        (ColType::Bytea, OrderingAtom::Bytes(bytes)) => Ok(Box::new(bytes.clone())),
        (ColType::Timestamptz, OrderingAtom::I64(micros)) => Ok(Box::new(micros_to_time(*micros)?)),
        _ => Err("cursor atom does not match the order column type".into()),
    }
}

fn decode_row(
    spec: &SourceSpec,
    columns: &HashMap<String, ColumnInfo>,
    row: &tokio_postgres::Row,
) -> Result<Record, String> {
    let mut atoms = Vec::new();
    for col in &spec.order_by {
        let index = row
            .columns()
            .iter()
            .position(|column| column.name() == col.name)
            .ok_or_else(|| format!("missing selected column {}", col.name))?;
        atoms.push(read_order(col.ty, row, index)?);
    }
    let mut payload = serde_json::Map::new();
    for name in &spec.payload {
        let info = columns
            .get(name)
            .ok_or_else(|| format!("missing payload type for {name}"))?;
        let index = row
            .columns()
            .iter()
            .position(|column| column.name() == name)
            .ok_or_else(|| format!("missing selected column {name}"))?;
        payload.insert(name.clone(), read_json(info, row, index)?);
    }
    Ok(Record {
        ordering: OrderingValue::new(atoms).map_err(|err| err.to_string())?,
        payload: Bytes::from(serde_json::Value::Object(payload).to_string().into_bytes()),
    })
}

fn read_order(
    ty: ColType,
    row: &tokio_postgres::Row,
    index: usize,
) -> Result<OrderingAtom, String> {
    match ty {
        ColType::Int2 => Ok(OrderingAtom::I64(i64::from(
            row.try_get::<_, i16>(index).map_err(|e| e.to_string())?,
        ))),
        ColType::Int4 => Ok(OrderingAtom::I64(i64::from(
            row.try_get::<_, i32>(index).map_err(|e| e.to_string())?,
        ))),
        ColType::Int8 => Ok(OrderingAtom::I64(
            row.try_get::<_, i64>(index).map_err(|e| e.to_string())?,
        )),
        ColType::Text | ColType::Varchar => Ok(OrderingAtom::Bytes(
            row.try_get::<_, String>(index)
                .map_err(|e| e.to_string())?
                .into_bytes(),
        )),
        ColType::Bytea => Ok(OrderingAtom::Bytes(
            row.try_get::<_, Vec<u8>>(index)
                .map_err(|e| e.to_string())?,
        )),
        ColType::Timestamptz => {
            let ts: SystemTime = row.try_get(index).map_err(|e| e.to_string())?;
            Ok(OrderingAtom::I64(time_to_micros(ts)?))
        }
    }
}

fn read_json(
    info: &ColumnInfo,
    row: &tokio_postgres::Row,
    index: usize,
) -> Result<serde_json::Value, String> {
    match info.ty {
        ColType::Int2 => json_opt(row.try_get::<_, Option<i16>>(index), |v| {
            serde_json::json!(v)
        }),
        ColType::Int4 => json_opt(row.try_get::<_, Option<i32>>(index), |v| {
            serde_json::json!(v)
        }),
        ColType::Int8 => json_opt(row.try_get::<_, Option<i64>>(index), |v| {
            serde_json::json!(v)
        }),
        ColType::Text | ColType::Varchar => json_opt(
            row.try_get::<_, Option<String>>(index),
            serde_json::Value::String,
        ),
        ColType::Bytea => json_opt(row.try_get::<_, Option<Vec<u8>>>(index), |bytes| {
            serde_json::Value::String(hex_encode(&bytes))
        }),
        ColType::Timestamptz => {
            let value = row
                .try_get::<_, Option<SystemTime>>(index)
                .map_err(|err| err.to_string())?;
            match value {
                Some(ts) => Ok(serde_json::json!(time_to_micros(ts)?)),
                None => Ok(serde_json::Value::Null),
            }
        }
    }
}

fn json_opt<T>(
    value: Result<Option<T>, tokio_postgres::Error>,
    map: impl FnOnce(T) -> serde_json::Value,
) -> Result<serde_json::Value, String> {
    match value.map_err(|err| err.to_string())? {
        Some(value) => Ok(map(value)),
        None => Ok(serde_json::Value::Null),
    }
}

fn micros_to_time(micros: i64) -> Result<SystemTime, String> {
    if micros >= 0 {
        UNIX_EPOCH
            .checked_add(Duration::from_micros(micros as u64))
            .ok_or_else(|| "timestamp overflow".into())
    } else {
        UNIX_EPOCH
            .checked_sub(Duration::from_micros(micros.unsigned_abs()))
            .ok_or_else(|| "timestamp overflow".into())
    }
}

fn time_to_micros(ts: SystemTime) -> Result<i64, String> {
    match ts.duration_since(UNIX_EPOCH) {
        Ok(duration) => {
            i64::try_from(duration.as_micros()).map_err(|_| "timestamp overflow".into())
        }
        Err(err) => {
            let micros =
                i64::try_from(err.duration().as_micros()).map_err(|_| "timestamp overflow")?;
            Ok(-micros)
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec() -> SourceSpec {
        SourceSpec::parse(&json!({
            "table": "app.events",
            "order_by": [
                { "column": "id", "type": "int2" },
                { "column": "name", "type": "varchar" }
            ],
            "payload": ["body"],
            "filter": "active",
        }))
        .unwrap()
    }

    #[test]
    fn build_query_includes_filter_collation_and_keyset() {
        let cursor = Some(
            OrderingValue::new(vec![
                OrderingAtom::I64(3),
                OrderingAtom::Bytes(b"a".to_vec()),
            ])
            .unwrap(),
        );
        let (sql, params) = build_query(&spec(), &cursor, 10).unwrap();
        assert!(sql.contains("WHERE (active) AND"));
        assert!(sql.contains("COLLATE \"C\""));
        assert!(sql.contains("LIMIT $3"));
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn build_query_rejects_a_bad_cursor_or_limit() {
        let wide = Some(
            OrderingValue::new(vec![
                OrderingAtom::I64(1),
                OrderingAtom::I64(2),
                OrderingAtom::I64(3),
            ])
            .unwrap(),
        );
        assert!(
            build_query(&spec(), &wide, 1)
                .unwrap_err()
                .contains("cursor width")
        );
        let too_big = (i64::MAX as usize).saturating_add(1);
        assert!(
            build_query(&spec(), &None, too_big)
                .unwrap_err()
                .contains("limit")
        );
    }

    #[test]
    fn atom_param_matches_column_types() {
        assert!(
            atom_param(ColType::Int2, &OrderingAtom::I64(40_000))
                .unwrap_err()
                .contains("int2")
        );
        assert!(
            atom_param(ColType::Int4, &OrderingAtom::I64(i64::from(i32::MAX) + 1))
                .unwrap_err()
                .contains("int4")
        );
        assert!(atom_param(ColType::Bytea, &OrderingAtom::Bytes(vec![0, 255])).is_ok());
        assert!(
            atom_param(ColType::Text, &OrderingAtom::Bytes(vec![0xff]))
                .unwrap_err()
                .contains("utf-8")
        );
        assert!(atom_param(ColType::Int8, &OrderingAtom::Bytes(vec![1])).is_err());
        assert!(atom_param(ColType::Timestamptz, &OrderingAtom::I64(-1_000_000)).is_ok());
    }

    #[test]
    fn timestamps_and_hex() {
        assert_eq!(hex_encode(&[0x0a, 0xff]), "0aff");
        assert_eq!(
            time_to_micros(UNIX_EPOCH + Duration::from_micros(7)).unwrap(),
            7
        );
        assert_eq!(
            time_to_micros(UNIX_EPOCH - Duration::from_secs(5)).unwrap(),
            -5_000_000
        );
        assert!(micros_to_time(-1_000_000).is_ok());
    }
}
