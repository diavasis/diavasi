use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use diavasi::core::{
    LogicalCursor, OrderingAtom, OrderingValue, Record, RecordSource, SourceError,
};
use diavasi::runtime::SourceOpen;
use futures::future::BoxFuture;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, Statement};

use crate::catalog::{ColumnInfo, describe};
use crate::connect::{PgEndpoint, connect, error_chain};
use crate::spec::{ColType, SourceSpec, quote_ident};

pub struct PostgresSource {
    client: Client,
    endpoint: PgEndpoint,
    spec: SourceSpec,
    statements: Statements,
    layout: Layout,
}

/// The two reads, prepared once per connection.
struct Statements {
    /// The first page: no cursor.
    first: Statement,
    /// A page strictly after a cursor.
    after: Statement,
}

impl Statements {
    async fn prepare(client: &Client, spec: &SourceSpec) -> Result<Self, String> {
        let prepare = |sql: String| async move {
            client
                .prepare(&sql)
                .await
                .map_err(|err| format!("source_spec does not compile: {}", error_chain(&err)))
        };
        Ok(Self {
            first: prepare(query_sql(spec, false)).await?,
            after: prepare(query_sql(spec, true)).await?,
        })
    }
}

/// Where each order and payload column sits in a result row.
struct Layout {
    order: Vec<(ColType, usize)>,
    payload: Vec<(String, ColumnInfo, usize)>,
}

impl Layout {
    fn new(
        spec: &SourceSpec,
        columns: &HashMap<String, ColumnInfo>,
        statement: &Statement,
    ) -> Result<Self, String> {
        let position = |name: &str| {
            statement
                .columns()
                .iter()
                .position(|column| column.name() == name)
                .ok_or_else(|| format!("missing selected column {name}"))
        };
        let order = spec
            .order_by
            .iter()
            .map(|col| Ok((col.ty, position(&col.name)?)))
            .collect::<Result<_, String>>()?;
        let payload = spec
            .payload
            .iter()
            .map(|name| {
                let info = columns
                    .get(name)
                    .cloned()
                    .ok_or_else(|| format!("missing payload type for {name}"))?;
                Ok((name.clone(), info, position(name)?))
            })
            .collect::<Result<_, String>>()?;
        Ok(Self { order, payload })
    }
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
        // Preparing surfaces filter and type errors at group create, not on
        // the first fetch.
        let statements = Statements::prepare(&client, &spec).await?;
        let layout = Layout::new(&spec, &columns, &statements.first)?;
        Ok(Self {
            client,
            endpoint,
            spec,
            statements,
            layout,
        })
    }

    /// Run the prepared read. After a failure, reconnect, prepare again, and
    /// retry once.
    async fn query_rows(
        &mut self,
        with_cursor: bool,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<tokio_postgres::Row>, String> {
        let statement = |statements: &Statements| {
            if with_cursor {
                statements.after.clone()
            } else {
                statements.first.clone()
            }
        };
        match self
            .client
            .query(&statement(&self.statements), params)
            .await
        {
            Ok(rows) => Ok(rows),
            Err(err) => {
                tracing::warn!("postgres fetch failed, reconnecting: {}", error_chain(&err));
                self.client = connect(&self.endpoint).await?;
                self.statements = Statements::prepare(&self.client, &self.spec).await?;
                self.client
                    .query(&statement(&self.statements), params)
                    .await
                    .map_err(|err| error_chain(&err))
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
        let owned = query_params(&self.spec, cursor, limit).map_err(SourceError::Contract)?;
        let refs: Vec<&(dyn ToSql + Sync)> = owned
            .iter()
            .map(|p| p.as_ref() as &(dyn ToSql + Sync))
            .collect();
        let rows = self
            .query_rows(cursor.is_some(), &refs)
            .await
            .map_err(SourceError::Transient)?;
        rows.iter()
            .map(|row| decode_row(&self.layout, row).map_err(SourceError::Contract))
            .collect()
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

/// The read and its parameters for `cursor`. Used by tests; the source runs
/// the prepared form of [`query_sql`] with [`query_params`].
#[cfg(test)]
fn build_query(
    spec: &SourceSpec,
    cursor: &LogicalCursor,
    limit: usize,
) -> Result<(String, Vec<Box<dyn ToSql + Sync + Send>>), String> {
    let params = query_params(spec, cursor, limit)?;
    Ok((query_sql(spec, cursor.is_some()), params))
}

/// `SELECT` of the order and payload columns, filtered, after the cursor
/// when `with_cursor`, in order, with the limit as the last parameter.
fn query_sql(spec: &SourceSpec, with_cursor: bool) -> String {
    let mut select = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for name in spec
        .order_by
        .iter()
        .map(|col| &col.name)
        .chain(spec.payload.iter())
    {
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
    let mut sql = format!("SELECT {} FROM {} ", select.join(", "), spec.quoted_table());
    let mut predicates = Vec::new();
    if let Some(filter) = &spec.filter {
        predicates.push(format!("({filter})"));
    }
    let mut next_param = 1;
    if with_cursor {
        let placeholders: Vec<String> = (0..spec.order_by.len())
            .map(|i| format!("${}", next_param + i))
            .collect();
        next_param += spec.order_by.len();
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
    sql.push_str(&format!(
        "ORDER BY {} LIMIT ${next_param}",
        order_exprs.join(", ")
    ));
    sql
}

/// Parameters for [`query_sql`]: the cursor values, then the limit.
fn query_params(
    spec: &SourceSpec,
    cursor: &LogicalCursor,
    limit: usize,
) -> Result<Vec<Box<dyn ToSql + Sync + Send>>, String> {
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    if let Some(cursor) = cursor {
        if cursor.atoms().len() != spec.order_by.len() {
            return Err("cursor width does not match order_by".into());
        }
        for (col, atom) in spec.order_by.iter().zip(cursor.atoms()) {
            params.push(atom_param(col.ty, atom)?);
        }
    }
    params.push(Box::new(
        i64::try_from(limit).map_err(|_| "limit overflow")?,
    ));
    Ok(params)
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

fn decode_row(layout: &Layout, row: &tokio_postgres::Row) -> Result<Record, String> {
    let mut atoms = Vec::with_capacity(layout.order.len());
    for (ty, index) in &layout.order {
        atoms.push(read_order(*ty, row, *index)?);
    }
    let mut payload = serde_json::Map::new();
    for (name, info, index) in &layout.payload {
        payload.insert(name.clone(), read_json(info, row, *index)?);
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
            serde_json::Value::String(hex::encode(bytes))
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
