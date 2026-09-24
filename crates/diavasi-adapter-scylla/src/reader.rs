//! Partition keyset and sequential token scan. The checkpoint is the logical key.

use bytes::Bytes;
use diavasi::core::{
    LogicalCursor, OrderingAtom, OrderingValue, Record, RecordSource, SourceError,
};
use diavasi::runtime::SourceOpen;
use futures::future::BoxFuture;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::{CqlValue, Row};
use serde_json::{Map, Value};

use crate::catalog::{Resolved, resolve};
use crate::connect::{ScyllaEndpoint, connect};
use crate::spec::{
    SourceSpec, cmp_op, columns_to_ordering, cql_to_json, ordering_to_values, quote_ident,
};

const MAX_TOKEN_PAGES: u32 = 8;

struct Reads {
    partition_base: Option<PreparedStatement>,
    partition_tails: Vec<PreparedStatement>,
    token_start: Option<PreparedStatement>,
    token_ge: Option<PreparedStatement>,
    token_gt: Option<PreparedStatement>,
}

pub struct ScyllaSource {
    session: Session,
    endpoint: ScyllaEndpoint,
    resolved: Resolved,
    reads: Reads,
    #[cfg(test)]
    fail_next: bool,
}

impl ScyllaSource {
    pub async fn open(request: SourceOpen) -> Result<Self, String> {
        let spec = SourceSpec::parse(&request.source_spec)?;
        let endpoint = ScyllaEndpoint::from_request(&request)?;
        let session = connect(&endpoint).await?;
        let resolved = resolve(&session, &endpoint, spec).await?;
        let reads = prepare(&session, &resolved).await?;
        Ok(Self {
            session,
            endpoint,
            resolved,
            reads,
            #[cfg(test)]
            fail_next: false,
        })
    }

    /// The next read fails once. `SessionBuilder` only returns after the
    /// control connection succeeds, so a dead port cannot be installed as a
    /// session that fails later. The fetch path still drops that error,
    /// opens a new session, and prepares again.
    #[cfg(test)]
    pub fn poison(&mut self) {
        self.fail_next = true;
    }

    async fn fetch(
        &mut self,
        cursor: &LogicalCursor,
        limit: usize,
    ) -> Result<Vec<Record>, SourceError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        match self.read(cursor, limit).await {
            Ok(records) => Ok(records),
            Err(err) => {
                tracing::warn!("scylla fetch failed, reconnecting: {err}");
                self.session = connect(&self.endpoint).await.map_err(SourceError)?;
                self.reads = prepare(&self.session, &self.resolved)
                    .await
                    .map_err(SourceError)?;
                self.read(cursor, limit).await.map_err(SourceError)
            }
        }
    }

    async fn read(&mut self, cursor: &LogicalCursor, limit: usize) -> Result<Vec<Record>, String> {
        #[cfg(test)]
        if self.fail_next {
            self.fail_next = false;
            return Err("poisoned session".into());
        }
        if self.resolved.token {
            self.read_token(cursor, limit).await
        } else {
            self.read_partition(cursor, limit).await
        }
    }

    async fn read_partition(
        &mut self,
        cursor: &LogicalCursor,
        limit: usize,
    ) -> Result<Vec<Record>, String> {
        let limit_value = limit_cql(limit);
        let mut running = cursor.clone();
        let mut out = Vec::new();
        if cursor.is_none() {
            let statement = self
                .reads
                .partition_base
                .as_ref()
                .ok_or("partition statement was not prepared")?;
            let mut values = self.resolved.partition_values.clone();
            values.push(limit_value);
            let rows = execute(&self.session, statement, values).await?;
            for row in rows {
                push_record(
                    &mut out,
                    &mut running,
                    row_to_record(&self.resolved, row, false)?,
                )?;
            }
            return Ok(out);
        }
        let originals = cursor_clustering(cursor, &self.resolved)?;
        let tails = self.reads.partition_tails.len();
        for index in (0..tails).rev() {
            let statement = &self.reads.partition_tails[index];
            let mut values = self.resolved.partition_values.clone();
            for value in originals.iter().take(index) {
                values.push(value.clone());
            }
            values.push(originals[index].clone());
            values.push(limit_value.clone());
            let rows = execute(&self.session, statement, values).await?;
            for row in rows {
                if out.len() == limit {
                    break;
                }
                push_record(
                    &mut out,
                    &mut running,
                    row_to_record(&self.resolved, row, false)?,
                )?;
            }
            if out.len() == limit {
                break;
            }
        }
        Ok(out)
    }

    async fn read_token(
        &mut self,
        cursor: &LogicalCursor,
        limit: usize,
    ) -> Result<Vec<Record>, String> {
        if cursor.is_none() {
            let statement = self
                .reads
                .token_start
                .as_ref()
                .ok_or("token statement was not prepared")?;
            let rows = execute(&self.session, statement, vec![limit_cql(limit)]).await?;
            let mut running = None;
            let mut out = Vec::new();
            for row in rows {
                push_record(
                    &mut out,
                    &mut running,
                    row_to_record(&self.resolved, row, true)?,
                )?;
            }
            return Ok(out);
        }
        let mut lower = cursor_token(cursor)?;
        let mut inclusive = true;
        let mut query_limit = limit.max(1);
        let mut pages = 0u32;
        let mut running = cursor.clone();
        let mut out = Vec::new();
        loop {
            pages += 1;
            if pages > MAX_TOKEN_PAGES {
                return Err("token scan did not advance past the cursor".into());
            }
            let statement = if inclusive {
                self.reads.token_ge.as_ref()
            } else {
                self.reads.token_gt.as_ref()
            }
            .ok_or("token statement was not prepared")?;
            let rows = execute(
                &self.session,
                statement,
                vec![CqlValue::BigInt(lower), limit_cql(query_limit)],
            )
            .await?;
            if rows.is_empty() {
                return Ok(out);
            }
            let page_len = rows.len();
            let last_token = row_token(rows.last().expect("page is not empty"))?;
            let mut added = 0usize;
            for row in rows {
                let record = row_to_record(&self.resolved, row, true)?;
                if !after(&running, &record.ordering) {
                    continue;
                }
                push_record(&mut out, &mut running, record)?;
                added += 1;
                if out.len() == limit {
                    return Ok(out);
                }
            }
            if page_len < query_limit {
                return Ok(out);
            }
            if added == 0 && last_token == lower {
                query_limit = query_limit.saturating_mul(2).min(100_000);
                continue;
            }
            inclusive = false;
            lower = last_token;
            query_limit = limit.max(1);
        }
    }
}

fn cursor_clustering(cursor: &LogicalCursor, resolved: &Resolved) -> Result<Vec<CqlValue>, String> {
    let Some(ordering) = cursor else {
        return Err("missing cursor".into());
    };
    ordering_to_values(&resolved.clustering, ordering)
}

fn cursor_token(cursor: &LogicalCursor) -> Result<i64, String> {
    let Some(ordering) = cursor else {
        return Err("missing cursor".into());
    };
    match ordering.atoms().first() {
        Some(OrderingAtom::I64(token)) => Ok(*token),
        _ => Err("scylla token cursor must start with the token".into()),
    }
}

fn limit_cql(limit: usize) -> CqlValue {
    let value = i32::try_from(limit).unwrap_or(i32::MAX);
    CqlValue::Int(value)
}

fn after(cursor: &LogicalCursor, ordering: &OrderingValue) -> bool {
    match cursor {
        None => true,
        Some(current) => ordering > current,
    }
}

fn push_record(
    out: &mut Vec<Record>,
    running: &mut LogicalCursor,
    record: Record,
) -> Result<(), String> {
    if !after(running, &record.ordering) {
        return Err("scylla row is not after the cursor".into());
    }
    *running = Some(record.ordering.clone());
    out.push(record);
    Ok(())
}

fn row_token(row: &Row) -> Result<i64, String> {
    match row.columns.first() {
        Some(Some(CqlValue::BigInt(token))) => Ok(*token),
        _ => Err("token() did not return a bigint".into()),
    }
}

fn row_to_record(resolved: &Resolved, row: Row, token_scan: bool) -> Result<Record, String> {
    let mut columns = row.columns.into_iter();
    let token = if token_scan {
        match columns.next().flatten() {
            Some(CqlValue::BigInt(token)) => Some(token),
            _ => return Err("token() did not return a bigint".into()),
        }
    } else {
        None
    };
    let mut table_values = Vec::with_capacity(resolved.select_columns.len());
    for name in &resolved.select_columns {
        let value = columns
            .next()
            .ok_or_else(|| format!("row is missing {name}"))?;
        table_values.push(value);
    }
    let ordering = if token_scan {
        token_ordering(resolved, token.unwrap_or(0), &table_values)?
    } else {
        let start = resolved.partition_columns.len();
        let end = start + resolved.clustering.len();
        let values = table_values[start..end]
            .iter()
            .map(|value| match value {
                Some(value) => Ok(value.clone()),
                None => Err("clustering column is null".into()),
            })
            .collect::<Result<Vec<_>, String>>()?;
        columns_to_ordering(&resolved.clustering, &values)?
    };
    let mut payload = Map::new();
    for (name, value) in resolved.select_columns.iter().zip(table_values) {
        payload.insert(name.clone(), cql_to_json(value)?);
    }
    let body = serde_json::to_vec(&Value::Object(payload)).map_err(|err| err.to_string())?;
    Ok(Record {
        ordering,
        payload: Bytes::from(body),
    })
}

fn token_ordering(
    resolved: &Resolved,
    token: i64,
    table_values: &[Option<CqlValue>],
) -> Result<OrderingValue, String> {
    let pk_len = resolved.partition_columns.len();
    let ck_len = resolved.clustering.len();
    let pk_values = required_values(&table_values[..pk_len], "partition key")?;
    let ck_values = required_values(&table_values[pk_len..pk_len + ck_len], "clustering key")?;
    let mut columns = Vec::with_capacity(1 + pk_len + ck_len);
    columns.push(OrderingAtom::I64(token));
    let pk = columns_to_ordering(&resolved.partition_columns, &pk_values)?;
    let ck = if ck_len == 0 {
        None
    } else {
        Some(columns_to_ordering(&resolved.clustering, &ck_values)?)
    };
    columns.extend(pk.atoms().iter().cloned());
    if let Some(ck) = ck {
        columns.extend(ck.atoms().iter().cloned());
    }
    OrderingValue::new(columns).map_err(|err| err.to_string())
}

fn required_values(values: &[Option<CqlValue>], what: &str) -> Result<Vec<CqlValue>, String> {
    values
        .iter()
        .map(|value| value.clone().ok_or_else(|| format!("{what} is null")))
        .collect()
}

async fn prepare(session: &Session, resolved: &Resolved) -> Result<Reads, String> {
    let table = format!(
        "{}.{}",
        quote_ident(&resolved.keyspace),
        quote_ident(&resolved.table)
    );
    let selected = resolved
        .select_columns
        .iter()
        .map(|name| quote_ident(name))
        .collect::<Vec<_>>()
        .join(", ");
    let pk_names = resolved
        .partition_columns
        .iter()
        .map(|column| quote_ident(&column.name))
        .collect::<Vec<_>>();
    if resolved.token {
        let token_expr = format!("token({})", pk_names.join(", "));
        let list = format!("{token_expr}, {selected}");
        let start = session
            .prepare(format!("SELECT {list} FROM {table} LIMIT ?"))
            .await
            .map_err(|err| err.to_string())?;
        let ge = session
            .prepare(format!(
                "SELECT {list} FROM {table} WHERE {token_expr} >= ? LIMIT ?"
            ))
            .await
            .map_err(|err| err.to_string())?;
        let gt = session
            .prepare(format!(
                "SELECT {list} FROM {table} WHERE {token_expr} > ? LIMIT ?"
            ))
            .await
            .map_err(|err| err.to_string())?;
        return Ok(Reads {
            partition_base: None,
            partition_tails: Vec::new(),
            token_start: Some(start),
            token_ge: Some(ge),
            token_gt: Some(gt),
        });
    }
    let pk_where = pk_names
        .iter()
        .map(|name| format!("{name} = ?"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let base = session
        .prepare(format!(
            "SELECT {selected} FROM {table} WHERE {pk_where} LIMIT ?"
        ))
        .await
        .map_err(|err| err.to_string())?;
    let mut tails = Vec::new();
    for index in 0..resolved.clustering.len() {
        let mut clause = pk_where.clone();
        for earlier in resolved.clustering.iter().take(index) {
            clause.push_str(" AND ");
            clause.push_str(&quote_ident(&earlier.name));
            clause.push_str(" = ?");
        }
        let column = &resolved.clustering[index];
        clause.push_str(" AND ");
        clause.push_str(&quote_ident(&column.name));
        clause.push(' ');
        clause.push_str(cmp_op(column.direction));
        clause.push_str(" ?");
        let statement = session
            .prepare(format!(
                "SELECT {selected} FROM {table} WHERE {clause} LIMIT ?"
            ))
            .await
            .map_err(|err| err.to_string())?;
        tails.push(statement);
    }
    Ok(Reads {
        partition_base: Some(base),
        partition_tails: tails,
        token_start: None,
        token_ge: None,
        token_gt: None,
    })
}

async fn execute(
    session: &Session,
    statement: &PreparedStatement,
    values: Vec<CqlValue>,
) -> Result<Vec<Row>, String> {
    let result = session
        .execute_unpaged(statement, values)
        .await
        .map_err(|err| err.to_string())?;
    let rows = result.into_rows_result().map_err(|err| err.to_string())?;
    let mut out = Vec::new();
    for row in rows.rows::<Row>().map_err(|err| err.to_string())? {
        out.push(row.map_err(|err| err.to_string())?);
    }
    Ok(out)
}

impl RecordSource for ScyllaSource {
    fn fetch_after<'a>(
        &'a mut self,
        cursor: &'a LogicalCursor,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Record>, SourceError>> {
        Box::pin(self.fetch(cursor, limit))
    }
}
