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

struct Reads {
    /// Partition mode: the first page of the partition.
    partition_base: Option<PreparedStatement>,
    /// Clustering tails, one per clustering column: equality on the earlier
    /// columns and a strict comparison on column `i`. Partition mode selects
    /// the table columns. Token mode also selects `token(pk)` first.
    tails: Vec<PreparedStatement>,
    /// Token mode: the first page of the ring.
    token_start: Option<PreparedStatement>,
    /// Token mode: partitions whose token is after the cursor's token.
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
            Err(SourceError::Transient(err)) => {
                tracing::warn!("scylla fetch failed, reconnecting: {err}");
                self.session = connect(&self.endpoint)
                    .await
                    .map_err(SourceError::Transient)?;
                self.reads = prepare(&self.session, &self.resolved)
                    .await
                    .map_err(SourceError::Transient)?;
                self.read(cursor, limit).await
            }
            other => other,
        }
    }

    async fn read(
        &mut self,
        cursor: &LogicalCursor,
        limit: usize,
    ) -> Result<Vec<Record>, SourceError> {
        #[cfg(test)]
        if self.fail_next {
            self.fail_next = false;
            return Err(SourceError::Transient("poisoned session".into()));
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
    ) -> Result<Vec<Record>, SourceError> {
        let mut running = cursor.clone();
        let mut out = Vec::new();
        if cursor.is_none() {
            let statement = self.reads.partition_base.as_ref().ok_or_else(|| {
                SourceError::Contract("partition statement was not prepared".into())
            })?;
            let mut values = self.resolved.partition_values.clone();
            values.push(limit_cql(limit));
            let rows = execute(&self.session, statement, values).await?;
            for row in rows {
                push_row(&mut out, &mut running, &self.resolved, row, false)?;
            }
            return Ok(out);
        }
        let clustering =
            cursor_clustering(cursor, &self.resolved).map_err(SourceError::Contract)?;
        let partition = self.resolved.partition_values.clone();
        self.read_tails(
            &partition,
            &clustering,
            false,
            limit,
            &mut running,
            &mut out,
        )
        .await?;
        Ok(out)
    }

    /// Rows of one partition that come after `clustering`, in clustering
    /// order, until `out` holds `limit` rows. Each tail statement fixes the
    /// first `i` clustering columns and moves past column `i`; the deepest
    /// tail runs first.
    async fn read_tails(
        &self,
        partition: &[CqlValue],
        clustering: &[CqlValue],
        token_scan: bool,
        limit: usize,
        running: &mut LogicalCursor,
        out: &mut Vec<Record>,
    ) -> Result<(), SourceError> {
        for index in (0..self.reads.tails.len()).rev() {
            if out.len() >= limit {
                break;
            }
            let mut values = partition.to_vec();
            values.extend(clustering.iter().take(index + 1).cloned());
            values.push(limit_cql(limit - out.len()));
            let rows = execute(&self.session, &self.reads.tails[index], values).await?;
            for row in rows {
                if out.len() >= limit {
                    break;
                }
                push_row(out, running, &self.resolved, row, token_scan)?;
            }
        }
        Ok(())
    }

    /// Token order: finish the cursor's partition with the clustering tails,
    /// then read partitions whose token is greater. Resuming inside a wide
    /// partition never re-reads its earlier rows.
    async fn read_token(
        &mut self,
        cursor: &LogicalCursor,
        limit: usize,
    ) -> Result<Vec<Record>, SourceError> {
        let mut running = cursor.clone();
        let mut out = Vec::new();
        let Some(position) = cursor else {
            let statement =
                self.reads.token_start.as_ref().ok_or_else(|| {
                    SourceError::Contract("token statement was not prepared".into())
                })?;
            let rows = execute(&self.session, statement, vec![limit_cql(limit)]).await?;
            for row in rows {
                push_row(&mut out, &mut running, &self.resolved, row, true)?;
            }
            return Ok(out);
        };
        let (token, partition, clustering) =
            split_token_cursor(position, &self.resolved).map_err(SourceError::Contract)?;
        self.read_tails(&partition, &clustering, true, limit, &mut running, &mut out)
            .await?;
        if out.len() < limit {
            let statement =
                self.reads.token_gt.as_ref().ok_or_else(|| {
                    SourceError::Contract("token statement was not prepared".into())
                })?;
            let values = vec![CqlValue::BigInt(token), limit_cql(limit - out.len())];
            for row in execute(&self.session, statement, values).await? {
                push_row(&mut out, &mut running, &self.resolved, row, true)?;
            }
        }
        Ok(out)
    }
}

fn cursor_clustering(cursor: &LogicalCursor, resolved: &Resolved) -> Result<Vec<CqlValue>, String> {
    let Some(ordering) = cursor else {
        return Err("missing cursor".into());
    };
    ordering_to_values(&resolved.clustering, ordering)
}

/// Split a token-scan cursor into the token, the partition key values, and
/// the clustering values.
fn split_token_cursor(
    ordering: &OrderingValue,
    resolved: &Resolved,
) -> Result<(i64, Vec<CqlValue>, Vec<CqlValue>), String> {
    let atoms = ordering.atoms();
    let pk_len = resolved.partition_columns.len();
    let ck_len = resolved.clustering.len();
    if atoms.len() != 1 + pk_len + ck_len {
        return Err(format!(
            "token cursor has {} values, the key has {}",
            atoms.len(),
            1 + pk_len + ck_len
        ));
    }
    let token = match &atoms[0] {
        OrderingAtom::I64(token) => *token,
        _ => return Err("scylla token cursor must start with the token".into()),
    };
    let part = |range: std::ops::Range<usize>, columns| -> Result<Vec<CqlValue>, String> {
        if range.is_empty() {
            return Ok(Vec::new());
        }
        let value = OrderingValue::new(atoms[range].to_vec()).map_err(|err| err.to_string())?;
        ordering_to_values(columns, &value)
    };
    let partition = part(1..1 + pk_len, &resolved.partition_columns)?;
    let clustering = part(1 + pk_len..atoms.len(), &resolved.clustering)?;
    Ok((token, partition, clustering))
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

/// Decode `row` and append it after `running`. Decode and order failures are
/// contract errors.
fn push_row(
    out: &mut Vec<Record>,
    running: &mut LogicalCursor,
    resolved: &Resolved,
    row: Row,
    token_scan: bool,
) -> Result<(), SourceError> {
    let record = row_to_record(resolved, row, token_scan).map_err(SourceError::Contract)?;
    push_record(out, running, record).map_err(SourceError::Contract)
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
    let pk_where = pk_names
        .iter()
        .map(|name| format!("{name} = ?"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let prepare =
        |cql: String| async move { session.prepare(cql).await.map_err(|err| err.to_string()) };
    if resolved.token {
        let token_expr = format!("token({})", pk_names.join(", "));
        let list = format!("{token_expr}, {selected}");
        let start = prepare(format!("SELECT {list} FROM {table} LIMIT ?")).await?;
        let gt = prepare(format!(
            "SELECT {list} FROM {table} WHERE {token_expr} > ? LIMIT ?"
        ))
        .await?;
        let mut tails = Vec::new();
        for clause in tail_clauses(&pk_where, resolved) {
            tails
                .push(prepare(format!("SELECT {list} FROM {table} WHERE {clause} LIMIT ?")).await?);
        }
        return Ok(Reads {
            partition_base: None,
            tails,
            token_start: Some(start),
            token_gt: Some(gt),
        });
    }
    let base = prepare(format!(
        "SELECT {selected} FROM {table} WHERE {pk_where} LIMIT ?"
    ))
    .await?;
    let mut tails = Vec::new();
    for clause in tail_clauses(&pk_where, resolved) {
        tails.push(
            prepare(format!(
                "SELECT {selected} FROM {table} WHERE {clause} LIMIT ?"
            ))
            .await?,
        );
    }
    Ok(Reads {
        partition_base: Some(base),
        tails,
        token_start: None,
        token_gt: None,
    })
}

/// One WHERE clause per clustering column `i`: the partition key, equality on
/// clustering columns before `i`, and a strict comparison on column `i` in its
/// clustering direction.
fn tail_clauses(pk_where: &str, resolved: &Resolved) -> Vec<String> {
    (0..resolved.clustering.len())
        .map(|index| {
            let mut clause = pk_where.to_string();
            for earlier in resolved.clustering.iter().take(index) {
                clause.push_str(&format!(" AND {} = ?", quote_ident(&earlier.name)));
            }
            let column = &resolved.clustering[index];
            clause.push_str(&format!(
                " AND {} {} ?",
                quote_ident(&column.name),
                cmp_op(column.direction)
            ));
            clause
        })
        .collect()
}

async fn execute(
    session: &Session,
    statement: &PreparedStatement,
    values: Vec<CqlValue>,
) -> Result<Vec<Row>, SourceError> {
    let result = session
        .execute_unpaged(statement, values)
        .await
        .map_err(|err| SourceError::Transient(err.to_string()))?;
    let contract = |err: &dyn std::fmt::Display| SourceError::Contract(err.to_string());
    let rows = result.into_rows_result().map_err(|err| contract(&err))?;
    let mut out = Vec::new();
    for row in rows.rows::<Row>().map_err(|err| contract(&err))? {
        out.push(row.map_err(|err| contract(&err))?);
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
