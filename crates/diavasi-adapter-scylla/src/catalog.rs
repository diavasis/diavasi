//! Read `system_schema.columns` and bind the source spec to that table.

use scylla::client::session::Session;
use scylla::value::{CqlValue, Row};

use crate::connect::ScyllaEndpoint;
use crate::spec::{Direction, KeyColumn, KeyType, SourceSpec, check_ident, json_to_cql};

#[derive(Clone, Debug)]
pub struct Resolved {
    pub keyspace: String,
    pub table: String,
    pub partition_columns: Vec<KeyColumn>,
    pub partition_values: Vec<CqlValue>,
    pub clustering: Vec<KeyColumn>,
    pub select_columns: Vec<String>,
    pub token: bool,
}

struct SchemaColumn {
    name: String,
    kind: String,
    ty: String,
    clustering_order: String,
    position: i32,
}

pub async fn resolve(
    session: &Session,
    endpoint: &ScyllaEndpoint,
    spec: SourceSpec,
) -> Result<Resolved, String> {
    let keyspace = match spec.keyspace.clone() {
        Some(name) => name,
        None => endpoint
            .keyspace
            .clone()
            .ok_or("source_spec.keyspace is required when the connection has no keyspace")?,
    };
    check_ident(&keyspace, "keyspace")?;
    let columns = load_columns(session, &keyspace, &spec.table).await?;
    if columns.is_empty() {
        return Err(format!("table {keyspace}.{} does not exist", spec.table));
    }
    let mut partition = Vec::new();
    let mut clustering = Vec::new();
    let mut regular = Vec::new();
    for column in columns {
        match column.kind.as_str() {
            "partition_key" => {
                let ty = KeyType::parse(&column.ty)?;
                partition.push((
                    column.position,
                    KeyColumn {
                        name: column.name,
                        ty,
                        direction: Direction::Asc,
                    },
                ));
            }
            "clustering" => {
                let ty = KeyType::parse(&column.ty)?;
                let direction = match column.clustering_order.trim().to_ascii_lowercase().as_str() {
                    "asc" => Direction::Asc,
                    "desc" => Direction::Desc,
                    other => {
                        return Err(format!(
                            "clustering column {} has order {other}",
                            column.name
                        ));
                    }
                };
                clustering.push((
                    column.position,
                    KeyColumn {
                        name: column.name,
                        ty,
                        direction,
                    },
                ));
            }
            "regular" | "static" => regular.push((column.position, column.name)),
            other => return Err(format!("column {} has kind {other}", column.name)),
        }
    }
    partition.sort_by_key(|(position, _)| *position);
    clustering.sort_by_key(|(position, _)| *position);
    regular.sort_by_key(|(position, _)| *position);
    let partition_columns: Vec<KeyColumn> =
        partition.into_iter().map(|(_, column)| column).collect();
    let clustering: Vec<KeyColumn> = clustering.into_iter().map(|(_, column)| column).collect();
    if partition_columns.is_empty() {
        return Err(format!(
            "table {keyspace}.{} has no partition key",
            spec.table
        ));
    }
    let (token, partition_values) = if spec.token {
        (true, Vec::new())
    } else {
        let raw = spec.partition.clone().unwrap_or_default();
        if raw.len() != partition_columns.len() {
            return Err(format!(
                "partition must name every partition key ({})",
                partition_columns
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        let mut values = Vec::with_capacity(partition_columns.len());
        for column in &partition_columns {
            let value = raw
                .get(&column.name)
                .ok_or_else(|| format!("partition is missing key column {}", column.name))?;
            values.push(json_to_cql(column.ty, value)?);
        }
        for name in raw.keys() {
            if !partition_columns.iter().any(|column| &column.name == name) {
                return Err(format!("partition value {name} is not a partition key"));
            }
        }
        (false, values)
    };
    if !token && clustering.is_empty() {
        return Err("partition read needs a clustering column".into());
    }
    let select_columns = select_list(&spec, &partition_columns, &clustering, &regular)?;
    Ok(Resolved {
        keyspace,
        table: spec.table,
        partition_columns,
        partition_values,
        clustering,
        select_columns,
        token,
    })
}

fn select_list(
    spec: &SourceSpec,
    partition: &[KeyColumn],
    clustering: &[KeyColumn],
    regular: &[(i32, String)],
) -> Result<Vec<String>, String> {
    let mut names = Vec::new();
    for column in partition.iter().chain(clustering) {
        names.push(column.name.clone());
    }
    match &spec.columns {
        None => {
            for (_, name) in regular {
                names.push(name.clone());
            }
        }
        Some(requested) => {
            let known: Vec<&str> = partition
                .iter()
                .chain(clustering)
                .map(|column| column.name.as_str())
                .chain(regular.iter().map(|(_, name)| name.as_str()))
                .collect();
            for name in requested {
                if !known.iter().any(|existing| *existing == name) {
                    return Err(format!("column {name} is not in the table"));
                }
                if names.iter().any(|existing| existing == name) {
                    continue;
                }
                names.push(name.clone());
            }
        }
    }
    Ok(names)
}

async fn load_columns(
    session: &Session,
    keyspace: &str,
    table: &str,
) -> Result<Vec<SchemaColumn>, String> {
    let result = session
        .query_unpaged(
            "SELECT column_name, kind, type, clustering_order, position FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?",
            (keyspace, table),
        )
        .await
        .map_err(|err| err.to_string())?;
    let rows = result.into_rows_result().map_err(|err| err.to_string())?;
    let mut out = Vec::new();
    for row in rows.rows::<Row>().map_err(|err| err.to_string())? {
        let row = row.map_err(|err| err.to_string())?;
        let mut cols = row.columns.into_iter();
        out.push(SchemaColumn {
            name: text_column(cols.next().flatten(), "column_name")?,
            kind: text_column(cols.next().flatten(), "kind")?,
            ty: text_column(cols.next().flatten(), "type")?,
            clustering_order: text_column(cols.next().flatten(), "clustering_order")?,
            position: int_column(cols.next().flatten(), "position")?,
        });
    }
    Ok(out)
}

fn text_column(value: Option<CqlValue>, name: &str) -> Result<String, String> {
    match value {
        Some(CqlValue::Text(text) | CqlValue::Ascii(text)) => Ok(text),
        _ => Err(format!("system_schema.columns.{name} is not text")),
    }
}

fn int_column(value: Option<CqlValue>, name: &str) -> Result<i32, String> {
    match value {
        Some(CqlValue::Int(value)) => Ok(value),
        _ => Err(format!("system_schema.columns.{name} is not an int")),
    }
}
