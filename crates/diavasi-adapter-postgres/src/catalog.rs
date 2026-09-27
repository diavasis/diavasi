use std::collections::HashMap;

use tokio_postgres::Client;

use crate::spec::{ColType, SourceSpec};

#[derive(Clone)]
pub struct ColumnInfo {
    pub ty: ColType,
}

pub async fn describe(
    client: &Client,
    spec: &SourceSpec,
) -> Result<HashMap<String, ColumnInfo>, String> {
    let rows = client
        .query(
            "SELECT a.attname::text, t.typname::text, a.attnotnull
             FROM pg_attribute a
             JOIN pg_class c ON c.oid = a.attrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             JOIN pg_type t ON t.oid = a.atttypid
             WHERE n.nspname = $1 AND c.relname = $2
               AND a.attnum > 0 AND NOT a.attisdropped",
            &[&spec.schema, &spec.table],
        )
        .await
        .map_err(|err| err.to_string())?;
    if rows.is_empty() {
        return Err(format!(
            "table {}.{} was not found",
            spec.schema, spec.table
        ));
    }
    let mut raw = HashMap::new();
    for row in rows {
        let name: String = row.get(0);
        let typname: String = row.get(1);
        let not_null: bool = row.get(2);
        raw.insert(name, (typname, not_null));
    }

    let mut columns = HashMap::new();
    for col in &spec.order_by {
        let Some((typname, not_null)) = raw.get(&col.name) else {
            return Err(format!("order column {} is missing", col.name));
        };
        if typname != col.ty.typname() {
            return Err(format!(
                "order column {} has type {typname}, spec says {}",
                col.name,
                col.ty.typname()
            ));
        }
        if !not_null {
            return Err(format!("order column {} is nullable", col.name));
        }
        columns.insert(col.name.clone(), ColumnInfo { ty: col.ty });
    }
    for name in &spec.payload {
        let Some((typname, _not_null)) = raw.get(name) else {
            return Err(format!("payload column {name} is missing"));
        };
        let ty = ColType::parse(typname)
            .map_err(|_| format!("payload column {name} has unsupported type {typname}"))?;
        columns.insert(name.clone(), ColumnInfo { ty });
    }

    if !spec.acknowledge_unsafe {
        let indexes = unique_indexes(client, spec).await?;
        let wanted: Vec<&str> = spec.order_by.iter().map(|col| col.name.as_str()).collect();
        if !indexes.iter().any(|cols| leads_with(&wanted, cols)) {
            return Err(
                "order columns must start with every column of a unique index; set acknowledge_unsafe to accept the risk"
                    .into(),
            );
        }
    }
    Ok(columns)
}

/// True when `order` begins with all columns of `unique`, in index order.
/// Only then is the order tuple unique, so a keyset read cannot skip rows that
/// share a key with the last delivered row.
fn leads_with(order: &[&str], unique: &[String]) -> bool {
    !unique.is_empty()
        && unique.len() <= order.len()
        && unique
            .iter()
            .map(String::as_str)
            .eq(order[..unique.len()].iter().copied())
}

async fn unique_indexes(client: &Client, spec: &SourceSpec) -> Result<Vec<Vec<String>>, String> {
    let rows = client
        .query(
            "SELECT COALESCE(array_agg(a.attname::text ORDER BY u.ord), ARRAY[]::text[])
             FROM pg_index i
             JOIN pg_class c ON c.oid = i.indrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS u(attnum, ord) ON u.attnum > 0
             JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = u.attnum
             WHERE n.nspname = $1 AND c.relname = $2
               AND i.indisunique AND i.indisvalid AND i.indpred IS NULL
             GROUP BY i.indexrelid",
            &[&spec.schema, &spec.table],
        )
        .await
        .map_err(|err| err.to_string())?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}
