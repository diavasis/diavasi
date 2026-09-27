use diavasi::runtime::check_keys;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColType {
    Int2,
    Int4,
    Int8,
    Text,
    Varchar,
    Bytea,
    Timestamptz,
}

impl ColType {
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "int2" => Ok(Self::Int2),
            "int4" => Ok(Self::Int4),
            "int8" => Ok(Self::Int8),
            "text" => Ok(Self::Text),
            "varchar" => Ok(Self::Varchar),
            "bytea" => Ok(Self::Bytea),
            "timestamptz" => Ok(Self::Timestamptz),
            other => Err(format!("unsupported column type {other}")),
        }
    }

    pub fn typname(self) -> &'static str {
        match self {
            Self::Int2 => "int2",
            Self::Int4 => "int4",
            Self::Int8 => "int8",
            Self::Text => "text",
            Self::Varchar => "varchar",
            Self::Bytea => "bytea",
            Self::Timestamptz => "timestamptz",
        }
    }

    pub fn collated(self) -> bool {
        matches!(self, Self::Text | Self::Varchar)
    }
}

#[derive(Debug, Clone)]
pub struct OrderCol {
    pub name: String,
    pub ty: ColType,
}

#[derive(Debug, Clone)]
pub struct SourceSpec {
    pub schema: String,
    pub table: String,
    pub order_by: Vec<OrderCol>,
    pub payload: Vec<String>,
    pub filter: Option<String>,
    pub acknowledge_unsafe: bool,
}

impl SourceSpec {
    pub fn parse(value: &Value) -> Result<Self, String> {
        check_keys(
            value,
            &[
                "table",
                "order_by",
                "payload",
                "filter",
                "acknowledge_unsafe",
            ],
            "source_spec",
        )?;
        let table = value
            .get("table")
            .and_then(|v| v.as_str())
            .ok_or("source_spec.table is required")?;
        let (schema, table) = split_table(table)?;
        let order_by = value
            .get("order_by")
            .and_then(|v| v.as_array())
            .ok_or("source_spec.order_by is required")?;
        if order_by.is_empty() {
            return Err("source_spec.order_by must be non-empty".into());
        }
        let mut cols = Vec::new();
        for col in order_by {
            check_keys(col, &["column", "type"], "order_by entry")?;
            let name = col
                .get("column")
                .and_then(|v| v.as_str())
                .ok_or("order_by.column is required")?;
            let ty = col
                .get("type")
                .and_then(|v| v.as_str())
                .ok_or("order_by.type is required")?;
            check_ident(name)?;
            cols.push(OrderCol {
                name: name.to_string(),
                ty: ColType::parse(ty)?,
            });
        }
        let payload = match value.get("payload") {
            Some(Value::Array(items)) => items
                .iter()
                .map(|item| {
                    let name = item.as_str().ok_or("payload entries must be strings")?;
                    check_ident(name)?;
                    Ok(name.to_string())
                })
                .collect::<Result<Vec<_>, String>>()?,
            Some(_) => return Err("source_spec.payload must be an array".into()),
            None => Vec::new(),
        };
        if payload.is_empty() {
            return Err("source_spec.payload must list at least one column".into());
        }
        let filter = match value.get("filter") {
            None | Some(Value::Null) => None,
            Some(Value::String(raw)) => {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    check_filter(trimmed)?;
                    Some(trimmed.to_string())
                }
            }
            Some(_) => return Err("source_spec.filter must be a string".into()),
        };
        let acknowledge_unsafe = match value.get("acknowledge_unsafe") {
            None | Some(Value::Null) => false,
            Some(Value::Bool(flag)) => *flag,
            Some(_) => return Err("source_spec.acknowledge_unsafe must be a bool".into()),
        };
        Ok(Self {
            schema,
            table,
            order_by: cols,
            payload,
            filter,
            acknowledge_unsafe,
        })
    }

    pub fn quoted_table(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.table))
    }
}

pub fn quote_ident(name: &str) -> String {
    format!("\"{name}\"")
}

pub fn check_ident(name: &str) -> Result<(), String> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err("identifier is empty".into());
    };
    if !(first.is_ascii_alphabetic() || first == '_')
        || !chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(format!("identifier {name} is not a plain name"));
    }
    Ok(())
}

fn split_table(table: &str) -> Result<(String, String), String> {
    let parts: Vec<&str> = table.split('.').collect();
    match parts.as_slice() {
        [name] => {
            check_ident(name)?;
            Ok(("public".into(), (*name).to_string()))
        }
        [schema, name] => {
            check_ident(schema)?;
            check_ident(name)?;
            Ok(((*schema).to_string(), (*name).to_string()))
        }
        _ => Err("table must be name or schema.name".into()),
    }
}

/// Reject a filter that could escape the `(<filter>)` it is placed in, add a
/// statement, or hide text in a comment. The scan skips quoted literals and
/// identifiers, so `note <> 'a--b'` is accepted. Outside quotes it rejects
/// `;`, `--`, `/*`, `*/`, `$`, and parentheses that do not balance.
fn check_filter(filter: &str) -> Result<(), String> {
    let chars: Vec<char> = filter.chars().collect();
    let mut depth = 0usize;
    let mut i = 0;
    while i < chars.len() {
        let next = chars.get(i + 1).copied();
        match chars[i] {
            quote @ ('\'' | '"') => {
                i += 1;
                loop {
                    match chars.get(i) {
                        None => return Err("filter has an unterminated quote".into()),
                        Some('\\') if quote == '\'' => {
                            return Err("filter literals must not contain a backslash".into());
                        }
                        Some(&c) if c == quote && chars.get(i + 1) == Some(&quote) => i += 2,
                        Some(&c) if c == quote => break,
                        Some(_) => i += 1,
                    }
                }
            }
            ';' => return Err("filter must not contain extra statements".into()),
            '-' if next == Some('-') => return Err("filter must not contain comments".into()),
            '/' if next == Some('*') => return Err("filter must not contain comments".into()),
            '*' if next == Some('/') => return Err("filter must not contain comments".into()),
            '$' => {
                return Err("filter must not contain `$` (parameters or dollar quoting)".into());
            }
            '(' => depth += 1,
            ')' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or("filter has a `)` without a matching `(`")?;
            }
            _ => {}
        }
        i += 1;
    }
    if depth != 0 {
        return Err("filter has a `(` without a matching `)`".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(extra: serde_json::Value) -> Result<SourceSpec, String> {
        let mut value = json!({
            "table": "events",
            "order_by": [{ "column": "id", "type": "int8" }],
            "payload": ["body"],
        });
        for (key, item) in extra.as_object().unwrap() {
            value[key] = item.clone();
        }
        SourceSpec::parse(&value)
    }

    #[test]
    fn parses_schema_filter_and_unsafe_flag() {
        let parsed = spec(json!({
            "table": "app.events",
            "filter": " id > 1 ",
            "acknowledge_unsafe": true,
        }))
        .unwrap();
        assert_eq!(parsed.schema, "app");
        assert_eq!(parsed.table, "events");
        assert_eq!(parsed.filter.as_deref(), Some("id > 1"));
        assert!(parsed.acknowledge_unsafe);
        assert_eq!(parsed.quoted_table(), "\"app\".\"events\"");
    }

    #[test]
    fn rejects_bad_shape() {
        assert!(
            spec(json!({"order_by": []}))
                .unwrap_err()
                .contains("non-empty")
        );
        assert!(
            spec(json!({"payload": "body"}))
                .unwrap_err()
                .contains("array")
        );
        assert!(
            spec(json!({"payload": []}))
                .unwrap_err()
                .contains("at least")
        );
        assert!(spec(json!({"filter": 1})).unwrap_err().contains("string"));
        assert!(
            spec(json!({"filter": "a; drop table t"}))
                .unwrap_err()
                .contains("statements")
        );
        assert!(spec(json!({"filter": "a -- b"})).is_err());
        assert!(spec(json!({"filter": "a /* b"})).is_err());
        assert!(spec(json!({"filter": "a */ b"})).is_err());
        assert!(spec(json!({"filter": "true) OR (true"})).is_err());
        assert!(spec(json!({"filter": "(a = 1"})).is_err());
        assert!(spec(json!({"filter": "note = 'open"})).is_err());
        assert!(spec(json!({"filter": "note = $1"})).is_err());
        assert!(spec(json!({"filter": r"note = 'a\' OR true'"})).is_err());
        assert!(
            spec(json!({"filter": "note <> 'a--b' AND (n > 1 OR \"odd;name\" = 'x')"})).is_ok()
        );
        assert!(spec(json!({"filter": "note = 'it''s'"})).is_ok());
        assert!(
            spec(json!({"table": "a.b.c"}))
                .unwrap_err()
                .contains("schema.name")
        );
        assert!(spec(json!({"table": ""})).unwrap_err().contains("empty"));
        assert!(
            spec(json!({"table": "bad-name"}))
                .unwrap_err()
                .contains("plain name")
        );
        assert!(
            spec(json!({"order_by": [{ "column": "id", "type": "json" }]}))
                .unwrap_err()
                .contains("unsupported")
        );
        let empty_filter = spec(json!({"filter": "   "})).unwrap();
        assert!(empty_filter.filter.is_none());
        let null_filter = spec(json!({"filter": null})).unwrap();
        assert!(null_filter.filter.is_none());
        assert!(!spec(json!({})).unwrap().acknowledge_unsafe);
    }

    #[test]
    fn column_types_round_trip() {
        for (name, ty, collated) in [
            ("int2", ColType::Int2, false),
            ("int4", ColType::Int4, false),
            ("int8", ColType::Int8, false),
            ("text", ColType::Text, true),
            ("varchar", ColType::Varchar, true),
            ("bytea", ColType::Bytea, false),
            ("timestamptz", ColType::Timestamptz, false),
        ] {
            assert_eq!(ColType::parse(name).unwrap(), ty);
            assert_eq!(ty.typname(), name);
            assert_eq!(ty.collated(), collated);
        }
    }

    /// S2: a misspelled key is an error, not an option left at its default.
    #[test]
    fn rejects_unknown_keys_and_a_non_bool_flag() {
        let typo = spec(json!({"filtr": "id > 1"})).unwrap_err();
        assert!(typo.contains("filtr"), "{typo}");
        let order = spec(json!({"order_by": [{"column": "id", "type": "int8", "dir": "asc"}]}))
            .unwrap_err();
        assert!(order.contains("dir"), "{order}");
        let flag = spec(json!({"acknowledge_unsafe": "yes"})).unwrap_err();
        assert!(flag.contains("bool"), "{flag}");
    }
}
