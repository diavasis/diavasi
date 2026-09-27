//! `source_spec` for a ScyllaDB table, and the cursor encoding.

use diavasi::core::encoding;
use diavasi::core::{OrderingAtom, OrderingValue};
use scylla::value::{CqlDate, CqlTimestamp, CqlValue};
use serde_json::{Map, Value};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Asc,
    Desc,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyType {
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Timestamp,
    Date,
    Boolean,
    Text,
    Ascii,
    Blob,
    Uuid,
}

impl KeyType {
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw.trim() {
            "tinyint" => Ok(Self::TinyInt),
            "smallint" => Ok(Self::SmallInt),
            "int" => Ok(Self::Int),
            "bigint" => Ok(Self::BigInt),
            "timestamp" => Ok(Self::Timestamp),
            "date" => Ok(Self::Date),
            "boolean" => Ok(Self::Boolean),
            "text" | "varchar" => Ok(Self::Text),
            "ascii" => Ok(Self::Ascii),
            "blob" => Ok(Self::Blob),
            "uuid" => Ok(Self::Uuid),
            other => Err(format!(
                "key column type {other} is not supported (double, decimal, collections, and user types cannot be keys)"
            )),
        }
    }

    fn signed(self) -> bool {
        matches!(
            self,
            Self::TinyInt
                | Self::SmallInt
                | Self::Int
                | Self::BigInt
                | Self::Timestamp
                | Self::Date
                | Self::Boolean
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyColumn {
    pub name: String,
    pub ty: KeyType,
    pub direction: Direction,
}

#[derive(Clone, Debug)]
pub struct SourceSpec {
    pub keyspace: Option<String>,
    pub table: String,
    pub partition: Option<Map<String, Value>>,
    pub token: bool,
    pub columns: Option<Vec<String>>,
}

impl SourceSpec {
    pub fn parse(value: &Value) -> Result<Self, String> {
        let obj = value.as_object().ok_or("source_spec must be an object")?;
        for key in obj.keys() {
            match key.as_str() {
                "keyspace" | "table" | "partition" | "scan" | "columns" => {}
                other => {
                    return Err(format!(
                        "unknown source_spec field {other} (ALLOW FILTERING, secondary indexes, and ORDER BY are not supported)"
                    ));
                }
            }
        }
        let table = obj
            .get("table")
            .and_then(|v| v.as_str())
            .ok_or("source_spec.table is required")?;
        check_ident(table, "table")?;
        let keyspace = match obj.get("keyspace") {
            None => None,
            Some(value) => {
                let name = value
                    .as_str()
                    .ok_or("source_spec.keyspace must be a string")?;
                check_ident(name, "keyspace")?;
                Some(name.to_string())
            }
        };
        let partition = match obj.get("partition") {
            None => None,
            Some(value) => Some(
                value
                    .as_object()
                    .cloned()
                    .ok_or("source_spec.partition must be an object")?,
            ),
        };
        let token = match obj.get("scan") {
            None => false,
            Some(value) => {
                let scan = value.as_str().ok_or("source_spec.scan must be a string")?;
                if scan != "token" {
                    return Err("source_spec.scan must be \"token\"".into());
                }
                true
            }
        };
        if partition.is_none() && !token {
            return Err(
                "source_spec needs partition or scan \"token\" (a full table scan is not the default)"
                    .into(),
            );
        }
        if partition.is_some() && token {
            return Err("source_spec cannot set both partition and scan".into());
        }
        let columns = match obj.get("columns") {
            None => None,
            Some(value) => {
                let list = value
                    .as_array()
                    .ok_or("source_spec.columns must be an array")?;
                let mut names = Vec::new();
                for item in list {
                    let name = item
                        .as_str()
                        .ok_or("source_spec.columns entries must be strings")?;
                    check_ident(name, "column")?;
                    if names.iter().any(|existing: &String| existing == name) {
                        return Err(format!("source_spec.columns lists {name} twice"));
                    }
                    names.push(name.to_string());
                }
                Some(names)
            }
        };
        Ok(Self {
            keyspace,
            table: table.to_string(),
            partition,
            token,
            columns,
        })
    }
}

pub fn check_ident(name: &str, what: &str) -> Result<(), String> {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return Err(format!("{what} {name} is not an identifier")),
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(format!("{what} {name} is not an identifier"));
    }
    Ok(())
}

pub fn quote_ident(name: &str) -> String {
    format!("\"{name}\"")
}

/// Map a signed value so a descending field still increases along the stream.
pub fn order_i64(value: i64, direction: Direction) -> i64 {
    encoding::order_i64(value, direction == Direction::Desc)
}

/// Bytes the cursor stores. See [`encoding::order_bytes`].
pub fn canonical_to_atom_bytes(bytes: &[u8], direction: Direction) -> Vec<u8> {
    encoding::order_bytes(bytes, direction == Direction::Desc)
}

/// The value bytes of a cursor atom. See [`encoding::unorder_bytes`].
pub fn atom_bytes_to_canonical(bytes: &[u8], direction: Direction) -> Result<Vec<u8>, String> {
    encoding::unorder_bytes(bytes, direction == Direction::Desc)
}

pub fn json_to_cql(ty: KeyType, value: &Value) -> Result<CqlValue, String> {
    match ty {
        KeyType::TinyInt => {
            Ok(CqlValue::TinyInt(
                narrow_i64(value, "tinyint", i8::MIN as i64, i8::MAX as i64)? as i8,
            ))
        }
        KeyType::SmallInt => {
            Ok(CqlValue::SmallInt(
                narrow_i64(value, "smallint", i16::MIN as i64, i16::MAX as i64)? as i16,
            ))
        }
        KeyType::Int => Ok(CqlValue::Int(
            narrow_i64(value, "int", i32::MIN as i64, i32::MAX as i64)? as i32,
        )),
        KeyType::BigInt => Ok(CqlValue::BigInt(as_i64(value, "bigint")?)),
        KeyType::Timestamp => Ok(CqlValue::Timestamp(CqlTimestamp(as_i64(
            value,
            "timestamp",
        )?))),
        KeyType::Date => {
            let days = as_i64(value, "date")?;
            let raw = days.checked_add(1 << 31).ok_or("date is out of range")?;
            let raw = u32::try_from(raw).map_err(|_| "date is out of range")?;
            Ok(CqlValue::Date(CqlDate(raw)))
        }
        KeyType::Boolean => {
            let flag = value
                .as_bool()
                .ok_or("boolean partition value must be a boolean")?;
            Ok(CqlValue::Boolean(flag))
        }
        KeyType::Text => {
            let text = value
                .as_str()
                .ok_or("text partition value must be a string")?;
            Ok(CqlValue::Text(text.to_string()))
        }
        KeyType::Ascii => {
            let text = value
                .as_str()
                .ok_or("ascii partition value must be a string")?;
            if !text.is_ascii() {
                return Err("ascii partition value must be ASCII".into());
            }
            Ok(CqlValue::Ascii(text.to_string()))
        }
        KeyType::Blob => {
            let text = value
                .as_str()
                .ok_or("blob partition value must be base64")?;
            Ok(CqlValue::Blob(b64_decode(text)?))
        }
        KeyType::Uuid => {
            let text = value
                .as_str()
                .ok_or("uuid partition value must be a string")?;
            let id = Uuid::parse_str(text).map_err(|_| format!("uuid {text} is not canonical"))?;
            Ok(CqlValue::Uuid(id))
        }
    }
}

fn as_i64(value: &Value, what: &str) -> Result<i64, String> {
    value
        .as_i64()
        .ok_or_else(|| format!("{what} partition value must be an integer"))
}

fn narrow_i64(value: &Value, what: &str, min: i64, max: i64) -> Result<i64, String> {
    let raw = as_i64(value, what)?;
    if raw < min || raw > max {
        return Err(format!("{what} partition value is out of range"));
    }
    Ok(raw)
}

pub fn cql_to_atom(column: &KeyColumn, value: &CqlValue) -> Result<OrderingAtom, String> {
    if column.ty.signed() {
        let raw = signed_of(column.ty, value)?;
        return Ok(OrderingAtom::I64(order_i64(raw, column.direction)));
    }
    let bytes = bytes_of(column.ty, value)?;
    Ok(OrderingAtom::Bytes(canonical_to_atom_bytes(
        &bytes,
        column.direction,
    )))
}

pub fn atom_to_cql(column: &KeyColumn, atom: &OrderingAtom) -> Result<CqlValue, String> {
    if column.ty.signed() {
        let OrderingAtom::I64(encoded) = atom else {
            return Err(format!("cursor column {} is not an integer", column.name));
        };
        let raw = order_i64(*encoded, column.direction);
        return signed_to_cql(column.ty, raw);
    }
    let OrderingAtom::Bytes(encoded) = atom else {
        return Err(format!("cursor column {} is not bytes", column.name));
    };
    let bytes = atom_bytes_to_canonical(encoded, column.direction)?;
    bytes_to_cql(column.ty, bytes)
}

fn signed_of(ty: KeyType, value: &CqlValue) -> Result<i64, String> {
    match (ty, value) {
        (KeyType::TinyInt, CqlValue::TinyInt(v)) => Ok(i64::from(*v)),
        (KeyType::SmallInt, CqlValue::SmallInt(v)) => Ok(i64::from(*v)),
        (KeyType::Int, CqlValue::Int(v)) => Ok(i64::from(*v)),
        (KeyType::BigInt, CqlValue::BigInt(v)) => Ok(*v),
        (KeyType::Timestamp, CqlValue::Timestamp(v)) => Ok(v.0),
        (KeyType::Date, CqlValue::Date(v)) => Ok(i64::from(v.0) - (1 << 31)),
        (KeyType::Boolean, CqlValue::Boolean(v)) => Ok(if *v { 1 } else { 0 }),
        _ => Err("key value does not match the column type".into()),
    }
}

fn signed_to_cql(ty: KeyType, raw: i64) -> Result<CqlValue, String> {
    match ty {
        KeyType::TinyInt => Ok(CqlValue::TinyInt(
            i8::try_from(raw).map_err(|_| "tinyint cursor")?,
        )),
        KeyType::SmallInt => Ok(CqlValue::SmallInt(
            i16::try_from(raw).map_err(|_| "smallint cursor")?,
        )),
        KeyType::Int => Ok(CqlValue::Int(i32::try_from(raw).map_err(|_| "int cursor")?)),
        KeyType::BigInt => Ok(CqlValue::BigInt(raw)),
        KeyType::Timestamp => Ok(CqlValue::Timestamp(CqlTimestamp(raw))),
        KeyType::Date => {
            let shifted = raw.checked_add(1 << 31).ok_or("date cursor")?;
            Ok(CqlValue::Date(CqlDate(
                u32::try_from(shifted).map_err(|_| "date cursor")?,
            )))
        }
        KeyType::Boolean => Ok(CqlValue::Boolean(raw != 0)),
        _ => Err("not a signed key".into()),
    }
}

fn bytes_of(ty: KeyType, value: &CqlValue) -> Result<Vec<u8>, String> {
    match (ty, value) {
        (KeyType::Text, CqlValue::Text(v)) => Ok(v.as_bytes().to_vec()),
        (KeyType::Ascii, CqlValue::Ascii(v)) => Ok(v.as_bytes().to_vec()),
        (KeyType::Blob, CqlValue::Blob(v)) => Ok(v.clone()),
        (KeyType::Uuid, CqlValue::Uuid(v)) => Ok(v.as_bytes().to_vec()),
        _ => Err("key value does not match the column type".into()),
    }
}

fn bytes_to_cql(ty: KeyType, bytes: Vec<u8>) -> Result<CqlValue, String> {
    match ty {
        KeyType::Text => Ok(CqlValue::Text(
            String::from_utf8(bytes).map_err(|_| "text cursor is not utf-8")?,
        )),
        KeyType::Ascii => {
            let text = String::from_utf8(bytes).map_err(|_| "ascii cursor is not utf-8")?;
            if !text.is_ascii() {
                return Err("ascii cursor is not ASCII".into());
            }
            Ok(CqlValue::Ascii(text))
        }
        KeyType::Blob => Ok(CqlValue::Blob(bytes)),
        KeyType::Uuid => Ok(CqlValue::Uuid(
            Uuid::from_slice(&bytes).map_err(|_| "uuid cursor is not 16 bytes")?,
        )),
        _ => Err("not a byte key".into()),
    }
}

pub fn columns_to_ordering(
    columns: &[KeyColumn],
    values: &[CqlValue],
) -> Result<OrderingValue, String> {
    if columns.len() != values.len() {
        return Err("ordering column count does not match the row".into());
    }
    let mut atoms = Vec::with_capacity(columns.len());
    for (column, value) in columns.iter().zip(values) {
        atoms.push(cql_to_atom(column, value)?);
    }
    OrderingValue::new(atoms).map_err(|err| err.to_string())
}

pub fn ordering_to_values(
    columns: &[KeyColumn],
    ordering: &OrderingValue,
) -> Result<Vec<CqlValue>, String> {
    let atoms = ordering.atoms();
    if atoms.len() != columns.len() {
        return Err(format!(
            "cursor has {} values, the key has {}",
            atoms.len(),
            columns.len()
        ));
    }
    let mut out = Vec::with_capacity(columns.len());
    for (column, atom) in columns.iter().zip(atoms) {
        out.push(atom_to_cql(column, atom)?);
    }
    Ok(out)
}

pub fn cql_to_json(value: Option<CqlValue>) -> Result<Value, String> {
    let Some(value) = value else {
        return Ok(Value::Null);
    };
    match value {
        CqlValue::TinyInt(v) => Ok(Value::from(i64::from(v))),
        CqlValue::SmallInt(v) => Ok(Value::from(i64::from(v))),
        CqlValue::Int(v) => Ok(Value::from(i64::from(v))),
        CqlValue::BigInt(v) => Ok(Value::from(v)),
        CqlValue::Counter(v) => Ok(Value::from(v.0)),
        CqlValue::Timestamp(v) => Ok(Value::from(v.0)),
        CqlValue::Date(v) => Ok(Value::from(i64::from(v.0) - (1 << 31))),
        CqlValue::Boolean(v) => Ok(Value::from(v)),
        CqlValue::Float(v) => json_f64(f64::from(v)),
        CqlValue::Double(v) => json_f64(v),
        CqlValue::Text(v) | CqlValue::Ascii(v) => Ok(Value::from(v)),
        CqlValue::Uuid(v) => Ok(Value::from(v.to_string())),
        CqlValue::Blob(v) => Ok(Value::from(b64_encode(&v))),
        other => Ok(Value::from(other.to_string())),
    }
}

fn json_f64(value: f64) -> Result<Value, String> {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| "payload float is not finite".into())
}

pub fn b64_encode(data: &[u8]) -> String {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    let mut index = 0;
    while index + 3 <= data.len() {
        let n = (u32::from(data[index]) << 16)
            | (u32::from(data[index + 1]) << 8)
            | u32::from(data[index + 2]);
        out.push(TABLE[((n >> 18) & 63) as usize] as char);
        out.push(TABLE[((n >> 12) & 63) as usize] as char);
        out.push(TABLE[((n >> 6) & 63) as usize] as char);
        out.push(TABLE[(n & 63) as usize] as char);
        index += 3;
    }
    let rest = data.len() - index;
    if rest == 1 {
        let n = u32::from(data[index]) << 16;
        out.push(TABLE[((n >> 18) & 63) as usize] as char);
        out.push(TABLE[((n >> 12) & 63) as usize] as char);
        out.push('=');
        out.push('=');
    } else if rest == 2 {
        let n = (u32::from(data[index]) << 16) | (u32::from(data[index + 1]) << 8);
        out.push(TABLE[((n >> 18) & 63) as usize] as char);
        out.push(TABLE[((n >> 12) & 63) as usize] as char);
        out.push(TABLE[((n >> 6) & 63) as usize] as char);
        out.push('=');
    }
    out
}

pub fn b64_decode(text: &str) -> Result<Vec<u8>, String> {
    fn val(byte: u8) -> Result<u8, String> {
        match byte {
            b'A'..=b'Z' => Ok(byte - b'A'),
            b'a'..=b'z' => Ok(byte - b'a' + 26),
            b'0'..=b'9' => Ok(byte - b'0' + 52),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => Err("blob is not base64".into()),
        }
    }
    let bytes = text.as_bytes();
    if bytes.len() % 4 != 0 {
        return Err("blob is not base64".into());
    }
    let mut out = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        let pad = (bytes[index + 2] == b'=') as usize + (bytes[index + 3] == b'=') as usize;
        if pad == 1 && bytes[index + 3] != b'=' {
            return Err("blob is not base64".into());
        }
        let a = val(bytes[index])?;
        let b = val(bytes[index + 1])?;
        let c = if pad == 2 { 0 } else { val(bytes[index + 2])? };
        let d = if pad >= 1 { 0 } else { val(bytes[index + 3])? };
        let n = (u32::from(a) << 18) | (u32::from(b) << 12) | (u32::from(c) << 6) | u32::from(d);
        out.push((n >> 16) as u8);
        if pad < 2 {
            out.push((n >> 8) as u8);
        }
        if pad < 1 {
            out.push(n as u8);
        }
        index += 4;
    }
    Ok(out)
}

pub fn cmp_op(direction: Direction) -> &'static str {
    match direction {
        Direction::Asc => ">",
        Direction::Desc => "<",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn neither_partition_nor_scan_is_rejected() {
        let err = SourceSpec::parse(&json!({"table": "events"})).unwrap_err();
        assert!(err.contains("scan"), "{err}");
    }

    #[test]
    fn partition_and_scan_together_are_rejected() {
        let err = SourceSpec::parse(&json!({
            "table": "events",
            "partition": {"bucket": 0},
            "scan": "token",
        }))
        .unwrap_err();
        assert!(err.contains("both"), "{err}");
    }

    #[test]
    fn allow_filtering_is_not_a_field() {
        let err = SourceSpec::parse(&json!({
            "table": "events",
            "partition": {"bucket": 0},
            "allow_filtering": true,
        }))
        .unwrap_err();
        assert!(err.contains("ALLOW FILTERING"), "{err}");
    }

    #[test]
    fn descending_i64_increases_as_values_fall() {
        let column = KeyColumn {
            name: "id".into(),
            ty: KeyType::BigInt,
            direction: Direction::Desc,
        };
        let high = cql_to_atom(&column, &CqlValue::BigInt(3)).unwrap();
        let mid = cql_to_atom(&column, &CqlValue::BigInt(2)).unwrap();
        let low = cql_to_atom(&column, &CqlValue::BigInt(1)).unwrap();
        assert!(high < mid && mid < low);
        assert_eq!(atom_to_cql(&column, &high).unwrap(), CqlValue::BigInt(3));
    }

    #[test]
    fn descending_text_reverses_including_a_prefix() {
        let column = KeyColumn {
            name: "name".into(),
            ty: KeyType::Text,
            direction: Direction::Desc,
        };
        let a = cql_to_atom(&column, &CqlValue::Text("a".into())).unwrap();
        let aa = cql_to_atom(&column, &CqlValue::Text("aa".into())).unwrap();
        let b = cql_to_atom(&column, &CqlValue::Text("b".into())).unwrap();
        assert!(b < aa && aa < a);
        assert_eq!(
            atom_to_cql(&column, &aa).unwrap(),
            CqlValue::Text("aa".into())
        );
    }

    #[test]
    fn blob_json_is_base64() {
        let encoded = b64_encode(&[0, 1, 255]);
        assert_eq!(b64_decode(&encoded).unwrap(), vec![0, 1, 255]);
        let json = cql_to_json(Some(CqlValue::Blob(vec![0, 1, 255]))).unwrap();
        assert_eq!(json, Value::from(encoded));
    }
}
