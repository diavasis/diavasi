use mongodb::bson::{Document, doc};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    ObjectId,
    Int32,
    Int64,
    String,
    Date,
    Bool,
    BinData,
}

impl FieldType {
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "objectId" => Ok(Self::ObjectId),
            "int32" => Ok(Self::Int32),
            "int64" => Ok(Self::Int64),
            "string" => Ok(Self::String),
            "date" => Ok(Self::Date),
            "bool" => Ok(Self::Bool),
            "binData" => Ok(Self::BinData),
            other => Err(format!("unsupported BSON type {other}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Asc,
    Desc,
}

impl Direction {
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "asc" => Ok(Self::Asc),
            "desc" => Ok(Self::Desc),
            other => Err(format!("unsupported direction {other}")),
        }
    }

    pub fn mongo(self) -> i32 {
        match self {
            Self::Asc => 1,
            Self::Desc => -1,
        }
    }

    pub fn inequality(self) -> &'static str {
        match self {
            Self::Asc => "$gt",
            Self::Desc => "$lt",
        }
    }
}

/// Map a signed value so a descending field still increases along the stream.
pub fn order_i64(value: i64, direction: Direction) -> i64 {
    match direction {
        Direction::Asc => value,
        Direction::Desc => !value,
    }
}

/// Bytes the cursor stores. Ascending keeps the canonical bytes. Descending
/// stores the bitwise complement of a memcomparable encoding, which reverses
/// order including the case where one value is a prefix of the other.
pub fn canonical_to_atom_bytes(bytes: &[u8], direction: Direction) -> Vec<u8> {
    match direction {
        Direction::Asc => bytes.to_vec(),
        Direction::Desc => encode_memcomparable(bytes)
            .into_iter()
            .map(|b| !b)
            .collect(),
    }
}

pub fn atom_bytes_to_canonical(bytes: &[u8], direction: Direction) -> Result<Vec<u8>, String> {
    match direction {
        Direction::Asc => Ok(bytes.to_vec()),
        Direction::Desc => {
            let flipped: Vec<u8> = bytes.iter().copied().map(|b| !b).collect();
            decode_memcomparable(&flipped)
        }
    }
}

fn encode_memcomparable(src: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut index = 0;
    loop {
        let remain = src.len().saturating_sub(index);
        let n = remain.min(8);
        let mut group = [0u8; 9];
        if n > 0 {
            group[..n].copy_from_slice(&src[index..index + n]);
        }
        group[8] = n as u8;
        buf.extend_from_slice(&group);
        if n < 8 {
            break;
        }
        index += 8;
    }
    buf
}

fn decode_memcomparable(src: &[u8]) -> Result<Vec<u8>, String> {
    if src.is_empty() || src.len() % 9 != 0 {
        return Err("bad ordered bytes".into());
    }
    let mut out = Vec::new();
    for group in src.chunks_exact(9) {
        let n = group[8] as usize;
        if n > 8 {
            return Err("bad ordered bytes".into());
        }
        if group[n..8].iter().any(|byte| *byte != 0) {
            return Err("bad ordered bytes".into());
        }
        out.extend_from_slice(&group[..n]);
        if n < 8 {
            return Ok(out);
        }
    }
    Err("ordered bytes ended on a full group".into())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderField {
    pub field: String,
    pub ty: FieldType,
    pub direction: Direction,
}

#[derive(Debug, Clone)]
pub struct SourceSpec {
    pub collection: String,
    pub order_by: Vec<OrderField>,
    pub fields: Option<Vec<String>>,
    pub filter: Option<Document>,
    pub acknowledge_unsafe: bool,
}

impl SourceSpec {
    pub fn parse(value: &Value) -> Result<Self, String> {
        let collection = value
            .get("collection")
            .and_then(|v| v.as_str())
            .ok_or("source_spec.collection is required")?;
        check_collection(collection)?;
        let order_by = match value.get("order_by") {
            None => vec![OrderField {
                field: "_id".into(),
                ty: FieldType::ObjectId,
                direction: Direction::Asc,
            }],
            Some(Value::Array(items)) if items.is_empty() => {
                return Err("source_spec.order_by must be non-empty".into());
            }
            Some(Value::Array(items)) => items.iter().map(parse_order).collect::<Result<_, _>>()?,
            Some(_) => return Err("source_spec.order_by must be an array".into()),
        };
        let fields = match value.get("fields") {
            None | Some(Value::Null) => None,
            Some(Value::Array(items)) => {
                let mut names = Vec::new();
                for item in items {
                    let name = item.as_str().ok_or("fields entries must be strings")?;
                    check_field(name)?;
                    names.push(name.to_string());
                }
                Some(names)
            }
            Some(_) => return Err("source_spec.fields must be an array".into()),
        };
        let filter = match value.get("filter") {
            None | Some(Value::Null) => None,
            Some(other) => {
                if !other.is_object() {
                    return Err("source_spec.filter must be an object".into());
                }
                reject_filter_ops(other)?;
                Some(mongodb::bson::to_document(other).map_err(|err| err.to_string())?)
            }
        };
        let acknowledge_unsafe = match value.get("acknowledge_unsafe") {
            None | Some(Value::Null) => false,
            Some(Value::Bool(flag)) => *flag,
            Some(_) => return Err("source_spec.acknowledge_unsafe must be a bool".into()),
        };
        Ok(Self {
            collection: collection.to_string(),
            order_by,
            fields,
            filter,
            acknowledge_unsafe,
        })
    }

    pub fn sort_document(&self) -> Document {
        let mut sort = Document::new();
        for field in &self.order_by {
            sort.insert(field.field.clone(), field.direction.mongo());
        }
        sort
    }

    pub fn projection(&self) -> Option<Document> {
        let fields = self.fields.as_ref()?;
        let mut projection = Document::new();
        for name in fields {
            projection.insert(name.clone(), 1);
        }
        for field in &self.order_by {
            projection.insert(field.field.clone(), 1);
        }
        Some(projection)
    }
}

fn parse_order(value: &Value) -> Result<OrderField, String> {
    let field = value
        .get("field")
        .and_then(|v| v.as_str())
        .ok_or("order_by.field is required")?;
    check_field(field)?;
    let ty = value
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or("order_by.type is required")?;
    let direction = value
        .get("direction")
        .and_then(|v| v.as_str())
        .ok_or("order_by.direction is required")?;
    Ok(OrderField {
        field: field.to_string(),
        ty: FieldType::parse(ty)?,
        direction: Direction::parse(direction)?,
    })
}

fn check_collection(name: &str) -> Result<(), String> {
    if name.is_empty() || name.contains(['$', '\0', '.']) || name.starts_with("system.") {
        return Err(format!("invalid collection name {name}"));
    }
    Ok(())
}

fn check_field(name: &str) -> Result<(), String> {
    if name.is_empty() || name.contains(['$', '\0', '.']) {
        return Err(format!("invalid field name {name}"));
    }
    Ok(())
}

fn reject_filter_ops(value: &Value) -> Result<(), String> {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if key == "$where" || key == "$function" {
                    return Err(format!("{key} is not allowed in filter"));
                }
                reject_filter_ops(child)?;
            }
            Ok(())
        }
        Value::Array(items) => {
            for item in items {
                reject_filter_ops(item)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub fn resume_filter(
    spec: &SourceSpec,
    atoms: &[diavasi::core::OrderingAtom],
) -> Result<Document, String> {
    use crate::reader::atom_to_bson;
    if atoms.len() != spec.order_by.len() {
        return Err("cursor width does not match order_by".into());
    }
    let mut branches = Vec::new();
    for index in 0..spec.order_by.len() {
        let mut branch = Document::new();
        for (field, atom) in spec.order_by.iter().zip(atoms.iter()).take(index) {
            branch.insert(field.field.clone(), atom_to_bson(field, atom)?);
        }
        let field = &spec.order_by[index];
        branch.insert(
            field.field.clone(),
            doc! { field.direction.inequality(): atom_to_bson(field, &atoms[index])? },
        );
        branches.push(branch);
    }
    Ok(doc! { "$or": branches })
}

pub fn query_filter(
    spec: &SourceSpec,
    cursor: &diavasi::core::LogicalCursor,
) -> Result<Document, String> {
    let resume = match cursor {
        None => None,
        Some(cursor) => Some(resume_filter(spec, cursor.atoms())?),
    };
    Ok(match (&spec.filter, resume) {
        (None, None) => Document::new(),
        (Some(filter), None) => filter.clone(),
        (None, Some(resume)) => resume,
        (Some(filter), Some(resume)) => doc! { "$and": [filter.clone(), resume] },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use diavasi::core::OrderingAtom;
    use serde_json::json;

    #[test]
    fn omitted_order_is_id_ascending() {
        let spec = SourceSpec::parse(&json!({"collection": "events"})).unwrap();
        assert_eq!(spec.order_by.len(), 1);
        assert_eq!(spec.order_by[0].field, "_id");
        assert_eq!(spec.order_by[0].ty, FieldType::ObjectId);
        assert_eq!(spec.order_by[0].direction, Direction::Asc);
        assert!(!spec.acknowledge_unsafe);
    }

    #[test]
    fn rejects_unsafe_filter_and_unknown_type() {
        let err = SourceSpec::parse(&json!({
            "collection": "events",
            "filter": { "$where": "this.a > 1" }
        }))
        .unwrap_err();
        assert!(err.contains("$where"), "{err}");
        let err = SourceSpec::parse(&json!({
            "collection": "events",
            "order_by": [{ "field": "n", "type": "double", "direction": "asc" }]
        }))
        .unwrap_err();
        assert!(err.contains("double"), "{err}");
    }

    #[test]
    fn mixed_direction_keyset() {
        let spec = SourceSpec::parse(&json!({
            "collection": "events",
            "order_by": [
                { "field": "k", "type": "int64", "direction": "asc" },
                { "field": "name", "type": "string", "direction": "desc" }
            ]
        }))
        .unwrap();
        let name = canonical_to_atom_bytes(b"b", Direction::Desc);
        let filter =
            resume_filter(&spec, &[OrderingAtom::I64(1), OrderingAtom::Bytes(name)]).unwrap();
        let or = filter.get_array("$or").unwrap();
        assert_eq!(or.len(), 2);
        let first = or[0].as_document().unwrap();
        assert!(first.get_document("k").unwrap().contains_key("$gt"));
        let second = or[1].as_document().unwrap();
        assert_eq!(second.get_i64("k").unwrap(), 1);
        assert_eq!(
            second.get_document("name").unwrap().get_str("$lt").unwrap(),
            "b"
        );
    }

    #[test]
    fn descending_bytes_reverse_order_and_round_trip() {
        let samples: &[&[u8]] = &[
            b"",
            b"a",
            b"aa",
            b"b",
            &[0],
            &[0, 0],
            &[0xff],
            &[0xff, 0x00],
            b"hello",
        ];
        for left in samples {
            let back = atom_bytes_to_canonical(
                &canonical_to_atom_bytes(left, Direction::Desc),
                Direction::Desc,
            )
            .unwrap();
            assert_eq!(&back, left);
            for right in samples {
                if left == right {
                    continue;
                }
                let raw = left.cmp(right);
                let encoded = canonical_to_atom_bytes(left, Direction::Desc)
                    .cmp(&canonical_to_atom_bytes(right, Direction::Desc));
                assert_eq!(encoded, raw.reverse(), "{left:?} vs {right:?}");
            }
        }
        assert_eq!(order_i64(5, Direction::Desc), !5);
        assert_eq!(order_i64(!5, Direction::Desc), 5);
    }
}
