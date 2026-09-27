use diavasi::core::encoding;
use diavasi::runtime::parse_json;
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

/// The `source_spec` JSON as written. Checked into a [`SourceSpec`].
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSpec {
    collection: String,
    #[serde(default)]
    order_by: Option<Vec<RawOrderField>>,
    #[serde(default)]
    fields: Option<Vec<String>>,
    #[serde(default)]
    filter: Option<serde_json::Map<String, Value>>,
    #[serde(default)]
    acknowledge_unsafe: Option<bool>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RawOrderField {
    field: String,
    #[serde(rename = "type")]
    ty: String,
    direction: String,
}

impl SourceSpec {
    pub fn parse(value: &Value) -> Result<Self, String> {
        let raw: RawSpec = parse_json(value, "source_spec")?;
        check_collection(&raw.collection)?;
        let order_by = match raw.order_by {
            None => vec![OrderField {
                field: "_id".into(),
                ty: FieldType::ObjectId,
                direction: Direction::Asc,
            }],
            Some(items) if items.is_empty() => {
                return Err("source_spec.order_by must be non-empty".into());
            }
            Some(items) => items
                .into_iter()
                .map(|item| {
                    check_field(&item.field)?;
                    Ok(OrderField {
                        field: item.field,
                        ty: FieldType::parse(&item.ty)?,
                        direction: Direction::parse(&item.direction)?,
                    })
                })
                .collect::<Result<_, String>>()?,
        };
        if let Some(names) = &raw.fields {
            for name in names {
                check_field(name)?;
            }
        }
        let filter = match raw.filter {
            None => None,
            Some(map) => {
                let filter = Value::Object(map);
                reject_filter_ops(&filter)?;
                Some(mongodb::bson::to_document(&filter).map_err(|err| err.to_string())?)
            }
        };
        Ok(Self {
            collection: raw.collection,
            order_by,
            fields: raw.fields,
            filter,
            acknowledge_unsafe: raw.acknowledge_unsafe.unwrap_or(false),
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

/// MongoDB allows dots in collection names (`app.events`). It reserves `$`,
/// NUL, and the `system.` prefix.
fn check_collection(name: &str) -> Result<(), String> {
    if name.is_empty() || name.contains(['$', '\0']) || name.starts_with("system.") {
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

    /// S2: a misspelled key is an error, not an option left at its default.
    #[test]
    fn rejects_unknown_keys() {
        let err = SourceSpec::parse(&json!({"collection": "events", "filtr": {}})).unwrap_err();
        assert!(err.contains("filtr"), "{err}");
        let err = SourceSpec::parse(&json!({
            "collection": "events",
            "order_by": [{"field": "_id", "type": "objectId", "direction": "asc", "x": 1}],
        }))
        .unwrap_err();
        assert!(err.contains("x"), "{err}");
    }
}
