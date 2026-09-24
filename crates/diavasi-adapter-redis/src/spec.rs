use diavasi::core::{LogicalCursor, OrderingAtom, OrderingValue};

/// Redis stream read. The group name is the Redis consumer group, not the Diavasi group id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSpec {
    pub stream: String,
    pub group: String,
    pub fields: Option<Vec<String>>,
}

impl SourceSpec {
    pub fn parse(value: &serde_json::Value) -> Result<Self, String> {
        let object = value.as_object().ok_or("source_spec must be an object")?;
        let stream = required_name(object, "stream")?;
        let group = required_name(object, "group")?;
        let fields = match object.get("fields") {
            None => None,
            Some(serde_json::Value::Array(items)) => {
                let mut names = Vec::with_capacity(items.len());
                for item in items {
                    let name = item
                        .as_str()
                        .filter(|name| !name.is_empty())
                        .ok_or("fields entries must be non-empty strings")?;
                    names.push(name.to_string());
                }
                Some(names)
            }
            Some(_) => return Err("fields must be an array of strings".into()),
        };
        Ok(Self {
            stream,
            group,
            fields,
        })
    }
}

fn required_name(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<String, String> {
    object
        .get(key)
        .and_then(|value| value.as_str())
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .ok_or_else(|| format!("{key} is required"))
}

/// Empty cursor is `0-0`, which is strictly before every id Redis will accept.
pub fn cursor_to_id(cursor: &LogicalCursor) -> Result<String, String> {
    match cursor {
        None => Ok("0-0".into()),
        Some(value) => match value.atoms() {
            [OrderingAtom::U64(ms), OrderingAtom::U64(seq)] => Ok(format!("{ms}-{seq}")),
            _ => Err("redis cursor must be milliseconds and sequence".into()),
        },
    }
}

pub fn id_to_ordering(id: &str) -> Result<OrderingValue, String> {
    let (ms, seq) = id
        .split_once('-')
        .filter(|(ms, seq)| !ms.is_empty() && !seq.is_empty() && !seq.contains('-'))
        .ok_or("stream id must be milliseconds-sequence")?;
    let ms: u64 = ms.parse().map_err(|_| format!("bad stream id {id}"))?;
    let seq: u64 = seq.parse().map_err(|_| format!("bad stream id {id}"))?;
    OrderingValue::new(vec![OrderingAtom::U64(ms), OrderingAtom::U64(seq)])
        .map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omitted_fields_reads_the_whole_entry() {
        let spec = SourceSpec::parse(&serde_json::json!({
            "stream": "events",
            "group": "diavasi",
        }))
        .unwrap();
        assert_eq!(spec.stream, "events");
        assert_eq!(spec.group, "diavasi");
        assert!(spec.fields.is_none());
    }

    #[test]
    fn rejects_a_missing_group_and_a_bad_field_list() {
        let missing = SourceSpec::parse(&serde_json::json!({"stream": "events"}));
        assert!(missing.unwrap_err().contains("group"));
        let fields = SourceSpec::parse(&serde_json::json!({
            "stream": "events",
            "group": "g",
            "fields": "body",
        }));
        assert!(fields.unwrap_err().contains("array"));
    }

    #[test]
    fn stream_ids_compare_numerically() {
        let earlier = id_to_ordering("9-1").unwrap();
        let later = id_to_ordering("10-0").unwrap();
        assert!(earlier < later);
        assert_eq!(cursor_to_id(&None).unwrap(), "0-0");
        assert_eq!(cursor_to_id(&Some(later.clone())).unwrap(), "10-0");
        assert!(id_to_ordering("nope").is_err());
    }
}
