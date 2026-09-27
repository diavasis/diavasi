use diavasi::core::{LogicalCursor, OrderingAtom, OrderingValue};

/// Redis stream read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSpec {
    /// Stream key.
    pub stream: String,
    /// Fields copied into the payload. `None` copies every field.
    pub fields: Option<Vec<String>>,
}

/// The `source_spec` JSON as written. Checked into a [`SourceSpec`].
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSpec {
    stream: String,
    // `group` named a Redis consumer group before reads became XRANGE.
    // Stored specs still carry it, so it is accepted and ignored.
    #[serde(default, rename = "group")]
    _group: Option<String>,
    #[serde(default)]
    fields: Option<Vec<String>>,
}

impl SourceSpec {
    pub fn parse(value: &serde_json::Value) -> Result<Self, String> {
        let raw: RawSpec = diavasi::runtime::parse_json(value, "source_spec")?;
        if raw.stream.is_empty() {
            return Err("stream is required".into());
        }
        if raw
            .fields
            .as_ref()
            .is_some_and(|names| names.iter().any(String::is_empty))
        {
            return Err("fields entries must be non-empty strings".into());
        }
        Ok(Self {
            stream: raw.stream,
            fields: raw.fields,
        })
    }
}

/// The cursor as `(milliseconds, sequence)`, or `None` at the start.
pub fn cursor_to_pair(cursor: &LogicalCursor) -> Result<Option<(u64, u64)>, String> {
    match cursor {
        None => Ok(None),
        Some(value) => match value.atoms() {
            [OrderingAtom::U64(ms), OrderingAtom::U64(seq)] => Ok(Some((*ms, *seq))),
            _ => Err("redis cursor must be milliseconds and sequence".into()),
        },
    }
}

/// Parse `milliseconds-sequence`.
pub fn id_to_pair(id: &str) -> Result<(u64, u64), String> {
    match id_to_ordering(id)?.atoms() {
        [OrderingAtom::U64(ms), OrderingAtom::U64(seq)] => Ok((*ms, *seq)),
        _ => unreachable!("id_to_ordering returns two U64 atoms"),
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
        }))
        .unwrap();
        assert_eq!(spec.stream, "events");
        assert!(spec.fields.is_none());
    }

    #[test]
    fn accepts_a_legacy_group_and_rejects_a_bad_field_list() {
        let legacy = SourceSpec::parse(&serde_json::json!({"stream": "events", "group": "g"}));
        assert_eq!(legacy.unwrap().stream, "events");
        let bad_group = SourceSpec::parse(&serde_json::json!({"stream": "events", "group": 1}));
        assert!(bad_group.unwrap_err().contains("group"));
        let fields = SourceSpec::parse(&serde_json::json!({
            "stream": "events",
            "group": "g",
            "fields": "body",
        }));
        assert!(fields.unwrap_err().contains("sequence"));
        let empty = SourceSpec::parse(&serde_json::json!({"stream": "events", "fields": [""]}));
        assert!(empty.unwrap_err().contains("non-empty"));
        let blank = SourceSpec::parse(&serde_json::json!({"stream": ""}));
        assert!(blank.unwrap_err().contains("stream"));
    }

    #[test]
    fn stream_ids_compare_numerically() {
        let earlier = id_to_ordering("9-1").unwrap();
        let later = id_to_ordering("10-0").unwrap();
        assert!(earlier < later);
        assert_eq!(cursor_to_pair(&None).unwrap(), None);
        assert_eq!(cursor_to_pair(&Some(later.clone())).unwrap(), Some((10, 0)));
        assert_eq!(id_to_pair("10-0").unwrap(), (10, 0));
        assert!(id_to_ordering("nope").is_err());
    }

    /// S2: a misspelled key is an error, not an option left at its default.
    #[test]
    fn rejects_unknown_keys() {
        let err = SourceSpec::parse(&serde_json::json!({"stream": "events", "feilds": ["a"]}))
            .unwrap_err();
        assert!(err.contains("feilds"), "{err}");
    }
}
