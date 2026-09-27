use mongodb::Client;
use mongodb::IndexModel;
use mongodb::bson::{Bson, Document};

use crate::spec::{Direction, SourceSpec};

pub async fn ensure_contract(
    client: &Client,
    database: &str,
    spec: &SourceSpec,
) -> Result<(), String> {
    let db = client.database(database);
    let names = db
        .list_collection_names()
        .await
        .map_err(|err| err.to_string())?;
    if !names.iter().any(|name| name == &spec.collection) {
        return Err(format!("collection {} was not found", spec.collection));
    }
    if spec.acknowledge_unsafe {
        return Ok(());
    }
    let collection = db.collection::<Document>(&spec.collection);
    let mut cursor = collection
        .list_indexes()
        .await
        .map_err(|err| err.to_string())?;
    let mut matched = false;
    while cursor.advance().await.map_err(|err| err.to_string())? {
        let model = cursor
            .deserialize_current()
            .map_err(|err| err.to_string())?;
        if counts_as_unique(&model) && covers(&model.keys, spec) {
            matched = true;
            break;
        }
    }
    if !matched {
        return Err(
            "order fields must start with every field of a unique index; set acknowledge_unsafe to accept the risk"
                .into(),
        );
    }
    Ok(())
}

fn counts_as_unique(model: &IndexModel) -> bool {
    let options = model.options.as_ref();
    if options.and_then(|opts| opts.sparse).unwrap_or(false) {
        return false;
    }
    if options
        .and_then(|opts| opts.partial_filter_expression.as_ref())
        .is_some()
    {
        return false;
    }
    if options.and_then(|opts| opts.hidden).unwrap_or(false) {
        return false;
    }
    if options.and_then(|opts| opts.unique).unwrap_or(false) {
        return true;
    }
    if options.and_then(|opts| opts.name.as_deref()) == Some("_id_") {
        return true;
    }
    is_builtin_id(&model.keys)
}

fn is_builtin_id(keys: &Document) -> bool {
    let mut fields = keys.iter();
    match (fields.next(), fields.next()) {
        (Some((name, value)), None) if name == "_id" => {
            direction_of(value) == Some(Direction::Asc.mongo())
        }
        _ => false,
    }
}

/// True when the sort begins with every field of the index key, in key order.
/// Only then is the sort tuple unique. Uniqueness does not depend on direction,
/// so only field names are compared.
fn covers(keys: &Document, spec: &SourceSpec) -> bool {
    if keys.is_empty() || keys.len() > spec.order_by.len() {
        return false;
    }
    keys.iter()
        .zip(spec.order_by.iter())
        .all(|((name, value), wanted)| name == &wanted.field && direction_of(value).is_some())
}

fn direction_of(value: &Bson) -> Option<i32> {
    match value {
        Bson::Int32(value) => Some(*value),
        Bson::Int64(value) => i32::try_from(*value).ok(),
        Bson::Double(value) if *value == 1.0 || *value == -1.0 => Some(*value as i32),
        _ => None,
    }
}
