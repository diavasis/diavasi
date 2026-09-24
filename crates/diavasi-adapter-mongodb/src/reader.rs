use crate::catalog::ensure_contract;
use crate::connect::{MongoEndpoint, connect};
use crate::spec::{
    FieldType, OrderField, SourceSpec, atom_bytes_to_canonical, canonical_to_atom_bytes, order_i64,
    query_filter,
};
use bytes::Bytes;
use diavasi::core::{
    LogicalCursor, OrderingAtom, OrderingValue, Record, RecordSource, SourceError,
};
use diavasi::runtime::SourceOpen;
use futures::future::BoxFuture;
use mongodb::Client;
use mongodb::bson::binary::Binary;
use mongodb::bson::oid::ObjectId;
use mongodb::bson::spec::BinarySubtype;
use mongodb::bson::{Bson, DateTime, Document};

pub struct MongoSource {
    client: Client,
    endpoint: MongoEndpoint,
    spec: SourceSpec,
}

impl MongoSource {
    pub async fn open(request: SourceOpen) -> Result<Self, String> {
        let spec = SourceSpec::parse(&request.source_spec)?;
        let mut endpoint = MongoEndpoint::from_request(&request)?;
        endpoint.app_name = format!("diavasi:{}", spec.collection);
        let client = connect(&endpoint).await?;
        ensure_contract(&client, &endpoint.database, &spec).await?;
        Ok(Self {
            client,
            endpoint,
            spec,
        })
    }

    /// Point the next command at an unreachable address so the retry path runs.
    #[cfg(test)]
    pub async fn poison(&mut self) -> Result<(), String> {
        use std::time::Duration;

        use mongodb::options::ClientOptions;

        let mut options = ClientOptions::parse("mongodb://127.0.0.1:1")
            .await
            .map_err(|err| err.to_string())?;
        options.direct_connection = Some(true);
        options.server_selection_timeout = Some(Duration::from_millis(200));
        options.connect_timeout = Some(Duration::from_millis(200));
        self.client = Client::with_options(options).map_err(|err| err.to_string())?;
        Ok(())
    }

    async fn find(
        &self,
        client: &Client,
        cursor: &LogicalCursor,
        limit: usize,
    ) -> Result<Vec<Document>, String> {
        let filter = query_filter(&self.spec, cursor)?;
        let sort = self.spec.sort_document();
        let limit = i64::try_from(limit).map_err(|_| "limit overflow")?;
        let collection = client
            .database(&self.endpoint.database)
            .collection::<Document>(&self.spec.collection);
        let mut find = collection.find(filter).sort(sort).limit(limit);
        if let Some(projection) = self.spec.projection() {
            find = find.projection(projection);
        }
        let mut cursor = find.await.map_err(|err| err.to_string())?;
        let mut docs = Vec::new();
        while cursor.advance().await.map_err(|err| err.to_string())? {
            docs.push(
                cursor
                    .deserialize_current()
                    .map_err(|err| err.to_string())?,
            );
        }
        Ok(docs)
    }

    async fn fetch(
        &mut self,
        cursor: &LogicalCursor,
        limit: usize,
    ) -> Result<Vec<Record>, SourceError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let docs = match self.find(&self.client.clone(), cursor, limit).await {
            Ok(docs) => docs,
            Err(err) => {
                tracing::warn!("mongodb fetch failed, reconnecting: {err}");
                self.client = connect(&self.endpoint).await.map_err(SourceError)?;
                self.find(&self.client.clone(), cursor, limit)
                    .await
                    .map_err(SourceError)?
            }
        };
        docs.into_iter()
            .map(|doc| decode_document(&self.spec, doc).map_err(SourceError))
            .collect()
    }
}

impl RecordSource for MongoSource {
    fn fetch_after<'a>(
        &'a mut self,
        cursor: &'a LogicalCursor,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Record>, SourceError>> {
        Box::pin(async move { self.fetch(cursor, limit).await })
    }
}

pub fn atom_to_bson(field: &OrderField, atom: &OrderingAtom) -> Result<Bson, String> {
    match (field.ty, atom) {
        (FieldType::Int32, OrderingAtom::I64(value)) => {
            let value = i32::try_from(order_i64(*value, field.direction))
                .map_err(|_| "int32 cursor out of range")?;
            Ok(Bson::Int32(value))
        }
        (FieldType::Int64, OrderingAtom::I64(value)) => {
            Ok(Bson::Int64(order_i64(*value, field.direction)))
        }
        (FieldType::Date, OrderingAtom::I64(millis)) => Ok(Bson::DateTime(DateTime::from_millis(
            order_i64(*millis, field.direction),
        ))),
        (FieldType::Bool, OrderingAtom::I64(value)) => match order_i64(*value, field.direction) {
            0 => Ok(Bson::Boolean(false)),
            1 => Ok(Bson::Boolean(true)),
            _ => Err("bool cursor must be 0 or 1".into()),
        },
        (FieldType::ObjectId, OrderingAtom::Bytes(bytes)) => {
            let bytes = atom_bytes_to_canonical(bytes, field.direction)?;
            let bytes: [u8; 12] = bytes
                .as_slice()
                .try_into()
                .map_err(|_| "objectId cursor must be 12 bytes")?;
            Ok(Bson::ObjectId(ObjectId::from_bytes(bytes)))
        }
        (FieldType::String, OrderingAtom::Bytes(bytes)) => {
            let bytes = atom_bytes_to_canonical(bytes, field.direction)?;
            let text = String::from_utf8(bytes).map_err(|_| "string cursor is not utf-8")?;
            Ok(Bson::String(text))
        }
        (FieldType::BinData, OrderingAtom::Bytes(bytes)) => {
            let bytes = atom_bytes_to_canonical(bytes, field.direction)?;
            let (subtype, payload) = bytes.split_first().ok_or("binData cursor is empty")?;
            Ok(Bson::Binary(Binary {
                subtype: BinarySubtype::from(*subtype),
                bytes: payload.to_vec(),
            }))
        }
        _ => Err("cursor atom does not match the order field type".into()),
    }
}

fn decode_document(spec: &SourceSpec, doc: Document) -> Result<Record, String> {
    let mut atoms = Vec::with_capacity(spec.order_by.len());
    for field in &spec.order_by {
        atoms.push(read_atom(field, doc.get(&field.field))?);
    }
    let value = Bson::Document(doc).into_relaxed_extjson();
    let payload = serde_json::to_vec(&value).map_err(|err| err.to_string())?;
    Ok(Record {
        ordering: OrderingValue::new(atoms).map_err(|err| err.to_string())?,
        payload: Bytes::from(payload),
    })
}

fn read_atom(field: &OrderField, value: Option<&Bson>) -> Result<OrderingAtom, String> {
    let Some(value) = value else {
        return Err(format!("order field {} is missing", field.field));
    };
    match (field.ty, value) {
        (_, Bson::Null) => Err(format!("order field {} is null", field.field)),
        (FieldType::Int32, Bson::Int32(value)) => Ok(OrderingAtom::I64(order_i64(
            i64::from(*value),
            field.direction,
        ))),
        (FieldType::Int64, Bson::Int64(value)) => {
            Ok(OrderingAtom::I64(order_i64(*value, field.direction)))
        }
        (FieldType::Date, Bson::DateTime(value)) => Ok(OrderingAtom::I64(order_i64(
            value.timestamp_millis(),
            field.direction,
        ))),
        (FieldType::Bool, Bson::Boolean(value)) => Ok(OrderingAtom::I64(order_i64(
            i64::from(*value),
            field.direction,
        ))),
        (FieldType::ObjectId, Bson::ObjectId(value)) => Ok(OrderingAtom::Bytes(
            canonical_to_atom_bytes(&value.bytes(), field.direction),
        )),
        (FieldType::String, Bson::String(value)) => Ok(OrderingAtom::Bytes(
            canonical_to_atom_bytes(value.as_bytes(), field.direction),
        )),
        (FieldType::BinData, Bson::Binary(value)) => {
            let mut bytes = Vec::with_capacity(1 + value.bytes.len());
            bytes.push(u8::from(value.subtype));
            bytes.extend_from_slice(&value.bytes);
            Ok(OrderingAtom::Bytes(canonical_to_atom_bytes(
                &bytes,
                field.direction,
            )))
        }
        _ => Err(format!(
            "order field {} has the wrong BSON type",
            field.field
        )),
    }
}
