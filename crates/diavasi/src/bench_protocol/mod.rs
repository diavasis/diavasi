//! The transport benchmark's wire protocol (`diavasi.bench.v1`).
//!
//! Used only by [`crate::transport_bench`]. The production protocol is
//! [`crate::dataplane`] (`diavasi.data.v1`).

pub mod pb {
    // tonic::Status is large; generated gRPC stubs trip clippy::result_large_err on 1.98+.
    #![allow(clippy::result_large_err)]
    tonic::include_proto!("diavasi.bench.v1");
}

pub use pb::*;

pub const PROTOCOL_VERSION: u32 = 1;

impl Envelope {
    pub fn join_group(group_id: impl Into<String>, consumer_id: impl Into<String>) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            body: Some(envelope::Body::JoinGroup(JoinGroup {
                group_id: group_id.into(),
                consumer_id: consumer_id.into(),
            })),
        }
    }

    pub fn joined(group_id: impl Into<String>, consumer_id: impl Into<String>) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            body: Some(envelope::Body::Joined(Joined {
                group_id: group_id.into(),
                consumer_id: consumer_id.into(),
            })),
        }
    }

    pub fn record_batch(batch: RecordBatch) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            body: Some(envelope::Body::RecordBatch(batch)),
        }
    }

    pub fn ack(batch_id: u64) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            body: Some(envelope::Body::Ack(Ack { batch_id })),
        }
    }

    pub fn flow_control(max_in_flight: u32) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            body: Some(envelope::Body::FlowControl(FlowControl { max_in_flight })),
        }
    }

    pub fn heartbeat() -> Self {
        Self {
            version: PROTOCOL_VERSION,
            body: Some(envelope::Body::Heartbeat(Heartbeat {})),
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            body: Some(envelope::Body::Error(ErrorMessage {
                message: message.into(),
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn round_trip_join() {
        let env = Envelope::join_group("g1", "c1");
        let bytes = env.encode_to_vec();
        let decoded = Envelope::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded.version, PROTOCOL_VERSION);
        match decoded.body {
            Some(envelope::Body::JoinGroup(j)) => {
                assert_eq!(j.group_id, "g1");
                assert_eq!(j.consumer_id, "c1");
            }
            other => panic!("unexpected body: {other:?}"),
        }
    }

    #[test]
    fn record_batch_round_trip() {
        let env = Envelope::record_batch(RecordBatch {
            batch_id: 1,
            records: vec![
                Record {
                    record_id: 1,
                    payload: b"aaa".to_vec(),
                },
                Record {
                    record_id: 2,
                    payload: vec![],
                },
            ],
            sent_at_unix_ns: 123,
        });
        let decoded = Envelope::decode(env.encode_to_vec().as_slice()).unwrap();
        match decoded.body {
            Some(envelope::Body::RecordBatch(b)) => {
                assert_eq!(b.batch_id, 1);
                assert_eq!(b.records.len(), 2);
                assert_eq!(b.records[0].payload, b"aaa");
                assert_eq!(b.records[1].record_id, 2);
                assert_eq!(b.sent_at_unix_ns, 123);
            }
            other => panic!("{other:?}"),
        }
    }
}
