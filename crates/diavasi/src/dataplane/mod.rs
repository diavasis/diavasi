//! Production data-plane protocol (`diavasi.data.v1`).

mod client;
mod error_codes;
mod server;
mod session;
mod tls;

#[cfg(test)]
mod tests;

pub mod pb {
    // tonic::Status is large; generated gRPC stubs trip clippy::result_large_err.
    #![allow(clippy::result_large_err)]
    tonic::include_proto!("diavasi.data.v1");
}

pub use client::{ConsumeReport, ConsumerClient, ConsumerOptions, SharedProgress};
pub use error_codes::*;
pub use server::{DataPlaneConfig, serve_dataplane};
pub use session::{DEFAULT_MAX_IN_FLIGHT, Effect, Phase, Session, Step};
pub use tls::{generate_self_signed, load_or_generate_pem};

use pb::envelope::Body;

pub const PROTOCOL_VERSION: u32 = 1;

pub use pb::Envelope;

pub fn hello(protocol_version: u32) -> Envelope {
    Envelope {
        version: PROTOCOL_VERSION,
        body: Some(Body::Hello(pb::Hello { protocol_version })),
    }
}

pub fn hello_ack(protocol_version: u32) -> Envelope {
    Envelope {
        version: PROTOCOL_VERSION,
        body: Some(Body::HelloAck(pb::HelloAck { protocol_version })),
    }
}

pub fn join_group(group_id: impl Into<String>, consumer_id: impl Into<String>) -> Envelope {
    Envelope {
        version: PROTOCOL_VERSION,
        body: Some(Body::JoinGroup(pb::JoinGroup {
            group_id: group_id.into(),
            consumer_id: consumer_id.into(),
        })),
    }
}

pub fn joined(group_id: impl Into<String>, consumer_id: impl Into<String>) -> Envelope {
    Envelope {
        version: PROTOCOL_VERSION,
        body: Some(Body::Joined(pb::Joined {
            group_id: group_id.into(),
            consumer_id: consumer_id.into(),
        })),
    }
}

pub fn record_batch(batch: pb::RecordBatch) -> Envelope {
    Envelope {
        version: PROTOCOL_VERSION,
        body: Some(Body::RecordBatch(batch)),
    }
}

pub fn ack(batch_id: u64) -> Envelope {
    Envelope {
        version: PROTOCOL_VERSION,
        body: Some(Body::Ack(pb::Ack { batch_id })),
    }
}

pub fn nack(batch_id: u64) -> Envelope {
    Envelope {
        version: PROTOCOL_VERSION,
        body: Some(Body::Nack(pb::Nack { batch_id })),
    }
}

pub fn heartbeat() -> Envelope {
    Envelope {
        version: PROTOCOL_VERSION,
        body: Some(Body::Heartbeat(pb::Heartbeat {})),
    }
}

pub fn flow_control(max_in_flight: u32) -> Envelope {
    Envelope {
        version: PROTOCOL_VERSION,
        body: Some(Body::FlowControl(pb::FlowControl { max_in_flight })),
    }
}

pub fn error_envelope(code: u32, message: impl Into<String>) -> Envelope {
    Envelope {
        version: PROTOCOL_VERSION,
        body: Some(Body::Error(pb::ErrorMessage {
            code,
            message: message.into(),
        })),
    }
}

pub fn leave() -> Envelope {
    Envelope {
        version: PROTOCOL_VERSION,
        body: Some(Body::Leave(pb::Leave {})),
    }
}
