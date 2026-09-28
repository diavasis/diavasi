//! The TLS gRPC data plane, protocol `diavasi.data.v1`.
//!
//! [`serve_dataplane`] runs `DataPlane.Consume`: one bidirectional stream per
//! consumer. [`Session`] is the protocol state machine without I/O; the
//! frame constructors below build each [`Envelope`]. [`ConsumerClient`] is
//! the in-repo consumer that `diavasi test` and the tests use. ADR 0007
//! describes the session and the error codes in [`BAD_VERSION`] and the
//! constants after it.
//!
//! A session from the client's side:
//!
//! ```text
//! client -> hello {protocol_version: 1}
//! server -> hello_ack {protocol_version: 1}
//! client -> join_group {group_id: "orders", consumer_id: "worker-1"}
//! server -> joined {group_id: "orders", consumer_id: "worker-1"}
//! client -> flow_control {max_in_flight: 4}
//! server -> record_batch {batch_id: 17, records: [{record_id: 9002, payload: b"{...}"}]}
//! client -> ack {batch_id: 17}
//! client -> leave {}
//! ```

mod client;
mod error_codes;
mod server;
mod session;
mod tls;

#[cfg(test)]
mod tests;

/// Types generated from `proto/data.proto`.
pub mod pb {
    // tonic::Status is large; generated gRPC stubs trip clippy::result_large_err.
    #![allow(clippy::result_large_err)]
    tonic::include_proto!("diavasi.data.v1");
}

pub use client::{ConsumeReport, ConsumerClient, ConsumerOptions, SharedProgress};
pub use error_codes::*;
pub use server::{DataPlaneConfig, DataPlaneError, serve_dataplane, serve_dataplane_on};
pub use session::{DEFAULT_MAX_IN_FLIGHT, Effect, Phase, Session, Step};
pub use tls::{generate_self_signed, load_or_generate_pem, load_pem};

use pb::envelope::Body;

/// The protocol version this server speaks.
pub const PROTOCOL_VERSION: u32 = 1;

pub use pb::Envelope;

/// The client's first frame.
///
/// ```
/// use diavasi::dataplane::{PROTOCOL_VERSION, Session, hello, join_group};
/// let mut session = Session::new();
/// assert_eq!(session.on_frame(&hello(PROTOCOL_VERSION)).frames.len(), 1); // HelloAck
/// let step = session.on_frame(&join_group("orders", "worker-1"));
/// assert!(!step.close);
/// ```
pub fn hello(protocol_version: u32) -> Envelope {
    Envelope {
        version: PROTOCOL_VERSION,
        body: Some(Body::Hello(pb::Hello { protocol_version })),
    }
}

/// The server's answer to `hello`.
pub fn hello_ack(protocol_version: u32) -> Envelope {
    Envelope {
        version: PROTOCOL_VERSION,
        body: Some(Body::HelloAck(pb::HelloAck { protocol_version })),
    }
}

/// Join `group_id` as `consumer_id`.
pub fn join_group(group_id: impl Into<String>, consumer_id: impl Into<String>) -> Envelope {
    Envelope {
        version: PROTOCOL_VERSION,
        body: Some(Body::JoinGroup(pb::JoinGroup {
            group_id: group_id.into(),
            consumer_id: consumer_id.into(),
        })),
    }
}

/// The server's answer to a successful join.
pub fn joined(group_id: impl Into<String>, consumer_id: impl Into<String>) -> Envelope {
    Envelope {
        version: PROTOCOL_VERSION,
        body: Some(Body::Joined(pb::Joined {
            group_id: group_id.into(),
            consumer_id: consumer_id.into(),
        })),
    }
}

/// Wrap a batch for sending.
pub fn record_batch(batch: pb::RecordBatch) -> Envelope {
    Envelope {
        version: PROTOCOL_VERSION,
        body: Some(Body::RecordBatch(batch)),
    }
}

/// Ack `batch_id`.
pub fn ack(batch_id: u64) -> Envelope {
    Envelope {
        version: PROTOCOL_VERSION,
        body: Some(Body::Ack(pb::Ack { batch_id })),
    }
}

/// A reserved frame the server rejects with error 6.
pub fn nack(batch_id: u64) -> Envelope {
    Envelope {
        version: PROTOCOL_VERSION,
        body: Some(Body::Nack(pb::Nack { batch_id })),
    }
}

/// A keep-alive frame.
pub fn heartbeat() -> Envelope {
    Envelope {
        version: PROTOCOL_VERSION,
        body: Some(Body::Heartbeat(pb::Heartbeat {})),
    }
}

/// Allow `max_in_flight` unacked batches on this stream (1 to 1024).
pub fn flow_control(max_in_flight: u32) -> Envelope {
    Envelope {
        version: PROTOCOL_VERSION,
        body: Some(Body::FlowControl(pb::FlowControl { max_in_flight })),
    }
}

/// A protocol error; see the codes starting at [`BAD_VERSION`].
pub fn error_envelope(code: u32, message: impl Into<String>) -> Envelope {
    Envelope {
        version: PROTOCOL_VERSION,
        body: Some(Body::Error(pb::ErrorMessage {
            code,
            message: message.into(),
        })),
    }
}

/// Leave the group. Unacked batches return for redelivery.
pub fn leave() -> Envelope {
    Envelope {
        version: PROTOCOL_VERSION,
        body: Some(Body::Leave(pb::Leave {})),
    }
}
