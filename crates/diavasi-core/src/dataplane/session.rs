use std::collections::HashSet;

use super::error_codes::{BAD_STATE, BAD_VERSION, DUPLICATE_ACK, UNKNOWN_ACK, UNSUPPORTED};
use super::pb::envelope::Body;
use super::{Envelope, PROTOCOL_VERSION, error_envelope, hello_ack};

/// Unacked batches allowed on a stream before the client sends `FlowControl`.
pub const DEFAULT_MAX_IN_FLIGHT: u32 = 1;
const MAX_IN_FLIGHT_CAP: u32 = 1024;

/// Where a session is in the protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Waiting for `Hello`.
    ExpectHello,
    /// Waiting for `JoinGroup`.
    ExpectJoin,
    /// Joined: batches flow and acks are accepted.
    Active,
    /// Closed after an error or `Leave`.
    Closed,
}

/// Work a frame asks the server to do outside the session.
#[derive(Debug)]
pub enum Effect {
    /// Join the group.
    Join {
        /// The group to join.
        group_id: String,
        /// The consumer id to join as.
        consumer_id: String,
    },
    /// Ack a batch.
    Ack {
        /// The batch to ack.
        batch_id: u64,
    },
    /// Leave the group.
    Leave,
}

/// What the server does after one client frame.
#[derive(Debug)]
pub struct Step {
    /// Frames to send back, in order.
    pub frames: Vec<Envelope>,
    /// Work to do, in order.
    pub effects: Vec<Effect>,
    /// True when the stream ends after the frames are sent.
    pub close: bool,
}

/// The protocol state machine for one stream, without I/O. Feed it client
/// frames with [`Session::on_frame`]; send its frames and carry out its
/// effects.
#[derive(Debug)]
pub struct Session {
    phase: Phase,
    max_in_flight: u32,
    outstanding: HashSet<u64>,
    acked: HashSet<u64>,
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

impl Session {
    /// A session waiting for `Hello`.
    pub fn new() -> Self {
        Self {
            phase: Phase::ExpectHello,
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            outstanding: HashSet::new(),
            acked: HashSet::new(),
        }
    }

    /// Where the session is.
    pub fn phase(&self) -> Phase {
        self.phase
    }

    /// Unacked batches allowed on this stream.
    pub fn max_in_flight(&self) -> u32 {
        self.max_in_flight
    }

    /// Batches sent and not yet acked.
    pub fn outstanding_len(&self) -> usize {
        self.outstanding.len()
    }

    /// True when the session is active and below its in-flight limit.
    pub fn can_assign(&self) -> bool {
        self.phase == Phase::Active && (self.outstanding.len() as u32) < self.max_in_flight
    }

    /// Record a batch sent to the client.
    pub fn note_assigned(&mut self, batch_id: u64) {
        self.outstanding.insert(batch_id);
    }

    /// Close the session.
    pub fn close(&mut self) {
        self.phase = Phase::Closed;
    }

    /// Handle one client frame. A frame that breaks the protocol closes the session with an error frame.
    pub fn on_frame(&mut self, env: &Envelope) -> Step {
        if self.phase == Phase::Closed {
            return close_step(BAD_STATE, "session closed");
        }
        if env.version != PROTOCOL_VERSION {
            self.phase = Phase::Closed;
            return close_step(BAD_VERSION, "unsupported envelope version");
        }
        let Some(body) = &env.body else {
            self.phase = Phase::Closed;
            return close_step(BAD_STATE, "missing envelope body");
        };
        match (self.phase, body) {
            (Phase::ExpectHello, Body::Hello(hello)) => {
                if hello.protocol_version != PROTOCOL_VERSION {
                    self.phase = Phase::Closed;
                    return close_step(BAD_VERSION, "unsupported protocol version");
                }
                self.phase = Phase::ExpectJoin;
                Step {
                    frames: vec![hello_ack(PROTOCOL_VERSION)],
                    effects: Vec::new(),
                    close: false,
                }
            }
            (Phase::ExpectJoin, Body::JoinGroup(join)) => {
                if join.group_id.is_empty() || join.consumer_id.is_empty() {
                    self.phase = Phase::Closed;
                    return close_step(BAD_STATE, "group_id and consumer_id are required");
                }
                self.phase = Phase::Active;
                Step {
                    frames: Vec::new(),
                    effects: vec![Effect::Join {
                        group_id: join.group_id.clone(),
                        consumer_id: join.consumer_id.clone(),
                    }],
                    close: false,
                }
            }
            (Phase::Active, Body::Ack(ack)) => self.on_ack(ack.batch_id),
            (Phase::Active, Body::FlowControl(flow)) => {
                if flow.max_in_flight == 0 || flow.max_in_flight > MAX_IN_FLIGHT_CAP {
                    self.phase = Phase::Closed;
                    return close_step(BAD_STATE, "max_in_flight out of range");
                }
                self.max_in_flight = flow.max_in_flight;
                Step {
                    frames: Vec::new(),
                    effects: Vec::new(),
                    close: false,
                }
            }
            (Phase::Active, Body::Heartbeat(_)) => Step {
                frames: Vec::new(),
                effects: Vec::new(),
                close: false,
            },
            (Phase::Active, Body::Leave(_)) => {
                self.phase = Phase::Closed;
                Step {
                    frames: Vec::new(),
                    effects: vec![Effect::Leave],
                    close: true,
                }
            }
            (_, Body::Nack(_)) => {
                self.phase = Phase::Closed;
                close_step(UNSUPPORTED, "nack is reserved and unused")
            }
            _ => {
                self.phase = Phase::Closed;
                close_step(BAD_STATE, "unexpected frame for session state")
            }
        }
    }

    fn on_ack(&mut self, batch_id: u64) -> Step {
        if self.acked.contains(&batch_id) {
            self.phase = Phase::Closed;
            return close_step(DUPLICATE_ACK, "duplicate ack");
        }
        if !self.outstanding.remove(&batch_id) {
            self.phase = Phase::Closed;
            return close_step(UNKNOWN_ACK, "unknown ack");
        }
        self.acked.insert(batch_id);
        Step {
            frames: Vec::new(),
            effects: vec![Effect::Ack { batch_id }],
            close: false,
        }
    }
}

fn close_step(code: u32, message: &str) -> Step {
    Step {
        frames: vec![error_envelope(code, message)],
        effects: Vec::new(),
        close: true,
    }
}
