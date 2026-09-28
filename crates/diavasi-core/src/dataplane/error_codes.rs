/// The envelope or protocol version is not 1.
pub const BAD_VERSION: u32 = 1;
/// The frame does not fit the session state, an id is invalid, or a newer stream took the consumer id over.
pub const BAD_STATE: u32 = 2;
/// An ack for a batch this stream was not sent.
pub const UNKNOWN_ACK: u32 = 3;
/// A second ack for the same batch.
pub const DUPLICATE_ACK: u32 = 4;
/// The group is not running.
pub const NOT_RUNNING: u32 = 5;
/// A reserved frame, such as `Nack`.
pub const UNSUPPORTED: u32 = 6;
/// A server error, such as a failed checkpoint write.
pub const INTERNAL: u32 = 7;
/// The client sent nothing for the heartbeat timeout (30 s).
pub const HEARTBEAT_TIMEOUT: u32 = 8;
