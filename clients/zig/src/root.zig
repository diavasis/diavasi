//! Zig client for `diavasi.data.v1`.
//!
//! `consume` opens the TLS stream, sends Hello version 1, joins the group, and
//! acks each batch. The server owns the cursor. `record_id` can be 0, so this
//! client does not dedupe on it. The transport is the C library in `clients/c`.

const std = @import("std");

const c = @cImport({
    @cInclude("diavasi.h");
});

/// Called for each record before the batch is acked.
/// `payload` is valid only for the duration of the call.
pub const RecordFn = *const fn (batch_id: u64, record_id: u64, payload: []const u8, ctx: ?*anyopaque) void;

/// Where to connect and how far to read.
pub const Options = struct {
    /// Data-plane address, `host:port`, without a scheme.
    addr: [:0]const u8,
    /// PEM file for the data-plane CA. The TLS name is `localhost`.
    ca_path: [:0]const u8,
    /// Bearer token. A bad token is a failure with code -1.
    token: [:0]const u8,
    /// Consumer group to join.
    group_id: [:0]const u8,
    /// Consumer id. Reconnect with the same id to replay unacked batches.
    consumer_id: [:0]const u8,
    /// Batches the server may have in flight. 0 is treated as 1.
    max_in_flight: u32 = 1,
    /// Stop after this many acks without Leave. 0 disables the limit.
    halt_after_acks: u32 = 0,
    /// Send Leave after this many records. 0 reads until the stream ends.
    expect_records: u64 = 0,
    /// Optional per-record hook. Null skips it.
    on_record: ?RecordFn = null,
    /// Passed through to `on_record`.
    ctx: ?*anyopaque = null,
};

/// Record ids and batch ids acked by one `consume` call.
pub const Report = struct {
    record_ids: []u64,
    batch_ids: []u64,

    pub fn deinit(self: Report, allocator: std.mem.Allocator) void {
        allocator.free(self.record_ids);
        allocator.free(self.batch_ids);
    }
};

/// A failed consume. `code` 1 through 8 is a protocol frame. `-1` is transport or auth.
pub const Failure = struct {
    code: i32,
    message: []u8,

    pub fn deinit(self: Failure, allocator: std.mem.Allocator) void {
        allocator.free(self.message);
    }
};

/// Either the acked ids or a protocol, transport, or auth failure.
pub const Outcome = union(enum) {
    report: Report,
    failure: Failure,
};

const Hook = struct {
    func: RecordFn,
    ctx: ?*anyopaque,
};

fn trampoline(
    batch_id: u64,
    record_id: u64,
    payload: [*c]const u8,
    payload_len: usize,
    user: ?*anyopaque,
) callconv(.c) void {
    const hook: *Hook = @ptrCast(@alignCast(user.?));
    const bytes = if (payload_len == 0) &[_]u8{} else payload[0..payload_len];
    hook.func(batch_id, record_id, bytes, hook.ctx);
}

/// Joins `options.group_id`, acks each batch, and copies the ids into `allocator`.
///
/// `on_record` runs before each ack. The caller frees a report or a failure
/// with `deinit`.
pub fn consume(allocator: std.mem.Allocator, options: Options) std.mem.Allocator.Error!Outcome {
    var hook: Hook = undefined;
    var c_opts = std.mem.zeroes(c.diavasi_options);
    c_opts.addr = options.addr.ptr;
    c_opts.ca_path = options.ca_path.ptr;
    c_opts.token = options.token.ptr;
    c_opts.group_id = options.group_id.ptr;
    c_opts.consumer_id = options.consumer_id.ptr;
    c_opts.max_in_flight = options.max_in_flight;
    c_opts.halt_after_acks = options.halt_after_acks;
    c_opts.expect_records = options.expect_records;
    if (options.on_record) |func| {
        hook = .{ .func = func, .ctx = options.ctx };
        c_opts.on_record = trampoline;
        c_opts.user = &hook;
    }

    var raw = std.mem.zeroes(c.diavasi_report);
    var error_buf = [_]u8{0} ** 512;
    const rc = c.diavasi_consume(&c_opts, &raw, &error_buf, error_buf.len);
    defer c.diavasi_report_free(&raw);

    if (rc != 0) {
        return .{ .failure = .{
            .code = rc,
            .message = try allocator.dupe(u8, std.mem.sliceTo(&error_buf, 0)),
        } };
    }

    const record_ids = try allocator.alloc(u64, raw.record_count);
    errdefer allocator.free(record_ids);
    if (raw.record_count > 0) {
        for (0..raw.record_count) |i| record_ids[i] = raw.record_ids[i];
    }

    const batch_ids = try allocator.alloc(u64, raw.batch_count);
    errdefer allocator.free(batch_ids);
    if (raw.batch_count > 0) {
        for (0..raw.batch_count) |i| batch_ids[i] = raw.batch_ids[i];
    }

    return .{ .report = .{ .record_ids = record_ids, .batch_ids = batch_ids } };
}
