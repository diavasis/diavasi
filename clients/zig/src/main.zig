//! Example consumer. Prints each record, then the acked record ids.
const std = @import("std");
const diavasi = @import("diavasi");

fn onRecord(batch_id: u64, record_id: u64, payload: []const u8, ctx: ?*anyopaque) void {
    _ = ctx;
    std.debug.print("batch {d} record {d} ({d} bytes)\n", .{ batch_id, record_id, payload.len });
}

fn flag(args: []const [:0]u8, name: []const u8, fallback: [:0]const u8) [:0]const u8 {
    var i: usize = 1;
    while (i + 1 < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], name)) return args[i + 1];
    }
    return fallback;
}

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const args = try std.process.argsAlloc(allocator);

    const outcome = try diavasi.consume(allocator, .{
        .addr = flag(args, "--addr", "127.0.0.1:7710"),
        .ca_path = flag(args, "--ca", "/tmp/diavasi-sdk/dataplane-ca.crt"),
        .token = flag(args, "--token", "sdk-demo"),
        .group_id = flag(args, "--group", "demo"),
        .consumer_id = flag(args, "--consumer", "zig"),
        .expect_records = 8,
        .on_record = onRecord,
    });

    switch (outcome) {
        .report => |report| {
            std.debug.print("record_ids", .{});
            for (report.record_ids) |id| std.debug.print(" {d}", .{id});
            std.debug.print("\n", .{});
        },
        .failure => |failure| {
            if (failure.code >= 1 and failure.code <= 8) {
                std.debug.print("protocol {d}: {s}\n", .{ failure.code, failure.message });
            } else {
                std.debug.print("{s}\n", .{failure.message});
            }
            std.process.exit(if (failure.code > 0) @intCast(failure.code) else 1);
        },
    }
}
