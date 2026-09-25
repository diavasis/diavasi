const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const diavasi_mod = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    diavasi_mod.addIncludePath(b.path("../c/include"));
    diavasi_mod.linkSystemLibrary("grpc", .{});
    diavasi_mod.addCSourceFiles(.{
        .root = b.path("../c"),
        .files = &.{ "src/consume.c", "src/proto.c" },
        .flags = &.{ "-std=c11", "-Wall" },
    });

    const exe = b.addExecutable(.{
        .name = "diavasi-zig",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "diavasi", .module = diavasi_mod },
            },
        }),
    });

    b.installArtifact(exe);
    const run = b.addRunArtifact(exe);
    if (b.args) |args| run.addArgs(args);
    b.step("run", "Join the demo group").dependOn(&run.step);
}
