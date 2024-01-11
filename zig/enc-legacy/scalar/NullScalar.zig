const std = @import("std");
const enc = @import("../enc.zig");

const Self = @This();

pub fn format(self: Self, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
    _ = self;
    _ = options;
    _ = fmt;
    try writer.writeAll("null");
}

pub fn cast(self: Self, allocator: std.mem.Allocator, dtype: enc.DType) !enc.Scalar {
    return switch (dtype) {
        .null => .{ .null = self },
        .nullable => enc.NullableScalar.initAbsentOwned(try dtype.clone(allocator)),
        else => return enc.Error.InvalidCast,
    };
}
