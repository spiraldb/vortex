const std = @import("std");
const enc = @import("../enc.zig");

const Self = @This();

value: bool,

pub fn init(value: bool) enc.Scalar {
    return .{ .bool = .{ .value = value } };
}

pub fn format(self: Self, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
    _ = options;
    _ = fmt;
    try writer.print("{}", .{self.value});
}

pub fn cast(self: Self, allocator: std.mem.Allocator, dtype: enc.DType) !enc.Scalar {
    _ = allocator;
    return switch (dtype) {
        .bool => .{ .bool = self },
        .int => enc.Scalar.init(@as(u1, if (self.value) 1 else 0)),
        else => return enc.Error.InvalidCast,
    };
}

pub fn as(self: Self, comptime T: type) !T {
    return switch (T) {
        bool => self.value,
        u1 => @intFromBool(self.value),
        else => enc.Error.InvalidCast,
    };
}

pub fn toBytes(self: Self, writer: anytype) !void {
    try writer.writeByte(@intFromBool(self.value));
}

pub fn fromBytes(reader: anytype, allocator: std.mem.Allocator) !Self {
    _ = allocator;
    return .{ .value = try reader.readByte() != 0 };
}
