const std = @import("std");
const enc = @import("../enc.zig");
const UIntScalar = @import("./int.zig").UIntScalar;

pub const LocalDateScalar = struct {
    const Self = @This();

    days: UIntScalar,

    pub fn cast(self: Self, allocator: std.mem.Allocator, into_dtype: enc.DType) !enc.Scalar {
        _ = into_dtype;
        _ = self;
        _ = allocator;
        return enc.Error.InvalidCast;
    }

    pub fn format(self: Self, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = options;
        _ = fmt;
        try writer.print("localdate[{}]", .{self.days});
    }

    pub fn toBytes(self: Self, writer: anytype) !void {
        try self.days.toBytes(writer);
    }

    pub fn fromBytes(reader: anytype, allocator: std.mem.Allocator) !Self {
        return .{ .days = try UIntScalar.fromBytes(reader, allocator) };
    }
};

/// Represents the number of unit values since midnight.
pub const LocalTimeScalar = TimeWithUnit("localtime");

/// Represents the number of unit values since Unix epoch.
pub const InstantScalar = TimeWithUnit("instant");

fn TimeWithUnit(comptime name: []const u8) type {
    return union(enc.DType.TimeUnit) {
        const Self = @This();

        ns: UIntScalar,
        us: UIntScalar,
        ms: UIntScalar,
        s: UIntScalar,

        pub fn cast(self: Self, allocator: std.mem.Allocator, into_dtype: enc.DType) !enc.Scalar {
            _ = into_dtype;
            _ = self;
            _ = allocator;
            return enc.Error.InvalidCast;
        }

        pub fn format(self: Self, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
            _ = options;
            _ = fmt;
            switch (self) {
                inline else => |value, tag| try writer.print("{s}[{}, unit={s}]", .{ name, value, @tagName(tag) }),
            }
        }

        pub fn toBytes(self: Self, writer: anytype) !void {
            switch (self) {
                inline else => |value, tag| {
                    try writer.writeByte(@intFromEnum(tag));
                    try value.toBytes(writer);
                },
            }
        }

        pub fn fromBytes(reader: anytype, allocator: std.mem.Allocator) !Self {
            const unit = @as(enc.DType.TimeUnit, @enumFromInt(try reader.readByte()));
            const integer = try UIntScalar.fromBytes(reader, allocator);
            return switch (unit) {
                inline else => |u| @unionInit(Self, @tagName(u), integer),
            };
        }
    };
}
