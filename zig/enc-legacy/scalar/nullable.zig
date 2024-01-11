const std = @import("std");
const enc = @import("../enc.zig");

pub const NullableScalar = union(enum) {
    const Self = @This();

    present: struct {
        scalar: *const enc.Scalar,
        allocator: ?std.mem.Allocator,
    },
    absent: enc.DType,

    pub fn initAbsentOwned(dtype: enc.DType) enc.Scalar {
        return .{ .nullable = .{ .absent = dtype } };
    }

    pub fn clone(self: Self, allocator: std.mem.Allocator) !Self {
        switch (self) {
            .present => |p| {
                const new_scalar = try allocator.create(enc.Scalar);
                new_scalar.* = try p.scalar.clone(allocator);
                return .{ .present = .{ .scalar = new_scalar, .allocator = allocator } };
            },
            .absent => |d| return .{ .absent = try d.clone(allocator) },
        }
    }

    pub fn deinit(self: Self) void {
        switch (self) {
            .present => |p| {
                p.scalar.deinit();
                if (p.allocator) |ally| ally.destroy(p.scalar);
            },
            .absent => |dtype| dtype.deinit(),
        }
    }

    pub fn format(self: Self, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = options;
        _ = fmt;
        switch (self) {
            .present => |p| try writer.print("{}", .{p.scalar}),
            .absent => try writer.writeAll("null"),
        }
    }

    pub fn cast(self: Self, allocator: std.mem.Allocator, dtype: enc.DType) !enc.Scalar {
        return switch (self) {
            .present => |p| blk: {
                defer if (p.allocator) |ally| ally.destroy(p.scalar);
                break :blk p.scalar.cast(allocator, dtype);
            },
            .absent => .{ .nullable = .{ .absent = try dtype.clone(allocator) } },
        };
    }

    pub fn getDType(self: Self, allocator: std.mem.Allocator) !enc.DType {
        return switch (self) {
            .present => |s| (try s.scalar.getDType(allocator)).toNullable(allocator),
            .absent => |d| d.toNullable(allocator),
        };
    }

    pub fn toBytes(self: Self, writer: anytype) !void {
        try writer.writeByte(if (self == .present) 0 else 1);
        switch (self) {
            .present => |p| try p.scalar.toBytes(writer),
            .absent => |d| try d.toBytes(writer),
        }
    }

    pub fn fromBytes(reader: anytype, allocator: std.mem.Allocator) !Self {
        switch (@as(u1, @intCast(try reader.readByte()))) {
            0 => {
                const newScalar = try allocator.create(enc.Scalar);
                newScalar.* = try enc.Scalar.fromBytes(reader, allocator);
                return .{ .present = .{ .scalar = newScalar, .allocator = allocator } };
            },
            1 => return .{ .absent = try enc.DType.fromBytes(reader, allocator) },
        }
    }
};
