const std = @import("std");
const enc = @import("../enc.zig");

pub const FloatScalar = union(enc.DType.FloatWidth) {
    const Self = @This();

    Unknown: f64,
    _16: f16,
    _32: f32,
    _64: f64,

    pub fn cast(self: Self, allocator: std.mem.Allocator, dtype: enc.DType) !enc.Scalar {
        _ = allocator;
        const ptype = dtype.toPType() orelse return enc.Error.InvalidCast;
        return switch (ptype) {
            inline else => |p| enc.Scalar.init(try self.as(p.astype())),
        };
    }

    pub fn as(self: Self, comptime T: type) !T {
        return switch (self) {
            inline else => |f| switch (@typeInfo(T)) {
                .Float => @floatCast(f),
                .Int => @intFromFloat(f),
                else => enc.Error.InvalidCast,
            },
        };
    }

    pub fn format(self: Self, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = options;
        _ = fmt;
        switch (self) {
            inline else => |v| try writer.print("{}", .{v}),
        }
    }

    pub fn toBytes(self: Self, writer: anytype) !void {
        switch (self) {
            inline else => |f, t| {
                try writer.writeByte(@intFromEnum(t));
                const serdeIntType = @Type(.{ .Int = .{ .signedness = .signed, .bits = @typeInfo(@TypeOf(f)).Float.bits } });
                try std.leb.writeILEB128(writer, @as(serdeIntType, @bitCast(f)));
            },
        }
    }

    pub fn fromBytes(reader: anytype, allocator: std.mem.Allocator) !Self {
        _ = allocator;
        const floatKind: enc.DType.FloatWidth = @enumFromInt(try reader.readByte());
        return switch (floatKind) {
            inline else => |f| blk: {
                const floatType = f.asType();
                const floatIntType = @Type(.{ .Int = .{ .signedness = .signed, .bits = @typeInfo(floatType).Float.bits } });
                break :blk @unionInit(
                    Self,
                    @tagName(f),
                    @as(floatType, @bitCast(try std.leb.readILEB128(floatIntType, reader))),
                );
            },
        };
    }
};
