const std = @import("std");
const enc = @import("../enc.zig");

pub const IntScalar = IntegerScalar(.signed);
pub const UIntScalar = IntegerScalar(.unsigned);

fn IntegerScalar(comptime signedness: std.builtin.Signedness) type {
    return union(enc.DType.IntWidth) {
        const Self = @This();

        Unknown: std.meta.Int(signedness, 64),
        _8: std.meta.Int(signedness, 8),
        _16: std.meta.Int(signedness, 16),
        _32: std.meta.Int(signedness, 32),
        _64: std.meta.Int(signedness, 64),

        pub fn init(value: anytype) enc.Scalar {
            const info = @typeInfo(@TypeOf(value)).Int;
            if (info.signedness != signedness) {
                @compileError("Incorrect signedness");
            }
            const scalar: Self = switch (info.bits) {
                1...8 => .{ ._8 = @intCast(value) },
                9...16 => .{ ._16 = @intCast(value) },
                17...32 => .{ ._32 = @intCast(value) },
                33...64 => .{ ._64 = @intCast(value) },
                else => @compileError("Unsupported integer width"),
            };
            return switch (signedness) {
                .signed => .{ .int = scalar },
                .unsigned => .{ .uint = scalar },
            };
        }

        pub fn cast(self: Self, allocator: std.mem.Allocator, dtype: enc.DType) !enc.Scalar {
            _ = allocator;
            const ptype = dtype.toPType() orelse return enc.Error.InvalidCast;
            return switch (ptype) {
                inline else => |p| enc.Scalar.init(try self.as(p.astype())),
            };
        }

        pub fn as(self: Self, comptime T: type) !T {
            return switch (self) {
                inline else => |i| switch (@typeInfo(T)) {
                    .Int => @intCast(i),
                    .Float => @floatFromInt(i),
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
            const encodeFn = switch (signedness) {
                .signed => std.leb.writeILEB128,
                .unsigned => std.leb.writeULEB128,
            };
            switch (self) {
                inline else => |i, t| {
                    try writer.writeByte(@intFromEnum(t));
                    try encodeFn(writer, i);
                },
            }
        }

        pub fn fromBytes(reader: anytype, allocator: std.mem.Allocator) !Self {
            _ = allocator;
            const decodeFn = switch (signedness) {
                .signed => std.leb.readILEB128,
                .unsigned => std.leb.readULEB128,
            };

            const intKind: enc.DType.IntWidth = @enumFromInt(try reader.readByte());
            return switch (intKind) {
                inline else => |i| @unionInit(Self, @tagName(i), try decodeFn(i.asType(signedness), reader)),
            };
        }
    };
}
