const std = @import("std");
const enc = @import("../enc.zig");
const serde = @import("../serde.zig");

pub const BinaryScalar = BytesScalar(enc.dtypes.binary);
pub const UTF8Scalar = BytesScalar(enc.dtypes.utf8);

fn BytesScalar(comptime dtype: enc.DType) type {
    return struct {
        const Self = @This();

        bytes: []const u8,
        allocator: ?std.mem.Allocator = null,

        pub fn initOwned(bytes: []const u8, allocator: std.mem.Allocator) enc.Scalar {
            return @unionInit(
                enc.Scalar,
                @tagName(dtype),
                .{ .bytes = bytes, .allocator = allocator },
            );
        }

        pub fn clone(self: Self, allocator: std.mem.Allocator) !Self {
            return .{
                .bytes = try allocator.dupe(u8, self.bytes),
                .allocator = allocator,
            };
        }

        pub fn deinit(self: Self) void {
            if (self.allocator) |ally| ally.free(self.bytes);
        }

        pub inline fn getDType(self: Self) !enc.DType {
            _ = self;
            return dtype;
        }

        pub fn cast(self: Self, allocator: std.mem.Allocator, into_dtype: enc.DType) !enc.Scalar {
            _ = allocator;
            return switch (into_dtype) {
                .utf8 => .{ .utf8 = .{ .bytes = self.bytes, .allocator = self.allocator } },
                .binary => .{ .binary = .{ .bytes = self.bytes, .allocator = self.allocator } },
                else => enc.Error.InvalidCast,
            };
        }

        pub fn format(self: Self, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
            _ = options;
            _ = fmt;
            switch (dtype) {
                .binary => try writer.print("bytes[{}]", .{self.bytes.len}),
                .utf8 => try writer.print("{s}", .{self.bytes}),
                else => @compileError("Invalid bytes dtype"),
            }
        }

        pub fn toBytes(self: Self, writer: anytype) !void {
            try serde.writeByteSlice(self.bytes, writer);
        }

        pub fn fromBytes(reader: anytype, allocator: std.mem.Allocator) !Self {
            return .{ .bytes = try serde.readByteSlice(reader, allocator), .allocator = allocator };
        }
    };
}
