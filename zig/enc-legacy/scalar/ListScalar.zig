const std = @import("std");
const enc = @import("../enc.zig");
const pretty = @import("pretty");

const Self = @This();

dtype: enc.DType,
values: []const enc.Scalar,
allocator: ?std.mem.Allocator = null,

pub fn clone(self: Self, allocator: std.mem.Allocator) !Self {
    return .{
        .dtype = try self.dtype.clone(allocator),
        .values = try allocator.dupe(enc.Scalar, self.values),
        .allocator = allocator,
    };
}

pub fn deinit(self: Self) void {
    for (self.values) |v| v.deinit();
    if (self.allocator) |ally| ally.free(self.values);
}

pub fn format(self: Self, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
    _ = options;
    _ = fmt;
    try writer.print("{}", .{pretty.List(enc.Scalar, "[]"){ .items = self.values }});
}

pub fn cast(self: Self, allocator: std.mem.Allocator, into_dtype: enc.DType) !enc.Scalar {
    _ = into_dtype;
    _ = self;
    _ = allocator;
    return enc.Error.InvalidCast;
}

pub fn getDType(self: Self, allocator: std.mem.Allocator) !enc.DType {
    const child_dtype = try allocator.create(enc.DType);
    child_dtype.* = try self.dtype.clone(allocator);
    return enc.dtypes.list(child_dtype, allocator);
}

pub fn toBytes(self: Self, writer: anytype) !void {
    try std.leb.writeULEB128(writer, self.values.len);
    for (self.values) |value| {
        try value.toBytes(writer);
    }
    try self.dtype.toBytes(writer);
}

pub fn fromBytes(reader: anytype, allocator: std.mem.Allocator) !Self {
    const nvalues = try std.leb.readULEB128(u64, reader);
    const values = try allocator.alloc(enc.Scalar, nvalues);
    for (values) |*value| {
        value.* = try enc.Scalar.fromBytes(reader, allocator);
    }
    const value_dtype = try enc.DType.fromBytes(reader, allocator);
    return .{ .dtype = value_dtype, .values = values, .allocator = allocator };
}
