const std = @import("std");
const enc = @import("../enc.zig");
const pretty = @import("pretty");
const serde = @import("../serde.zig");

const Self = @This();

names: []const []const u8,
values: []const enc.Scalar,
allocator: ?std.mem.Allocator = null,

pub fn clone(self: Self, allocator: std.mem.Allocator) !Self {
    const names = try allocator.alloc([]const u8, self.names.len);
    const values = try allocator.alloc(enc.Scalar, self.names.len);
    for (names, values, self.names, self.values) |*new_name, *new_value, name, value| {
        new_name.* = try allocator.dupe(u8, name);
        new_value.* = try value.clone(allocator);
    }
    return .{
        .names = names,
        .values = values,
        .allocator = allocator,
    };
}

pub fn deinit(self: Self) void {
    for (self.values) |value| {
        value.deinit();
    }
    if (self.allocator) |ally| {
        for (self.names) |name| {
            ally.free(name);
        }
        ally.free(self.names);
        ally.free(self.values);
    }
}

pub fn cast(self: Self, allocator: std.mem.Allocator, into_dtype: enc.DType) !enc.Scalar {
    _ = into_dtype;
    _ = self;
    _ = allocator;
    return enc.Error.InvalidCast;
}

pub fn format(self: Self, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
    _ = options;
    _ = fmt;
    try writer.print("{}", .{pretty.KeyValue([]const u8, enc.Scalar){ .keys = self.names, .values = self.values }});
}

pub fn getDType(self: Self, allocator: std.mem.Allocator) !enc.DType {
    const names = try allocator.alloc([]const u8, self.names.len);
    const fields = try allocator.alloc(enc.DType, self.values.len);
    for (names, fields, self.names, self.values) |*name, *field, field_name, value| {
        name.* = try allocator.dupe(u8, field_name);
        field.* = try value.getDType(allocator);
    }
    return enc.dtypes.struct_(names, fields, allocator);
}

pub fn toBytes(self: Self, writer: anytype) !void {
    try std.leb.writeULEB128(writer, self.names.len);
    for (self.names) |name| {
        try serde.writeByteSlice(name, writer);
    }
    for (self.values) |value| {
        try value.toBytes(writer);
    }
}

pub fn fromBytes(reader: anytype, allocator: std.mem.Allocator) !Self {
    const fieldCount = try std.leb.readULEB128(u64, reader);
    const names = try allocator.alloc([]u8, fieldCount);
    for (names) |*name| {
        name.* = try serde.readByteSlice(reader, allocator);
    }
    const values = try allocator.alloc(enc.Scalar, fieldCount);
    for (values) |*value| {
        value.* = try enc.Scalar.fromBytes(reader, allocator);
    }
    return .{ .names = names, .values = values, .allocator = allocator };
}
