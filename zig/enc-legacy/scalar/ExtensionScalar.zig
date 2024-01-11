const std = @import("std");
const enc = @import("../enc.zig");

const Self = @This();

id: []const u8,
ptr: *anyopaque,
vtable: *const VTable,

pub const VTable = struct {
    clone: *const fn (*anyopaque, std.mem.Allocator) anyerror!Self,
    deinit: *const fn (*anyopaque) void,
    equal: *const fn (*anyopaque, other: *anyopaque) bool,
    getDType: *const fn (*anyopaque, std.mem.Allocator) anyerror!enc.DType,
};

pub fn clone(self: Self, allocator: std.mem.Allocator) !Self {
    return try self.vtable.clone(self.ptr, allocator);
}

pub fn deinit(self: Self) void {
    self.vtable.deinit(self.ptr);
}

pub fn format(self: Self, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
    _ = options;
    _ = fmt;
    try writer.print("ext:{s}", .{self.id});
}

pub fn cast(self: Self, allocator: std.mem.Allocator, into_dtype: enc.DType) !enc.Scalar {
    _ = into_dtype;
    _ = self;
    _ = allocator;
    return enc.Error.InvalidCast;
}

pub fn getDType(self: Self, allocator: std.mem.Allocator) !enc.DType {
    return try self.vtable.getDType(self.ptr, allocator);
}

pub fn toBytes(self: Self, writer: anytype) !void {
    _ = writer;
    _ = self;
}

pub fn fromBytes(reader: anytype, allocator: std.mem.Allocator) !Self {
    _ = allocator;
    _ = reader;
}
