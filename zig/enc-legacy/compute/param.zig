const std = @import("std");
const enc = @import("../enc.zig");

pub const ParamKind = enum { array, scalar };

pub const ParamSpec = union(ParamKind) {
    const Self = @This();

    array: ?enc.ArrayKind,
    scalar: void,

    pub fn format(value: Self, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = fmt;
        _ = options;
        switch (value) {
            .array => |maybe| if (maybe) |a| {
                try writer.print("array:{s}", .{@tagName(a)});
            } else {
                try writer.writeAll("array");
            },
            .scalar => try writer.writeAll("scalar"),
        }
    }

    pub fn matchesParam(self: ParamSpec, param: Param) bool {
        switch (self) {
            .array => |maybe_array_kind| {
                if (param != .array) return false;
                if (maybe_array_kind) |kind| {
                    if (param.array.kind != kind) return false;
                }
            },
            .scalar => {
                if (param != .scalar) return false;
            },
        }
        return true;
    }
};

pub const Param = union(ParamKind) {
    const Self = @This();

    array: *const enc.Array,
    scalar: enc.Scalar,

    pub fn format(value: Self, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = fmt;
        _ = options;
        switch (value) {
            .array => |a| try writer.print("array:{s}", .{a.id}),
            .scalar => try writer.writeAll("scalar"),
        }
    }

    pub fn getDType(self: Self, gpa: std.mem.Allocator) !enc.DType {
        return switch (self) {
            .array => |a| a.dtype.clone(gpa),
            .scalar => |s| s.getDType(gpa),
        };
    }
};

pub const Result = union(ParamKind) {
    array: *enc.Array,
    scalar: enc.Scalar,
};
