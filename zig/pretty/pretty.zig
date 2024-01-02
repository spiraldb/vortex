const std = @import("std");

pub fn List(comptime T: type, comptime parens: *const [2]u8) type {
    return struct {
        items: []const T,

        pub fn init(items: []const T) @This() {
            return .{ .items = items };
        }

        pub fn format(value: @This(), comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
            _ = fmt;
            _ = options;
            try writer.writeAll(parens[0..1]);
            for (value.items, 0..) |item, i| {
                try writer.print("{any}", .{item});
                if (i < value.items.len - 1) {
                    try writer.writeAll(", ");
                }
            }
            try writer.writeAll(parens[1..2]);
        }
    };
}

pub fn KeyValue(comptime K: type, comptime V: type) type {
    return struct {
        keys: []const K,
        values: []const V,

        pub fn format(self: @This(), comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
            _ = options;
            _ = fmt;
            try writer.writeAll("{ ");
            for (self.keys, self.values, 0..) |key, value, i| {
                try writer.print("{any}: {any}", .{ key, value });
                if (i < self.keys.len - 1) {
                    try writer.writeAll(", ");
                }
            }
            try writer.writeAll(" }");
        }
    };
}
