const std = @import("std");

pub fn cloneStrings(allocator: std.mem.Allocator, strings: []const []const u8) ![]const []const u8 {
    const new_strings = try allocator.alloc([]const u8, strings.len);
    for (strings, 0..) |str, i| {
        new_strings[i] = try allocator.dupe(u8, str);
    }
    return new_strings;
}
