const std = @import("std");
const ec = @import("./compute.zig");

const Self = @This();

registry: ec.Registry,
gpa: std.mem.Allocator,

pub fn testing() Self {
    return init(std.testing.allocator) catch @panic("Failed to setup testing ctx");
}

pub fn init(gpa: std.mem.Allocator) !Self {
    return .{
        .registry = try ec.Registry.initWithDefaults(gpa),
        .gpa = gpa,
    };
}

pub fn deinit(self: *Self) void {
    self.registry.deinit();
}
