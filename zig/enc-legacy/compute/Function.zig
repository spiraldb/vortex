const std = @import("std");
const enc = @import("../enc.zig");
const ec = @import("./compute.zig");
const pretty = @import("pretty");

const Self = @This();

name: []const u8,
doc: []const u8,
param_kinds: []const ?ec.ParamKind,
allocator: std.mem.Allocator,
kernels: std.ArrayList(ec.Kernel),

pub fn deinit(self: *Self) void {
    self.allocator.free(self.name);
    self.allocator.free(self.doc);
    self.allocator.free(self.param_kinds);
    for (self.kernels.items) |*kernel| kernel.deinit();
    self.kernels.deinit();
}

pub fn init(
    allocator: std.mem.Allocator,
    name: []const u8,
    doc: []const u8,
    param_kinds: []const ?ec.ParamKind,
) !Self {
    return .{
        .name = try allocator.dupe(u8, name),
        .doc = try allocator.dupe(u8, doc),
        .param_kinds = try allocator.dupe(?ec.ParamKind, param_kinds),
        .allocator = allocator,
        .kernels = std.ArrayList(ec.Kernel).init(allocator),
    };
}

/// Registers a kernel implementation for this function.
pub fn registerOwnedKernel(self: *Self, kernel: ec.Kernel) !void {
    if (!kernel.matchesParamKinds(self.param_kinds)) {
        return error.InvalidArguments;
    }
    try self.kernels.append(kernel);
}

pub fn unregisterKernel(self: *Self, kernel: ec.Kernel) bool {
    for (self.kernels.items, 0..) |k, i| {
        if (std.meta.eql(k, kernel)) {
            var removed = self.kernels.swapRemove(i);
            removed.deinit();
            return true;
        }
    }
    return false;
}

/// Invoke this function with the given parameters, resolving an appropriate output dtype.
pub fn call(self: *const Self, ctx: enc.Ctx, params: []const ec.Param, options: *const anyopaque) anyerror!ec.Result {
    if (self.param_kinds.len != params.len) {
        return error.InvalidArguments;
    }

    for (self.kernels.items) |kernel| {
        if (kernel.matchesParams(params)) {
            return kernel.call(ctx, params, options) catch |err| switch (err) {
                // If a kernel returns NoKernel at runtime, then we move on and try the next matching kernel.
                error.NoKernel => continue,
                else => return err,
            };
        }
    }

    std.debug.print("No kernel {s} matching params {any}. Choosing from {any}\n", .{
        self.name, params, self.kernels.items,
    });
    return error.NoKernel;
}
