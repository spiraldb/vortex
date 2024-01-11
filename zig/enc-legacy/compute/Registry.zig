const std = @import("std");
const enc = @import("../enc.zig");
const ec = @import("./compute.zig");
const ops = @import("./ops.zig");
const Type = std.builtin.Type;

const Self = @This();

functions: std.StringHashMap(ec.Function),

pub fn deinit(self: *Self) void {
    var iter = self.functions.iterator();
    while (iter.next()) |entry| {
        entry.value_ptr.*.deinit();
    }
    self.functions.deinit();
}

pub fn init(allocator: std.mem.Allocator) Self {
    return .{ .functions = std.StringHashMap(ec.Function).init(allocator) };
}

/// Initialize a registry with the default functions.
pub fn initWithDefaults(gpa: std.mem.Allocator) !Self {
    var self = init(gpa);
    inline for (@typeInfo(ops).Struct.decls) |d| {
        const decl = @field(ops, d.name);
        if (@TypeOf(decl) == type and @hasDecl(decl, "function")) {
            const function: ec.Function = try decl.function(gpa);
            try self.functions.put(function.name, function);
        }
    }
    return self;
}

pub fn findFunction(self: *const Self, name: []const u8) ?*ec.Function {
    return self.functions.getPtr(name);
}

pub fn call(self: *const Self, function: []const u8, ctx: enc.Ctx, params: []const ec.Param, options: *const anyopaque) anyerror!ec.Result {
    const func = self.findFunction(function) orelse return error.NoSuchFunction;
    return func.call(ctx, params, options);
}
