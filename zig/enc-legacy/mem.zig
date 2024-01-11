const std = @import("std");

/// A slice that will invoke deinit on each element prior to deallocation.
pub fn DeinitSlice(comptime T: type, comptime deinitFunction: anytype) type {
    return struct {
        const Self = @This();

        items: []T,
        allocator: ?std.mem.Allocator,

        pub fn deinit(self: Self) void {
            for (self.items) |item| deinitFunction(item);
            if (self.allocator) |ally| ally.free(self.items);
        }
    };
}
