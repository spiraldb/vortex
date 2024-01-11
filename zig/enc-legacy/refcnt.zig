const std = @import("std");

/// Single-threaded ref counter.
/// Look into this for atomic: https://ravendb.net/articles/atomic-reference-counting-with-zig-code-samples
pub fn SingleThreadedRefCnt(comptime ParentType: type, comptime parentField: []const u8) type {
    return struct {
        const Self = @This();

        cnt: i32,
        destroy: *const fn (*ParentType) void,

        pub fn init(destroy: *const fn (*ParentType) void) Self {
            // std.debug.print("REFCNT {*}\n", .{cnt});
            // std.debug.dumpCurrentStackTrace(@returnAddress());
            // std.debug.print("\n\n", .{});
            return Self{ .cnt = 1, .destroy = destroy };
        }

        pub fn isExclusive(self: *const Self) bool {
            return self.cnt == 1;
        }

        pub fn incref(self: *Self) void {
            self.cnt += 1;
            // std.debug.print("INCREF {*}\n", .{@fieldParentPtr(ParentType, parentField, self)});
            // std.debug.dumpCurrentStackTrace(@returnAddress());
            // std.debug.print("\n\n", .{});
        }

        pub fn decref(self: *Self) void {
            self.cnt -= 1;
            // std.debug.print("DECREF {} {*}\n", .{ self.cnt, @fieldParentPtr(ParentType, parentField, self) });
            // std.debug.dumpCurrentStackTrace(@returnAddress());
            // std.debug.print("\n\n", .{});

            if (self.cnt <= 0) {
                const ptr = @fieldParentPtr(ParentType, parentField, self);
                self.destroy(ptr);
            }
        }

        pub fn Fns() type {
            return struct {
                pub fn retain(self: *const ParentType) *ParentType {
                    const mut_self = @constCast(self);
                    @field(mut_self, parentField).incref();
                    return mut_self;
                }

                pub fn release(self: *ParentType) void {
                    @field(self, parentField).decref();
                }
            };
        }
    };
}
