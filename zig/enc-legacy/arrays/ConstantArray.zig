const std = @import("std");
const enc = @import("../enc.zig");
const rc = @import("../refcnt.zig");
const stats = @import("../stats/constant.zig");

const Self = @This();

const RefCnt = rc.SingleThreadedRefCnt(Self, "refcnt");
pub usingnamespace RefCnt.Fns();

scalar: enc.Scalar,
refcnt: RefCnt = RefCnt.init(&destroy),
allocator: std.mem.Allocator,
array: enc.Array,

const vtable = enc.VTable.Lift(Encoding);

fn destroy(self: *Self) void {
    self.scalar.deinit();
    self.array.deinit();
    self.allocator.destroy(self);
}

pub fn from(array: anytype) enc.Array.Downcast(@TypeOf(array), Self) {
    return @fieldParentPtr(Self, "array", array);
}

pub fn allocWithOwnedScalar(gpa: std.mem.Allocator, scalar: enc.Scalar, len: usize) !*Self {
    const self = try gpa.create(Self);
    self.* = .{
        .scalar = scalar,
        .allocator = gpa,
        .array = try enc.Array.init("enc.constant", &vtable, gpa, try scalar.getDType(gpa), len),
    };
    return self;
}

pub fn fromArray(gpa: std.mem.Allocator, array: *const enc.Array) !*Self {
    std.debug.assert(if (array.stats.get(.is_constant)) |constant| constant.bool.value else false);
    return allocWithOwnedScalar(gpa, try array.getScalar(gpa, 0), array.len);
}

//
// Encoding Functions
//
const Encoding = struct {
    pub inline fn from(array: anytype) enc.Array.Downcast(@TypeOf(array), Self) {
        return Self.from(array);
    }

    pub inline fn retain(self: *const Self) *enc.Array {
        return &self.retain().array;
    }
    pub inline fn release(self: *Self) void {
        self.release();
    }

    pub fn getNBytes(self: *const Self) !usize {
        _ = self;
        return @sizeOf(enc.Scalar);
    }

    pub fn getScalar(self: *const Self, gpa: std.mem.Allocator, index: usize) !enc.Scalar {
        if (index >= self.array.len) {
            return error.IndexOutOfBounds;
        }
        return self.scalar.clone(gpa);
    }

    pub fn getSlice(self: *const Self, gpa: std.mem.Allocator, start: usize, stop: usize) !*enc.Array {
        if (stop >= self.array.len) {
            return error.IndexOutOfBounds;
        }
        const sliced = try allocWithOwnedScalar(gpa, try self.scalar.clone(gpa), stop - start);
        return &sliced.array;
    }

    pub fn getMasked(self: *const Self, gpa: std.mem.Allocator, mask: *const enc.Array) !*enc.Array {
        const masked = try allocWithOwnedScalar(gpa, try self.scalar.clone(gpa), mask.len);
        return &masked.array;
    }

    pub inline fn computeStatistic(self: *const Self, allocator: std.mem.Allocator, stat: enc.Stats.Stat) !enc.Scalar {
        return stats.compute(self, allocator, stat);
    }

    pub fn iterPlain(self: *const Self, allocator: std.mem.Allocator) !enc.Array.Iterator {
        // TODO(ngates): we can surely improve upon this? Maybe an ArrayBuilder that accepts scalars...
        return try enc.Array.Iterator.WithState(struct {
            const Iter = @This();

            self: *Self,
            exported: bool = false,

            pub fn next(state: *Iter, ally: std.mem.Allocator) !?*enc.Array {
                if (state.exported) {
                    return null;
                }
                state.exported = true;

                const dtype = try state.self.scalar.getDType(ally);

                if (dtype.toPType()) |ptype| {
                    const result = try enc.PrimitiveArray.allocEmpty(ally, ptype, state.self.array.len);
                    // TODO(ngates): instead of this, maybe we have a way for PType's to just do a plain memcpy?
                    // We don't care most of the time about the actual type of the scalar, just the size.
                    switch (ptype) {
                        inline else => |p| {
                            const T = p.astype();
                            @memset(result.asMutableSlice(T), state.self.scalar.as(T));
                        },
                    }
                    return &result.array;
                }

                if (dtype == .bool) {
                    var result = try enc.BoolArray.allocEmpty(ally, state.self.array.len);
                    const slice = result.asSlice();

                    std.debug.assert(slice.bit_offset == 0);
                    @memset(slice.bytes, @as(u8, state.self.scalar.as(u1)) << 7);
                    return &result.array;
                }

                std.debug.panic("Unsupported constant scalar {}", .{state.self});
            }

            pub fn deinit(state: *Iter) void {
                state.self.release();
            }
        }).alloc(allocator, .{ .self = self.retain() });
    }
};
