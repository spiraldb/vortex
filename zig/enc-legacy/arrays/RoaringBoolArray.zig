const std = @import("std");
const enc = @import("../enc.zig");
const rc = @import("../refcnt.zig");
const stats = @import("../stats/roaring_bool.zig");
const roaring = @import("roaring");
const zimd = @import("zimd");

const Self = @This();

const RefCnt = rc.SingleThreadedRefCnt(Self, "refcnt");
pub usingnamespace RefCnt.Fns();

bitmap: *roaring.Bitmap,
refcnt: RefCnt = RefCnt.init(&destroy),
allocator: std.mem.Allocator,
array: enc.Array,

const vtable = enc.VTable.Lift(Encoding);

fn destroy(self: *Self) void {
    self.bitmap.free();
    self.array.deinit();
    self.allocator.destroy(self);
}

pub fn from(array: anytype) enc.Array.Downcast(@TypeOf(array), Self) {
    return @fieldParentPtr(Self, "array", array);
}

/// Allocate an uninitialized bool array of the requested length.
pub fn allocEmpty(gpa: std.mem.Allocator, len: usize) !*Self {
    const bitmap = try roaring.Bitmap.create();
    errdefer bitmap.free();
    return allocWithOwnedBitmap(gpa, bitmap, len);
}

/// Allocate with a copy of the given bools
pub fn allocWithBools(gpa: std.mem.Allocator, bools: []const bool) !*Self {
    const self = try allocEmpty(gpa, bools.len);
    errdefer self.destroy();

    for (bools, 0..) |b, i| {
        if (b) {
            self.bitmap.add(@intCast(i));
        }
    }
    self.optimize();
    return self;
}

/// Allocate a boolean array with an owned buffer.
pub fn allocWithOwnedBitmap(gpa: std.mem.Allocator, bitmap: *roaring.Bitmap, len: usize) !*Self {
    const self = try gpa.create(Self);
    if (bitmap.cardinality() > len) {
        return enc.Error.IndexOutOfBounds;
    }

    self.* = Self{
        .bitmap = bitmap,
        .allocator = gpa,
        .array = try enc.Array.init("enc.roaring", &vtable, gpa, enc.dtypes.bool_, len),
    };

    return self;
}

pub fn optimize(self: *Self) void {
    _ = self.bitmap.runOptimize();
    _ = self.bitmap.shrinkToFit();
}

pub fn getScalar(self: *const Self, gpa: std.mem.Allocator, index: usize) !enc.Scalar {
    _ = gpa;
    if (index >= self.array.len) {
        return enc.Error.IndexOutOfBounds;
    }
    return enc.Scalar.init(self.bitmap.contains(@intCast(index)));
}

pub fn toBoolArray(self: *const Self, gpa: std.mem.Allocator) !*enc.BoolArray {
    // we manually allocate the backing bitset array with enough memory in the correct alignment, using the roaring allocator
    const numWords = try std.math.divCeil(usize, self.bitmap.maximum(), @bitSizeOf(u64));
    const roaringArray = try roaring.aligned_allocator.alignedAlloc(u64, enc.Buffer.Alignment, numWords);
    errdefer roaring.aligned_allocator.free(roaringArray);

    // this is on the stack, since we discard the bitset structure after passing ownership of the backing array
    var bitset = roaring.Bitset{ .array = @ptrCast(roaringArray.ptr), .arraysize = roaringArray.len, .capacity = roaringArray.len };

    // this should not do any additional allocation
    try self.bitmap.copyToBitset(&bitset);
    std.debug.assert(@intFromPtr(bitset.array) == @intFromPtr(roaringArray.ptr));

    const Closure = struct {
        fn deinit(b: *enc.Buffer) void {
            roaring.aligned_allocator.free(b.bytes);
        }
    };

    const buffer = try gpa.create(enc.Buffer);
    buffer.* = enc.Buffer{
        .bytes = @alignCast(std.mem.sliceAsBytes(roaringArray)),
        .is_mutable = true,
        .ptr = null,
        .deinit = &Closure.deinit,
        .gpa = gpa,
    };
    errdefer buffer.release();
    return enc.BoolArray.allocWithOwnedBuffer(gpa, buffer, 0, self.array.len);
}

//
// Encoding Functions
//
const Encoding = struct {
    pub fn from(array: anytype) enc.Array.Downcast(@TypeOf(array), Self) {
        return Self.from(array);
    }

    pub inline fn retain(self: *const Self) *enc.Array {
        return &self.retain().array;
    }

    pub inline fn release(self: *Self) void {
        self.release();
    }

    pub fn getNBytes(self: *const Self) !usize {
        return self.bitmap.sizeInBytes();
    }

    pub fn getScalar(self: *const Self, allocator: std.mem.Allocator, index: usize) !enc.Scalar {
        return self.getScalar(allocator, index);
    }

    pub fn getSlice(self: *const Self, gpa: std.mem.Allocator, start: usize, stop: usize) !*enc.Array {
        _ = self;
        _ = gpa;
        _ = start;
        _ = stop;

        std.debug.panic("Roaring getSlice not supported", .{});
    }

    pub fn getMasked(self: *const Self, allocator: std.mem.Allocator, mask: *const enc.Array) !*enc.Array {
        _ = allocator;
        _ = mask;
        _ = self;
        std.debug.panic("Boolean getMasked not supported", .{});
    }

    pub inline fn computeStatistic(self: *const Self, allocator: std.mem.Allocator, stat: enc.Stats.Stat) !enc.Scalar {
        return stats.compute(self, allocator, stat);
    }

    pub fn iterPlain(self: *const Self, allocator: std.mem.Allocator) !enc.Array.Iterator {
        return try enc.Array.Iterator.WithState(struct {
            const Iter = @This();

            self: *Self,
            exported: bool = false,

            pub fn next(state: *Iter, gpa: std.mem.Allocator) !?*enc.Array {
                if (state.exported) {
                    return null;
                }

                const plain = try state.self.toBoolArray(gpa);
                state.exported = true;
                return &plain.array;
            }

            pub fn deinit(state: *Iter) void {
                state.self.release();
            }
        }).alloc(allocator, .{ .self = self.retain() });
    }
};
