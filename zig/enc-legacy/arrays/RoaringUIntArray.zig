const std = @import("std");
const arrow = @import("arrow");
const enc = @import("../enc.zig");
const rc = @import("../refcnt.zig");
const serde = @import("../serde.zig");
const stats = @import("../stats/roaring_uint.zig");
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

/// Allocate with an uninitialized array.
fn allocEmpty(gpa: std.mem.Allocator) !*Self {
    const bitmap = try roaring.Bitmap.create();
    errdefer bitmap.free();
    const self = try allocWithOwnedBitmap(gpa, bitmap, 0);
    return self;
}

/// Allocate with a copy of the given zig bitset.
pub fn allocWithZigBitSet(gpa: std.mem.Allocator, zbs: std.bit_set.DynamicBitSet) !*Self {
    const bitmap = try roaring.Bitmap.create();
    errdefer bitmap.free();

    const len = zbs.count();
    var ints = try std.ArrayList(u32).initCapacity(gpa, len);
    defer ints.deinit();

    var iter = zbs.iterator(.{});
    while (iter.next()) |idx| {
        ints.appendAssumeCapacity(@intCast(idx));
    }
    return try allocWithInts(u32, gpa, ints.items);
}

pub fn allocWithInts(comptime T: type, gpa: std.mem.Allocator, ints: []const T) !*Self {
    if (comptime std.math.minInt(T) < 0) {
        const min = zimd.math.min(T, ints);
        if (min < 0) {
            std.debug.panic("provided integers contain at least one element ({}) less than 0", .{min});
        }
    }

    if (comptime std.math.maxInt(T) > std.math.maxInt(u32)) {
        const max = zimd.math.max(T, ints);
        if (max > std.math.maxValue(u32)) {
            std.debug.panic("provided integers contain at least one element ({}) too large for roaring bitmap", .{max});
        }
    }

    const bitmap = try roaring.Bitmap.create();
    if (comptime T == u32) {
        bitmap.addMany(ints);
    } else if (ints.len > 0) {
        for (ints) |i| {
            bitmap.add(@intCast(i));
        }
    }

    const self = try allocWithOwnedBitmap(gpa, bitmap, ints.len);
    self.optimize();
    return self;
}

/// Allocate a RoaringArray with the given bitmap. The RoaringArray takes ownership of the bitmap.
pub fn allocWithOwnedBitmap(gpa: std.mem.Allocator, bitmap: *roaring.Bitmap, len: usize) !*Self {
    std.debug.assert(bitmap.cardinality() == len);
    const dtype = enc.dtypes.uint32;
    const self = try gpa.create(Self);
    self.* = Self{
        .bitmap = bitmap,
        .allocator = gpa,
        .array = try enc.Array.init("enc.roaring_uint", &vtable, gpa, dtype, len),
    };

    return self;
}

pub fn indexOf(self: *const Self, index: usize) ?usize {
    // bitmap rank returns the number of set bits smaller than or equal to the given index
    // thus a value of 0 means that index is smaller than the index of the smallest set bit
    const rank = self.bitmap.rank(@intCast(index));
    if (rank == 0 or !self.bitmap.contains(@intCast(index))) {
        return null;
    }
    return @intCast(rank - 1);
}

/// Returns a slice containing all elements in the provided range of values [startVal, stopVal).
pub fn getValueRangeSlice(self: *const Self, gpa: std.mem.Allocator, startVal: usize, stopVal: usize) !*Self {
    if (startVal > stopVal) {
        std.debug.panic("startVal ({}) must be less than or equal to stopVal({})", .{ startVal, stopVal });
    }

    const len = self.bitmap.cardinalityRange(startVal, stopVal);
    if (len == 0) {
        return allocEmpty(gpa);
    }

    const ints = try gpa.alloc(u32, len);
    defer gpa.free(ints);

    var iter = self.bitmap.iterator();
    const hasNext = iter.moveEqualOrLarger(@intCast(startVal));
    std.debug.assert(hasNext);

    const numRead = iter.read(ints);
    std.debug.assert(numRead == len);

    return allocWithInts(u32, gpa, ints);
}

pub fn subtractOffsetInPlace(self: *Self, offset: usize) !void {
    if (offset == 0 or self.array.len == 0) {
        return;
    }

    const first = try self.get(0);
    if (offset > first) {
        std.debug.panic("offset ({}) is larger than the smallest value in the array ({})", .{ offset, first });
    }
    const newBitmap = try self.bitmap.addOffset(-@as(i64, @intCast(offset)));
    errdefer newBitmap.free();

    const oldBitmap = self.bitmap;
    defer oldBitmap.free();

    std.debug.assert(newBitmap.cardinality() == self.array.len);
    self.bitmap = newBitmap;
}

pub fn getSlice(self: *const Self, gpa: std.mem.Allocator, start: usize, stop: usize) !*enc.Array {
    if (start > stop) {
        std.debug.panic("start ({}) must be less than or equal to stop ({})", .{ start, stop });
    }
    if (start >= self.array.len or stop > self.array.len) {
        return enc.Error.IndexOutOfBounds;
    }

    const startVal = try self.get(start);
    const stopVal = try self.get(stop);

    const slice = try self.getValueRangeSlice(gpa, startVal, stopVal);
    return &slice.array;
}

pub fn numberOfElementsLessThan(self: *const Self, index: usize) usize {
    // bitmap rank returns the number of set bits smaller than or equal to the given index
    // thus a value of 0 means that index is smaller than the index of the smallest set bit
    const adjustment = @intFromBool(self.bitmap.contains(@intCast(index)));
    return @intCast(self.bitmap.rank(@intCast(index)) - adjustment);
}

pub fn get(self: *const Self, index: usize) !u32 {
    if (index >= self.array.len) {
        return enc.Error.IndexOutOfBounds;
    }
    var out: u32 = undefined;
    if (self.bitmap.select(@intCast(index), &out)) {
        return out;
    } else {
        std.debug.panic("failed to select rank {} from roaring bitmap despite being in bounds (len {})", .{
            index,
            self.array.len,
        });
    }
}

pub fn optimize(self: *Self) void {
    _ = self.bitmap.runOptimize();
    _ = self.bitmap.shrinkToFit();
}

pub fn toIntArray(self: *const Self, gpa: std.mem.Allocator) !*enc.PrimitiveArray {
    std.debug.assert(self.array.len == self.bitmap.cardinality());
    const ints = try gpa.alignedAlloc(u32, enc.Buffer.Alignment, self.array.len);
    errdefer gpa.free(ints);

    var iter = self.bitmap.iterator();
    const numRead = iter.read(ints);
    std.debug.assert(numRead == self.array.len);
    return enc.PrimitiveArray.allocWithOwnedSlice(gpa, u32, ints);
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
        _ = allocator;
        return enc.Scalar.init(try self.get(index));
    }

    pub fn getSlice(self: *const Self, gpa: std.mem.Allocator, start: usize, stop: usize) !*enc.Array {
        return self.getSlice(gpa, start, stop);
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

                const plain = try state.self.toIntArray(gpa);
                state.exported = true;
                return &plain.array;
            }

            pub fn deinit(state: *Iter) void {
                state.self.release();
            }
        }).alloc(allocator, .{ .self = self.retain() });
    }
};
