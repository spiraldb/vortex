const std = @import("std");
const arrow = @import("arrow");
const encArrow = @import("../arrow.zig");
const enc = @import("../enc.zig");
const cloning = @import("../cloning.zig");
const rc = @import("../refcnt.zig");
const itertools = @import("../itertools.zig");
const stats = @import("../stats/patched.zig");

const Self = @This();

const RefCnt = rc.SingleThreadedRefCnt(Self, "refcnt");
pub usingnamespace RefCnt.Fns();

base: *enc.Array,
patchIndices: *enc.RoaringUIntArray,
patchValues: *enc.Array,
refcnt: RefCnt = RefCnt.init(&destroy),
allocator: std.mem.Allocator,
array: enc.Array,

const vtable = enc.VTable.Lift(Encoding);

fn destroy(self: *Self) void {
    self.base.release();
    self.patchIndices.release();
    self.patchValues.release();

    self.array.deinit();
    self.allocator.destroy(self);
}

pub fn from(array: anytype) enc.Array.Downcast(@TypeOf(array), Self) {
    return @fieldParentPtr(Self, "array", array);
}

pub fn allocWithOwnedChildren(gpa: std.mem.Allocator, base: *enc.Array, patchIndices: *enc.RoaringUIntArray, patchValues: *enc.Array) !*Self {
    const dtype: enc.DType = blk: {
        if (!base.dtype.equal(patchValues.dtype)) {
            std.debug.panic("dtype mismatch: base {}, patch {}", .{ base.dtype, patchValues.dtype });
        }
        break :blk base.dtype;
    };

    const maxPatchIndex = try patchIndices.array.computeStatistic(gpa, .max);
    defer maxPatchIndex.deinit();
    if (maxPatchIndex.as(usize) >= base.len) {
        std.debug.panic("patch index out of bounds: index {}, len {}", .{ maxPatchIndex.as(usize), base.len });
    }

    if (patchIndices.array.len != patchValues.len) {
        std.debug.panic("patchIndices.len != patchValues.len: {} != {}", .{ patchIndices.array.len, patchValues.len });
    }

    const self = try gpa.create(Self);
    self.* = Self{
        .base = base,
        .patchIndices = patchIndices,
        .patchValues = patchValues,
        .allocator = gpa,
        .array = try enc.Array.init("enc.patched", &vtable, gpa, dtype, base.len),
    };

    return self;
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

    pub inline fn getNBytes(self: *const Self) !usize {
        return try self.base.getNBytes() + try self.patchIndices.array.getNBytes() + try self.patchValues.getNBytes();
    }

    pub inline fn getScalar(self: *const Self, gpa: std.mem.Allocator, index: usize) !enc.Scalar {
        if (self.patchIndices.indexOf(index)) |patchValuesIndex| {
            return self.patchValues.getScalar(gpa, patchValuesIndex);
        } else {
            return self.base.getScalar(gpa, index);
        }
    }

    pub fn getSlice(self: *const Self, gpa: std.mem.Allocator, start: usize, stop: usize) !*enc.Array {
        std.debug.assert(stop >= start);

        const baseSlice = try self.base.getSlice(gpa, start, stop);
        errdefer baseSlice.release();

        var patchIndicesSlice = try self.patchIndices.getValueRangeSlice(gpa, start, stop);
        errdefer patchIndicesSlice.release();
        if (patchIndicesSlice.array.len == 0) {
            patchIndicesSlice.release();
            return baseSlice;
        }

        // otherwise, we need to adjust all of the index values in patchIndicesSlice, and also grab the corresponding patch values
        try patchIndicesSlice.subtractOffsetInPlace(start);
        if (self.patchIndices.indexOf(try patchIndicesSlice.get(0))) |patchValueStart| {
            const patchValuesSlice = try self.patchValues.getSlice(gpa, patchValueStart, patchValueStart + patchIndicesSlice.array.len);
            errdefer patchValuesSlice.release();
            const slice = try allocWithOwnedChildren(gpa, baseSlice, patchIndicesSlice, patchValuesSlice);
            return &slice.array;
        } else {
            std.debug.panic("patchIndicesSlice.get(0) not in patchIndices", .{});
        }
    }

    pub fn getMasked(self: *const Self, gpa: std.mem.Allocator, mask: *const enc.Array) !*enc.Array {
        _ = mask;
        _ = gpa;
        _ = self;
        std.debug.panic("PatchedArray.getMasked is not implemented", .{});
    }

    pub inline fn computeStatistic(self: *const Self, allocator: std.mem.Allocator, stat: enc.Stats.Stat) !enc.Scalar {
        return stats.compute(self, allocator, stat);
    }

    pub fn iterPlain(self: *const Self, allocator: std.mem.Allocator) !enc.Array.Iterator {
        return enc.Array.Iterator.WithState(struct {
            const Iter = @This();

            self: *Self,
            baseOffset: usize,
            baseIter: ?enc.Array.Iterator,
            patchIndicesIter: ?enc.Array.Iterator,
            patchValuesIter: ?enc.Array.Iterator,

            pub fn next(iter: *Iter, gpa: std.mem.Allocator) !?*enc.Array {
                _ = gpa;
                if (iter.baseOffset == iter.self.base.len) {
                    return null;
                }

                std.debug.panic("PatchedArray.iterPlain is not implemented", .{}); // TODO(wmanning: implement this)
            }

            pub fn deinit(iter: *Iter) void {
                if (iter.baseIter) |it| {
                    it.deinit();
                }
                if (iter.patchIndicesIter) |it| {
                    it.deinit();
                }
                if (iter.patchValuesIter) |it| {
                    it.deinit();
                }
                iter.self.release();
            }
        }).alloc(
            allocator,
            .{ .self = self.retain(), .baseOffset = 0, .baseIter = null, .patchIndicesIter = null, .patchValuesIter = null },
        );
    }
};
