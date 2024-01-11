const std = @import("std");
const enc = @import("../enc.zig");
const rc = @import("../refcnt.zig");

const Self = @This();

const RefCnt = rc.SingleThreadedRefCnt(Self, "refcnt");
pub usingnamespace RefCnt.Fns();

chunks: []*enc.Array,
indexBounds: []usize,
refcnt: RefCnt = RefCnt.init(&destroy),
allocator: std.mem.Allocator,
array: enc.Array,
cachedChunkIdx: ?usize = null,

const vtable = enc.VTable.Lift(Encoding);

fn destroy(self: *Self) void {
    for (self.chunks) |chunk| chunk.release();
    self.allocator.free(self.chunks);
    self.allocator.free(self.indexBounds);

    self.array.deinit();
    self.allocator.destroy(self);
}

pub fn from(array: anytype) enc.Array.Downcast(@TypeOf(array), Self) {
    return @fieldParentPtr(Self, "array", array);
}

pub fn allocWithOwnedChunks(gpa: std.mem.Allocator, chunks: []*enc.Array) !*Self {
    const dtype: enc.DType = blk: {
        var dtype_: ?enc.DType = null;
        for (chunks) |chunk| {
            if (dtype_) |existing_dtype| {
                if (!existing_dtype.equal(chunk.dtype)) {
                    std.debug.panic("Chunks have different dtypes: {} and {}", .{ existing_dtype, chunk.dtype });
                }
            } else {
                dtype_ = chunk.dtype;
            }
        }
        break :blk dtype_ orelse enc.dtypes.null_;
    };

    var indexBounds = try gpa.alloc(usize, chunks.len + 1);
    errdefer gpa.free(indexBounds);

    indexBounds[0] = 0;
    const len = blk: {
        var len_: usize = 0;
        for (chunks, 1..) |chunk, i| {
            len_ += chunk.len;
            indexBounds[i] = len_;
        }
        break :blk len_;
    };
    std.debug.assert(indexBounds[chunks.len] == len);

    const self = try gpa.create(Self);
    self.* = Self{
        .chunks = chunks,
        .indexBounds = indexBounds,
        .allocator = gpa,
        .array = try enc.Array.init("enc.chunked", &vtable, gpa, dtype, len),
    };

    return self;
}

const PhysicalLocation = struct { chunkIdx: usize, idxInChunk: usize };
inline fn logicalIndexToPhysical(self: *const Self, index: usize) PhysicalLocation {
    if (index >= self.array.len) {
        std.debug.panic("index out of bounds: index {}, len {}", .{ index, self.array.len });
    }

    var minChunkIndexInclusive: usize = 0;
    var maxChunkIndexExclusive: usize = self.chunks.len;

    // we cache the most-recently returned chunkIdx to avoid doing a binary search for each element in sequential scan
    var current: usize = @min(self.cachedChunkIdx orelse maxChunkIndexExclusive / 2, maxChunkIndexExclusive - 1);
    // binary search should never take more than N iterations
    for (0..self.chunks.len) |_| {
        if (index < self.indexBounds[current]) {
            // if index is less than the current chunk's inclusive lower bound, go left
            maxChunkIndexExclusive = current;
        } else if (index >= self.indexBounds[current + 1]) {
            // if index is greater than or equal to the current chunk's exclusive upper bound, go right
            minChunkIndexInclusive = current + 1;
        } else {
            // found it! cache the value in case we're calling this in a hot loop
            @constCast(self).cachedChunkIdx = current;
            return PhysicalLocation{
                .chunkIdx = current,
                .idxInChunk = index - self.indexBounds[current],
            };
        }
        std.debug.assert(maxChunkIndexExclusive > minChunkIndexInclusive);
        current = minChunkIndexInclusive + (maxChunkIndexExclusive - minChunkIndexInclusive) / 2;
    }
    std.debug.panic("binary search of index bounds failed!! index {}, bounds {any}", .{ index, self.indexBounds });
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
        var nbytes: usize = 0;
        for (self.chunks) |chunk| {
            nbytes += try chunk.getNBytes();
        }
        return nbytes;
    }

    pub fn getScalar(self: *const Self, gpa: std.mem.Allocator, index: usize) !enc.Scalar {
        const location = self.logicalIndexToPhysical(index);
        return self.chunks[location.chunkIdx].getScalar(gpa, location.idxInChunk);
    }

    pub fn getSlice(self: *const Self, gpa: std.mem.Allocator, start: usize, stop: usize) !*enc.Array {
        std.debug.assert(stop >= start);
        const startLoc = self.logicalIndexToPhysical(start);
        const stopLoc = self.logicalIndexToPhysical(stop);
        const chunksToSlice = self.chunks[startLoc.chunkIdx .. stopLoc.chunkIdx + 1];

        const newChunks = try gpa.alloc(*enc.Array, chunksToSlice.len);
        for (chunksToSlice, 0..) |chunk, i| {
            const startIdx = if (i == 0) startLoc.idxInChunk else 0;
            const stopIdx = if (i == chunksToSlice.len - 1) stopLoc.idxInChunk else chunk.len;
            newChunks[i] = try chunk.getSlice(gpa, startIdx, stopIdx);
        }

        var slice = try allocWithOwnedChunks(gpa, newChunks);
        return &slice.array;
    }

    pub fn getMasked(self: *const Self, gpa: std.mem.Allocator, mask: *const enc.Array) !*enc.Array {
        _ = mask;
        _ = gpa;
        _ = self;
        std.debug.panic("ChunkedArray.getMasked is not implemented", .{});
    }

    pub inline fn computeStatistic(self: *const Self, allocator: std.mem.Allocator, stat: enc.Stats.Stat) !enc.Scalar {
        _ = stat;
        _ = allocator;
        _ = self;
        return enc.Error.StatisticNotSupported;
    }

    pub fn iterPlain(self: *const Self, allocator: std.mem.Allocator) !enc.Array.Iterator {
        return enc.Array.Iterator.WithState(struct {
            const Iter = @This();

            self: *Self,
            nextIdx: usize,
            currentIter: ?enc.Array.Iterator,

            pub fn next(iter: *Iter, gpa: std.mem.Allocator) !?*enc.Array {
                var res = if (iter.currentIter) |cIt| try cIt.next(gpa) else null;
                while (res == null and iter.nextIdx < iter.self.chunks.len) {
                    const idx = iter.nextIdx;
                    iter.nextIdx += 1;
                    const it = try iter.self.chunks[idx].iterPlain(gpa);
                    if (iter.currentIter) |cIt| {
                        cIt.deinit();
                    }
                    iter.currentIter = it;
                    res = try it.next(gpa);
                }
                return res;
            }

            pub fn deinit(iter: *Iter) void {
                if (iter.currentIter) |curIt| {
                    curIt.deinit();
                }
                iter.self.release();
            }
        }).alloc(
            allocator,
            .{ .self = self.retain(), .nextIdx = 0, .currentIter = null },
        );
    }
};

test "iterPlain" {
    const constArr1 = try enc.ConstantArray.allocWithOwnedScalar(std.testing.allocator, enc.Scalar.init(42), 1000);
    const constArr2 = try enc.ConstantArray.allocWithOwnedScalar(std.testing.allocator, enc.Scalar.init(42), 1000);
    const arrays = try std.testing.allocator.dupe(*enc.Array, &.{ &constArr1.array, &constArr2.array });
    const chunked = try Self.allocWithOwnedChunks(std.testing.allocator, @constCast(arrays));
    defer chunked.release();
    const iter = try chunked.array.iterPlain(std.testing.allocator);
    defer iter.deinit();
    const expected = &([_]i64{42} ** 1000);
    while (try iter.next(std.testing.allocator)) |chunk| {
        if (chunk.kind) |k| {
            switch (k) {
                .primitive => {
                    try std.testing.expectEqualSlices(i64, expected, enc.PrimitiveArray.from(chunk).asSlice(i64));
                    chunk.release();
                },
                else => std.debug.panic("Expected primitive array, got {s}", .{@tagName(k)}),
            }
        } else {
            std.debug.panic("Expected builtin array", .{});
        }
    }
}
