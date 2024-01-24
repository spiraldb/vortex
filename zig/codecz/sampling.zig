const std = @import("std");
const Allocator = std.mem.Allocator;

pub const DefaultNumSampleSlices = 10;
pub const DefaultSampleSliceLen = 64;

pub fn defaultSampleBufferSize(comptime T: type) usize {
    const sampleSizeInBytes = DefaultNumSampleSlices * DefaultSampleSliceLen * @sizeOf(T);
    const overheadInBytes = @sizeOf(SampleSliceIterator(T)) + DefaultNumSampleSlices * @sizeOf(ArraySlice);
    const sizeInBytes = sampleSizeInBytes + overheadInBytes + 128;
    return (std.math.divCeil(usize, sizeInBytes, 1024) catch unreachable) * 1024;
}

test "default sample buffer size" {
    try std.testing.expectEqual(defaultSampleBufferSize(f32), 3072);
    try std.testing.expectEqual(defaultSampleBufferSize(f64), 6144);
}

pub fn defaultSample(comptime T: type, gpa: std.mem.Allocator, vec: []const T) ![]const T {
    const numSampleSlices = 10;
    const sampleSliceLen = 64;
    const totalNumSamples = @min(vec.len, numSampleSlices * sampleSliceLen);

    var sampleList = try std.ArrayList(T).initCapacity(gpa, totalNumSamples);
    defer sampleList.deinit();

    if (vec.len <= totalNumSamples) {
        sampleList.appendSliceAssumeCapacity(vec);
    } else {
        var iter = try SampleSliceIterator(T).init(gpa, vec, sampleSliceLen, numSampleSlices);
        defer iter.deinit();
        while (iter.next()) |slice| {
            sampleList.appendSliceAssumeCapacity(slice);
        }
    }
    return try sampleList.toOwnedSlice();
}

fn stratifiedSlices(gpa: std.mem.Allocator, length: usize, sampleSliceLen: u16, numSampleSlices: u16) ![]const ArraySlice {
    const totalNumSamples: u64 = sampleSliceLen * numSampleSlices;
    if (totalNumSamples >= length) {
        const singleton = try gpa.alloc(ArraySlice, 1);
        singleton[0] = ArraySlice{ .start = 0, .stop = length };
        return singleton;
    }
    var slices = try std.ArrayList(ArraySlice).initCapacity(gpa, numSampleSlices);
    const partitions = try partitionIndices(gpa, length, numSampleSlices);
    defer gpa.free(partitions);

    var prng = std.rand.DefaultPrng.init(42);
    const rand = prng.random();

    for (partitions) |partSlice| {
        std.debug.assert(partSlice.stop - partSlice.start >= sampleSliceLen);
        const randomStart = rand.intRangeLessThan(usize, partSlice.start, partSlice.stop - sampleSliceLen);
        try slices.append(ArraySlice{ .start = randomStart, .stop = randomStart + sampleSliceLen });
    }

    return slices.toOwnedSlice();
}

/// Split a range of array indices into as-equal-as-possible slices. If the provided `num_partitions` doesn't
/// evenly divide into `length`, then the first `(length % num_partitions)` slices will have an extra element.
fn partitionIndices(gpa: std.mem.Allocator, length: usize, numPartitions: u32) ![]const ArraySlice {
    const numLongParts = length % numPartitions;
    const shortStep = length / numPartitions;
    const longStep = shortStep + 1;

    var partitions = try std.ArrayList(ArraySlice).initCapacity(gpa, numPartitions);
    defer partitions.deinit();
    var pos: usize = 0;
    for (0..numLongParts) |i| {
        pos = i * longStep;
        try partitions.append(.{ .start = pos, .stop = pos + longStep });
    }
    std.debug.assert(pos == numLongParts * longStep);
    while (pos < length) : (pos += shortStep) {
        try partitions.append(.{ .start = pos, .stop = pos + shortStep });
    }

    return partitions.toOwnedSlice();
}

pub const ArraySlice = struct {
    start: usize,
    stop: usize,
};

pub fn SampleSliceIterator(comptime T: type) type {
    return struct {
        const Self = @This();
        parent: []const T, // borrowed
        sampleSlices: []const ArraySlice, // owned
        index: usize = 0,
        totalNumSamplesCachedDoNotAccess: ?usize = null,
        allocator: std.mem.Allocator,

        pub fn init(allocator: std.mem.Allocator, parent: []const T, sampleSliceLen: u16, numSampleSlices: u16) !Self {
            return Self{
                .parent = parent,
                .sampleSlices = try stratifiedSlices(allocator, parent.len, sampleSliceLen, numSampleSlices),
                .allocator = allocator,
            };
        }

        pub fn initDefault(allocator: std.mem.Allocator, parent: []const T) !Self {
            const sampleSliceLen: u16 = 64;
            const numSampleSlices: u16 = 10;
            return init(allocator, parent, sampleSliceLen, numSampleSlices);
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.sampleSlices);
        }

        pub fn len(self: *const Self) usize {
            return self.sampleSlices.len;
        }

        pub fn next(self: *Self) ?[]const T {
            std.debug.assert(self.index <= self.sampleSlices.len);
            if (self.index == self.sampleSlices.len) {
                self.reset();
                return null;
            }

            const sliceIndices = self.sampleSlices[self.index];
            if (sliceIndices.start > sliceIndices.start or sliceIndices.start > self.parent.len - 1 or sliceIndices.stop > self.parent.len) {
                std.debug.panic("invalid slices of parent of len {}! {any}", .{ self.parent.len, self.sampleSlices });
            }

            self.index += 1;
            return self.parent[sliceIndices.start..sliceIndices.stop];
        }

        pub fn totalNumSamples(self: *Self) usize {
            if (self.totalNumSamplesCachedDoNotAccess) |ns| {
                return ns;
            } else {
                var ns: usize = 0;
                for (self.sampleSlices) |slice| {
                    ns += slice.stop - slice.start;
                }
                self.totalNumSamplesCachedDoNotAccess = ns;
                return ns;
            }
        }

        pub fn reset(self: *Self) void {
            self.index = 0;
        }
    };
}

test "sample" {
    var floats: [1000]f64 = undefined;
    for (&floats, 0..) |*f, i| {
        f.* = @floatFromInt(i);
    }

    var sampleIter = try SampleSliceIterator(f64).initDefault(std.testing.allocator, &floats);
    defer sampleIter.deinit();
    try std.testing.expectEqual(@as(usize, 640), sampleIter.totalNumSamples());
    for (0..10) |_| {
        var i: usize = 0;
        while (sampleIter.next()) |slice| : (i += 1) {
            try std.testing.expectEqual(@as(usize, 64), slice.len);
            for (slice) |item| {
                const intItem = std.math.lossyCast(usize, item);
                try std.testing.expect(intItem >= i * 100 and intItem < (i + 1) * 100);
            }
        }
        try std.testing.expectEqual(@as(usize, 10), i);
    }
}

test "sample iter small array" {
    var tooSmall: [10]f64 = undefined;
    for (&tooSmall, 0..) |*f, i| {
        f.* = @floatFromInt(i);
    }
    var sampleIter = try SampleSliceIterator(f64).initDefault(std.testing.allocator, &tooSmall);
    defer sampleIter.deinit();
    try std.testing.expectEqual(tooSmall.len, sampleIter.totalNumSamples());
    try std.testing.expectEqual(@as(usize, 1), sampleIter.len());
    try std.testing.expectEqualSlices(f64, &tooSmall, sampleIter.next().?);
}
