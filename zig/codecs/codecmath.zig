const std = @import("std");
const Allocator = std.mem.Allocator;
const benchmarks = @import("benchmarks.zig");

pub fn comptimeCheckFloat(comptime F: type) void {
    if (@typeInfo(F) != .Float) {
        @compileError("unknown floating point type " ++ @typeName(F));
    }
}

pub fn comptimeCheckInt(comptime T: type) void {
    const tpInfo = @typeInfo(T);
    if (tpInfo != .Int and tpInfo != .ComptimeInt) {
        @compileError("unknown integer type " ++ @typeName(T));
    }
}

pub fn coveringIntBits(comptime F: type) comptime_int {
    // we should allocate at least fraction size + 1 MSB bit + 1 sign bit. this ensures coverage
    // of the full range of integers where every one is exactly representable in the corresponding float type.
    // NOTE: std.math.floatFractionalBits is not the same as std.math.floatMantissaBits because
    // for some godforsaken reason, f80 *does not* have the implicit MSB bit on its mantissa
    return std.math.floatFractionalBits(F) + 2;
}

pub fn coveringIntMax(comptime F: type) comptime_float {
    return @floatFromInt(std.math.maxInt(std.meta.Int(.signed, coveringIntBits(F))));
}

pub fn coveringIntMin(comptime F: type) comptime_float {
    return @floatFromInt(std.math.minInt(std.meta.Int(.signed, coveringIntBits(F))));
}

pub fn coveringIntTypePowerOfTwo(comptime F: type) type {
    // we round up coveringIntBits to next power of 2, since we might as well use every physically allocated bit
    return std.meta.Int(.signed, std.math.ceilPowerOfTwoAssert(u8, coveringIntBits(F)));
}

test "covering int bit-width" {
    try std.testing.expectEqual(12, coveringIntBits(f16));
    try std.testing.expectEqual(25, coveringIntBits(f32));
    try std.testing.expectEqual(54, coveringIntBits(f64));
    try std.testing.expectEqual(65, coveringIntBits(f80));
    try std.testing.expectEqual(114, coveringIntBits(f128));
}

test "covering int type" {
    try std.testing.expectEqual(i16, coveringIntTypePowerOfTwo(f16));
    try std.testing.expectEqual(i32, coveringIntTypePowerOfTwo(f32));
    try std.testing.expectEqual(i64, coveringIntTypePowerOfTwo(f64));
    try std.testing.expectEqual(i128, coveringIntTypePowerOfTwo(f80));
    try std.testing.expectEqual(i128, coveringIntTypePowerOfTwo(f128));
}

pub inline fn fastFloatRound(comptime F: type, val: F) F {
    const sweet = comptime blk: {
        const bits = std.math.floatFractionalBits(F);
        const bitsf = @as(F, @floatFromInt(bits));
        const bitsf_m1 = @as(F, @floatFromInt(bits - 1));
        break :blk (@as(F, @exp2(bitsf) + @exp2(bitsf_m1)));
    };
    return (val + sweet) - sweet;
}

test "fast fp round" {
    const types: [5]type = .{ f16, f32, f64, f80, f128 };
    const vals: [12]comptime_float = .{ -10.0, -5.5, -0.0, 0.0, 1.99, 2.0, 2.1, 2.4, 2.5, 2.6, 3000.0, 2_500_000_000_000_000.0 };
    inline for (types) |V| {
        inline for (vals) |val| {
            const fast_rounded = fastFloatRound(V, val);
            const float_val: V = @as(V, val);

            const slow_rounded = @round(@as(V, val));
            const slow_ceil = @as(@TypeOf(fast_rounded), @ceil(float_val));
            const slow_floor = @as(@TypeOf(fast_rounded), @floor(float_val));
            if (fast_rounded != slow_rounded) {
                std.debug.print("testing type {} and val {}, got {} from fast_fp_round, {} from @round, {} from @ceil, {} from @floor\n", .{ V, val, fast_rounded, slow_rounded, slow_ceil, slow_floor });
            }
            try std.testing.expect(fast_rounded == slow_ceil or fast_rounded == slow_floor);
        }
    }
}

// we explicitly enumerate these values in code such that the compiler will treat them
// as type `comptime_float` and then coerce to the specified float type.
// we do this in order to minimize floating point errors from e.g., generating the values
pub inline fn powersOfTen(comptime F: type) *[maxExponentToTry(F) + 1]F {
    comptime var floats = [36]F{
        1.0,
        10.0,
        100.0,
        1_000.0,
        10_000.0,
        100_000.0,
        1_000_000.0,
        10_000_000.0,
        100_000_000.0,
        1_000_000_000.0,
        10_000_000_000.0,
        100_000_000_000.0,
        1_000_000_000_000.0,
        10_000_000_000_000.0,
        100_000_000_000_000.0,
        1_000_000_000_000_000.0,
        10_000_000_000_000_000.0,
        100_000_000_000_000_000.0,
        1_000_000_000_000_000_000.0,
        10_000_000_000_000_000_000.0,
        100_000_000_000_000_000_000.0,
        1_000_000_000_000_000_000_000.0,
        10_000_000_000_000_000_000_000.0,
        100_000_000_000_000_000_000_000.0,
        1_000_000_000_000_000_000_000_000.0,
        10_000_000_000_000_000_000_000_000.0,
        100_000_000_000_000_000_000_000_000.0,
        1_000_000_000_000_000_000_000_000_000.0,
        10_000_000_000_000_000_000_000_000_000.0,
        100_000_000_000_000_000_000_000_000_000.0,
        1_000_000_000_000_000_000_000_000_000_000.0,
        10_000_000_000_000_000_000_000_000_000_000.0,
        100_000_000_000_000_000_000_000_000_000_000.0,
        1_000_000_000_000_000_000_000_000_000_000_000.0,
        10_000_000_000_000_000_000_000_000_000_000_000.0,
        100_000_000_000_000_000_000_000_000_000_000_000.0,
    };
    return floats[0 .. maxExponentToTry(F) + 1];
}

pub inline fn inversePowersOfTen(comptime F: type) *[maxExponentToTry(F) + 1]F {
    comptime var floats = [36]F{
        1.0,
        0.1,
        0.01,
        0.001,
        0.0001,
        0.00001,
        0.000001,
        0.0000001,
        0.00000001,
        0.000000001,
        0.0000000001,
        0.00000000001,
        0.000000000001,
        0.0000000000001,
        0.00000000000001,
        0.000000000000001,
        0.0000000000000001,
        0.00000000000000001,
        0.000000000000000001,
        0.0000000000000000001,
        0.00000000000000000001,
        0.000000000000000000001,
        0.0000000000000000000001,
        0.00000000000000000000001,
        0.000000000000000000000001,
        0.0000000000000000000000001,
        0.00000000000000000000000001,
        0.000000000000000000000000001,
        0.0000000000000000000000000001,
        0.00000000000000000000000000001,
        0.000000000000000000000000000001,
        0.0000000000000000000000000000001,
        0.00000000000000000000000000000001,
        0.000000000000000000000000000000001,
        0.0000000000000000000000000000000001,
        0.00000000000000000000000000000000001,
    };
    return floats[0 .. maxExponentToTry(F) + 1];
}

test "powers of ten" {
    try std.testing.expectEqual(powersOfTen(f16).len, inversePowersOfTen(f16).len);
    inline for (benchmarks.FloatTypes) |F| {
        for (powersOfTen(F), inversePowersOfTen(F)) |p, ip| {
            try std.testing.expectEqual(@as(F, 1.0), @round(p * ip));
        }
    }
}

pub fn maxExponentToTry(comptime F: type) comptime_int {
    const significandPrecision = std.math.floatFractionalBits(F) + 1;
    const maxRepresentibleInteger = std.math.maxInt(std.meta.Int(.unsigned, significandPrecision));
    return std.math.log10_int(@as(u128, maxRepresentibleInteger)) + 1;
}

test "max exponent" {
    try std.testing.expectEqual(4, maxExponentToTry(f16));
    try std.testing.expectEqual(8, maxExponentToTry(f32));
    try std.testing.expectEqual(16, maxExponentToTry(f64));
    try std.testing.expectEqual(20, maxExponentToTry(f80));
    try std.testing.expectEqual(35, maxExponentToTry(f128));
}

pub fn toIndexArray(comptime T: type, comptime alignment: u16, bitset: std.bit_set.DynamicBitSet, allocator: Allocator) ![]align(alignment) const T {
    var indices = try allocator.alignedAlloc(T, alignment, bitset.count());
    errdefer allocator.free(indices);
    var pos_iter = bitset.iterator(.{});
    var i: usize = 0;
    while (pos_iter.next()) |pos| {
        indices[i] = pos;
        i += 1;
    }
    return indices;
}

pub const CompressError = error{
    TooManySamplesFromSlice,
};

pub fn sample(comptime T: type, gpa: std.mem.Allocator, vec: []const T) ![]const T {
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
        if (sampleSliceLen > partSlice.stop - partSlice.start) {
            return CompressError.TooManySamplesFromSlice;
        }
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
