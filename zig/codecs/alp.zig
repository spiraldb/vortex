const std = @import("std");
const Allocator = std.mem.Allocator;
const codecmath = @import("codecmath.zig");
const fastlanes = @import("fastlanes.zig");
const Alignment = fastlanes.Alignment;

// for encoding, we multiply a given element 'n' by 10^e and 10^(-f)
// for decoding, we do the reverse: n * 10^f * 10^(-e)
pub const Exponents = struct {
    e: u8,
    f: u8,
};

// based on https://ir.cwi.nl/pub/33334/33334.pdf
pub fn AdaptiveLosslessFloatingPoint(comptime F: type) type {
    codecmath.comptimeCheckFloat(F);
    const i_F10 = comptime codecmath.inversePowersOfTen(F);
    const F10 = comptime codecmath.powersOfTen(F);
    const EncInt = comptime std.meta.Int(.signed, codecmath.coveringIntBits(F));
    const FMask = comptime std.meta.Int(.unsigned, @bitSizeOf(F));

    return struct {
        pub const ALPEncoded = struct {
            const Self = @This();

            allocator: Allocator,
            encodedValues: []align(Alignment) const EncInt,
            exponents: Exponents,
            exceptions: []const F,
            exceptionPositions: std.bit_set.DynamicBitSet,

            pub fn exceptionCount(self: Self) usize {
                return self.exceptions.len;
            }

            pub fn deinit(self: *Self) void {
                self.allocator.free(self.encodedValues);
                self.allocator.free(self.exceptions);
                self.exceptionPositions.deinit();
            }
        };

        inline fn encodeSingle(val: F, exponents: Exponents) EncInt {
            const rounded: F = codecmath.fastFloatRound(F, val * F10[exponents.e] * i_F10[exponents.f]);
            const inBounds: u1 = @intFromBool(rounded < codecmath.coveringIntMax(F)) & @intFromBool(rounded > codecmath.coveringIntMin(F));
            // mask is all 1s if in bounds, all zeros otherwise
            const mask: FMask = @as(FMask, @intCast(~inBounds)) -% 1;
            const masked: F = @bitCast(@as(FMask, @bitCast(rounded)) & mask);
            return @intFromFloat(masked);
        }

        inline fn decodeSingle(enc: EncInt, exponents: Exponents) F {
            return @as(F, @floatFromInt(enc)) * F10[exponents.f] * i_F10[exponents.e];
        }

        pub fn encode(allocator: Allocator, elems: []const F) !ALPEncoded {
            var encoded = try allocator.alignedAlloc(EncInt, Alignment, elems.len);
            errdefer allocator.free(encoded);
            var decoded = try allocator.alloc(F, elems.len);
            defer allocator.free(decoded);

            const exponents = try findExponents(allocator, elems);
            var numExceptions: u32 = 0;
            var exceptionPositions = try std.bit_set.DynamicBitSet.initEmpty(allocator, elems.len);
            errdefer exceptionPositions.deinit();
            for (elems, 0..) |n, i| {
                encoded[i] = encodeSingle(n, exponents);
                decoded[i] = decodeSingle(encoded[i], exponents);
                const neq = decoded[i] != elems[i];
                numExceptions += @intFromBool(neq);
                exceptionPositions.setValue(i, neq);
            }

            var exceptions = try allocator.alloc(F, numExceptions);
            errdefer allocator.free(exceptions);

            var positionIterator = exceptionPositions.iterator(.{});
            var i: usize = 0;
            while (positionIterator.next()) |pos| : (i += 1) {
                exceptions[i] = elems[pos];
            }
            return .{
                .allocator = allocator,
                .encodedValues = encoded,
                .exponents = exponents,
                .exceptions = exceptions,
                .exceptionPositions = exceptionPositions,
            };
        }

        pub fn findExponents(gpa: std.mem.Allocator, vec: []const F) !Exponents {
            var sampleIter = try codecmath.defaultSample(F, gpa, vec);
            defer sampleIter.deinit();
            var bestE: usize = 0;
            var bestF: usize = 0;
            var bestSize: usize = std.math.maxInt(usize);

            // TODO(wmanning): idea, start with highest e, then find the best f
            // after that, try e's in descending order, with a gap no larger than the original e - f
            for (0..codecmath.maxExponentToTry(F) + 1) |e| {
                for (0..e) |f| {
                    const size = estimateSizeWithExponents(&sampleIter, Exponents{
                        .e = @intCast(e),
                        .f = @intCast(f),
                    });
                    sampleIter.reset();

                    if (size < bestSize) {
                        bestSize = size;
                        bestE = e;
                        bestF = f;
                    } else if (size == bestSize and e - f < bestE - bestF) {
                        bestE = e;
                        bestF = f;
                    }
                }
            }

            std.debug.print("BEST EXPONENTS: type = {}, e = {}, f = {}\n", .{ F, bestE, bestF });
            return .{ .e = @intCast(bestE), .f = @intCast(bestF) };
        }

        fn estimateSizeWithExponents(sliceIter: *codecmath.SampleSliceIterator(F), exponents: Exponents) usize {
            const EncIntBitWidth = comptime @bitSizeOf(EncInt);
            const zz = @import("zigzag.zig").ZigZag(EncInt);
            const maxEncInt: zz.Unsigned = std.math.maxInt(zz.Unsigned);

            var numExceptions: usize = 0;
            var bitWidthFreq: [EncIntBitWidth + 1]usize = [_]usize{0} ** (EncIntBitWidth + 1);
            var minEncoded: zz.Unsigned = maxEncInt;
            while (sliceIter.next()) |sampleSlice| {
                for (sampleSlice) |val| {
                    const encoded = encodeSingle(val, exponents);
                    const decoded = decodeSingle(encoded, exponents);
                    const eq: u1 = @intFromBool(decoded == val);
                    const neq: u1 = ~eq;

                    numExceptions += neq;
                    // if encoding is a success, count number of leading zeroes to estimate bitpacking efficacy
                    // see comment below for why we zigzag before counting clz
                    // NB: if encoding failed, encoded/zzEncoded are both 0.
                    const zzEncoded = zz.encode_single(encoded);
                    const encodedClz = @clz(zzEncoded);

                    // element count of elements of bit width i
                    bitWidthFreq[EncIntBitWidth - encodedClz] += eq;
                    const encodedOrMaxInt = zzEncoded * @as(zz.Unsigned, eq) + maxEncInt * @as(zz.Unsigned, neq);
                    minEncoded = @min(minEncoded, encodedOrMaxInt);
                }
            }

            // We estimate the encoded size assuming that the downstream encodings are zigzag + fastlanes ffor,
            // since that combo is reasonably low variance & robust across different distributions.
            // In particular, our raw encoded ints are likely to have large-but-similar absolute values, but
            // opposite signs (so zigzag helps make them unsigned but of similar absolute values), and then ffor
            // should achieve good bitpacking on the resulting large unsigned values.
            if (minEncoded > 0) {
                const ctzRemoved = std.math.log2_int(zz.Unsigned, minEncoded);
                // Subtraction should remove roughly log2 (floor) bits, which is equivalent to adding that many
                // to @clz for every value (i.e., shifting the bit width histogram to the left)
                for (0..bitWidthFreq.len - ctzRemoved) |i| {
                    bitWidthFreq[i] = bitWidthFreq[i + ctzRemoved];
                }
                for (bitWidthFreq.len - ctzRemoved..bitWidthFreq.len) |i| {
                    bitWidthFreq[i] = 0;
                }
            }

            // transform clz histogram into reverse cumulative sum (i.e., sum of all elements *after*)
            var fforExceptionCounts: [bitWidthFreq.len]usize = [_]usize{0} ** (bitWidthFreq.len);
            var cumsum: usize = 0;
            for (0..fforExceptionCounts.len) |j| {
                // iterate in reverse
                const i = fforExceptionCounts.len - 1 - j;
                fforExceptionCounts[i] = cumsum + bitWidthFreq[i];
                cumsum += bitWidthFreq[i];
            }

            const powersOfTwo: [8]u8 = [_]u8{ 0, 1, 2, 4, 8, 16, 32, 64 };
            var bestPackedSize: usize = std.math.maxInt(usize);
            var bestPackedWidth: u8 = undefined;
            for (powersOfTwo) |packedWidth| {
                if (packedWidth >= fforExceptionCounts.len) {
                    break;
                }
                const sizeInBits = packedWidth * sliceIter.totalNumSamples() + fforExceptionCounts[packedWidth] * EncIntBitWidth;
                if (sizeInBits < bestPackedSize) {
                    bestPackedSize = sizeInBits;
                    bestPackedWidth = packedWidth;
                }
            }

            const size = numExceptions * (@bitSizeOf(F) + @bitSizeOf(usize)) + bestPackedSize;
            // std.debug.print("ALP_SIZE: F = {}, e = {}, f = {}, avgSizeInBits = {d:.1}, numExceptions = {}, bestPackedWidth = {}, minEncoded = {}, fforExceptions = {}\n", .{
            //     F,
            //     exponents.e,
            //     exponents.f,
            //     @as(f64, @floatFromInt(size)) / @as(f64, @floatFromInt(sliceIter.numSamples())),
            //     numExceptions,
            //     bestPackedWidth,
            //     minEncoded,
            //     fforExceptionCounts[bestPackedWidth],
            // });
            return size;
        }

        pub fn decode(allocator: Allocator, input: ALPEncoded) ![]const F {
            var decoded: []F = try allocator.alloc(F, input.encodedValues.len);
            errdefer allocator.free(decoded);

            for (input.encodedValues, 0..) |enc, i| {
                decoded[i] = decodeSingle(enc, input.exponents);
            }

            var pos_iter = input.exceptionPositions.iterator(.{});
            var i: usize = 0;
            while (pos_iter.next()) |pos| : (i += 1) {
                decoded[pos] = input.exceptions[i];
            }

            return decoded;
        }
    };
}

const benchmarks = @import("benchmarks.zig");
test "alp round trip" {
    try benchmarks.testFloatsRoundTrip(AdaptiveLosslessFloatingPoint);
}

test "alp benchmark" {
    try benchmarks.generatedDecimals(AdaptiveLosslessFloatingPoint, "ALP");
}
