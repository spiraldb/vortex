const std = @import("std");
const Allocator = std.mem.Allocator;
const codecmath = @import("../codecmath.zig");
const sampling = @import("../sampling.zig");
const fastlanes = @import("fastlanes.zig");
const Alignment = fastlanes.Alignment;
const abi = @import("abi");
const CodecError = abi.CodecError;
const patch = @import("../patch.zig");

// for encoding, we multiply a given element 'n' by 10^e and 10^(-f)
// for decoding, we do the reverse: n * 10^f * 10^(-e)
pub const AlpExponents = abi.AlpExponents;

// based on https://ir.cwi.nl/pub/33334/33334.pdf
pub fn AdaptiveLosslessFloatingPoint(comptime F: type) type {
    codecmath.comptimeCheckFloat(F);
    const i_F10 = comptime codecmath.inversePowersOfTen(F);
    const F10 = comptime codecmath.powersOfTen(F);
    const FMask = comptime std.meta.Int(.unsigned, @bitSizeOf(F));

    return struct {
        pub usingnamespace patch.CopyPatchesMixin;

        pub const EncInt = codecmath.coveringIntTypePowerOfTwo(F);
        pub const ALPEncoded = struct {
            const Self = @This();

            allocator: Allocator,
            encodedValues: []align(Alignment) const EncInt,
            exponents: AlpExponents,
            exceptionPositions: std.bit_set.DynamicBitSetUnmanaged,
            numExceptions: usize,

            pub fn exceptionCount(self: *const Self) usize {
                return self.numExceptions;
            }

            pub fn deinit(self: *Self) void {
                self.allocator.free(self.encodedValues);
                self.exceptionPositions.deinit(self.allocator);
            }
        };

        pub fn valuesBufferSizeInBytes(len: usize) usize {
            return @sizeOf(EncInt) * len;
        }

        pub inline fn encodeSingle(val: F, exponents: AlpExponents) EncInt {
            const rounded: F = codecmath.fastFloatRound(F, val * F10[exponents.e] * i_F10[exponents.f]);
            const inBounds: u1 = @intFromBool(rounded < codecmath.coveringIntMax(F)) & @intFromBool(rounded > codecmath.coveringIntMin(F));
            // mask is all 1s if in bounds, all zeros otherwise
            const mask: FMask = @as(FMask, @intCast(~inBounds)) -% 1;
            const masked: F = @bitCast(@as(FMask, @bitCast(rounded)) & mask);
            return @intFromFloat(masked);
        }

        pub inline fn decodeSingle(enc: EncInt, exponents: AlpExponents) F {
            return @as(F, @floatFromInt(enc)) * F10[exponents.f] * i_F10[exponents.e];
        }

        pub fn encode(gpa: Allocator, elems: []const F) CodecError!ALPEncoded {
            const exponents = try sampleFindExponents(elems);
            const encoded = try gpa.alignedAlloc(EncInt, Alignment, elems.len);
            errdefer gpa.free(encoded);

            var excPositionsBitSet = try std.bit_set.DynamicBitSetUnmanaged.initEmpty(gpa, elems.len);
            errdefer excPositionsBitSet.deinit(gpa);
            var excPositions = patch.toPackedSlice(excPositionsBitSet);

            const numExceptions = try encodeRaw(elems, exponents, encoded, &excPositions);
            std.debug.assert(numExceptions == excPositionsBitSet.count());
            return ALPEncoded{
                .allocator = gpa,
                .encodedValues = encoded,
                .exponents = exponents,
                .exceptionPositions = excPositionsBitSet,
                .numExceptions = numExceptions,
            };
        }

        pub fn encodeRaw(
            elems: []const F,
            exponents: AlpExponents,
            encoded: []align(Alignment) EncInt,
            excPositions: *std.PackedIntSlice(u1),
        ) CodecError!usize {
            if (encoded.len < elems.len or excPositions.len < elems.len) {
                std.debug.print("ALP.encodeRaw: encoded.len = {}, excPositions.len = {}, elems.len = {}\n", .{ encoded.len, excPositions.len, elems.len });
                return CodecError.OutputBufferTooSmall;
            }
            if (exponents.e < exponents.f or exponents.e > codecmath.maxExponentToTry(F)) {
                std.debug.print("ALP.encodeRaw: exponents.e = {}, exponents.f = {}, maxExponentToTry = {}\n", .{ exponents.e, exponents.f, codecmath.maxExponentToTry(F) });
                return CodecError.InvalidInput;
            }

            var numExceptions: u32 = 0;
            for (elems, 0..) |n, i| {
                encoded[i] = encodeSingle(n, exponents);
                const decoded: F = decodeSingle(encoded[i], exponents);
                const neq = @intFromBool(decoded != elems[i]);
                numExceptions += neq;
                excPositions.set(i, neq);
            }

            return numExceptions;
        }

        pub fn sampleFindExponents(vec: []const F) CodecError!AlpExponents {
            const bufSize = comptime sampling.defaultSampleBufferSize(F);
            var buf: [bufSize]u8 = undefined;
            var fba = std.heap.FixedBufferAllocator.init(&buf);
            const ally = fba.allocator();

            const sample: []const F = try sampling.defaultSample(F, ally, vec);
            defer ally.free(sample);
            return findExponents(sample);
        }

        pub fn findExponents(vec: []const F) CodecError!AlpExponents {
            var bestE: usize = 0;
            var bestF: usize = 0;
            var bestSize: usize = std.math.maxInt(usize);

            // TODO(wmanning): idea, start with highest e, then find the best f
            // after that, try e's in descending order, with a gap no larger than the original e - f
            for (0..codecmath.maxExponentToTry(F) + 1) |e| {
                for (0..e) |f| {
                    const size = estimateSizeWithExponents(vec, AlpExponents{
                        .e = @intCast(e),
                        .f = @intCast(f),
                    });

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

            //std.debug.print("BEST EXPONENTS: type = {}, e = {}, f = {}\n", .{ F, bestE, bestF });
            return .{ .e = @intCast(bestE), .f = @intCast(bestF) };
        }

        fn estimateSizeWithExponents(sampleSlice: []const F, exponents: AlpExponents) usize {
            const EncIntBitWidth = comptime @bitSizeOf(EncInt);
            const zz = @import("zigzag.zig").ZigZag(EncInt);
            const maxEncInt: zz.Unsigned = std.math.maxInt(zz.Unsigned);

            var numExceptions: usize = 0;
            var bitWidthFreq: [EncIntBitWidth + 1]usize = [_]usize{0} ** (EncIntBitWidth + 1);
            var minEncoded: zz.Unsigned = maxEncInt;
            for (sampleSlice) |val| {
                const encoded = encodeSingle(val, exponents);
                const decoded = decodeSingle(encoded, exponents);
                const eq: u1 = @intFromBool(decoded == val);
                const neq: u1 = ~eq;

                numExceptions += neq;
                // if encoding is a success, count number of leading zeroes to estimate bitpacking efficacy
                // see comment below for why we zigzag before counting clz
                // NB: if encoding failed, encoded/zzEncoded are both 0.
                const zzEncoded = zz.encodeSingle(encoded);
                const encodedClz = @clz(zzEncoded);

                // element count of elements of bit width i
                bitWidthFreq[EncIntBitWidth - encodedClz] += eq;
                const encodedOrMaxInt = zzEncoded * @as(zz.Unsigned, eq) + maxEncInt * @as(zz.Unsigned, neq);
                minEncoded = @min(minEncoded, encodedOrMaxInt);
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
                const sizeInBits = packedWidth * sampleSlice.len + fforExceptionCounts[packedWidth] * EncIntBitWidth;
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

        pub fn decode(allocator: Allocator, input: ALPEncoded) CodecError![]F {
            const decoded: []F = try allocator.alloc(F, input.encodedValues.len);
            errdefer allocator.free(decoded);
            try decodeRaw(input.encodedValues, input.exponents, decoded);
            return decoded;
        }

        pub fn decodeRaw(
            input: []const EncInt,
            exponents: AlpExponents,
            out: []F,
        ) CodecError!void {
            if (out.len < input.len) {
                std.debug.print("ALP.decodeRaw: out.len = {}, input.len = {}\n", .{ out.len, input.len });
                return CodecError.OutputBufferTooSmall;
            }

            for (input, out) |enc, *o| {
                o.* = decodeSingle(enc, exponents);
            }
        }
    };
}

const benchmarks = @import("../benchmarks.zig");
test "alp round trip" {
    try benchmarks.testFloatsRoundTrip(AdaptiveLosslessFloatingPoint);
}

test "alp benchmark" {
    try benchmarks.generatedDecimals(AdaptiveLosslessFloatingPoint, "ALP");
}

test "encoded int size & alignment match input float size & alignment" {
    const types = [_]type{ f32, f64 };
    inline for (types) |T| {
        const codec = AdaptiveLosslessFloatingPoint(T);
        const bufSize = codec.valuesBufferSizeInBytes(10);
        try std.testing.expect(bufSize == @sizeOf([10]T));
        try std.testing.expect(bufSize == 10 * @sizeOf(codec.EncInt));
        try std.testing.expect(@alignOf(codec.EncInt) == @alignOf(T));
        try std.testing.expect(@sizeOf(codec.EncInt) == @sizeOf(T));
    }
}
