const std = @import("std");
const Allocator = std.mem.Allocator;
const codecmath = @import("../codecmath.zig");
const simd_math = @import("../simd_math.zig");
const fastlanes = @import("fastlanes.zig");
const Alignment = fastlanes.Alignment;
const FLVec = fastlanes.FLVec;
const abi = @import("abi");
const CodecError = abi.CodecError;
const patch = @import("../patch.zig");
const ScatterPatches = patch.ScatterPatchesMixin;

const Codec = enum {
    packed_ints, // bitpacking only
    ffor, // fused frame of reference
};

pub fn PackedInts(comptime T: u8, comptime W: u8) type {
    return PackedIntsImpl(.unsigned, T, W, .packed_ints);
}

pub fn FFOR(comptime V: type, comptime W: u8) type {
    codecmath.comptimeCheckInt(V);
    const vti = @typeInfo(V).Int;
    return PackedIntsImpl(vti.signedness, vti.bits, W, .ffor);
}

pub fn UnsignedFFOR(comptime T: u8, comptime W: u8) type {
    return PackedIntsImpl(.unsigned, T, W, .ffor);
}

pub fn SignedFFOR(comptime T: u8, comptime W: u8) type {
    return PackedIntsImpl(.signed, T, W, .ffor);
}

fn EncodedImpl(comptime V: type) type {
    return struct {
        const Self = @This();

        allocator: Allocator,
        bytes: []align(Alignment) const u8,
        elems_len: usize,
        min_val: ?V,
        num_exceptions: usize = 0,

        exceptions: ?[]align(Alignment) const V = null,
        exception_indices: ?std.bit_set.DynamicBitSetUnmanaged = null,

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.bytes);
            if (self.exceptions) |ex| {
                self.allocator.free(ex);
            }
            if (self.exception_indices) |_| {
                self.exception_indices.?.deinit(self.allocator);
            }
        }
    };
}

/// Pack and unpack integers of width T into packed width W.
fn PackedIntsImpl(comptime signedness: std.builtin.Signedness, comptime T: u8, comptime W: u8, comptime codec: Codec) type {
    if (signedness == .signed and codec != .ffor) {
        @compileError("Cannot use signed integers with codec " ++ codec);
    }
    if (W >= T) {
        @compileError("W must be smaller than T for bitpacking");
    }
    return struct {
        pub const V = std.meta.Int(signedness, T);
        pub const UV = std.meta.Int(.unsigned, T);
        pub const P = @Type(.{ .Int = .{ .signedness = .unsigned, .bits = W } });
        pub const Encoded = EncodedImpl(V);

        const vlen = fastlanes.vecLen(V);
        const BitVec = @Vector(vlen, u1);
        const TminusW = @as(@Vector(vlen, @TypeOf(T)), @splat(T - W));
        const elemsPerTranche = fastlanes.FLWidth;
        const bytesPerTranche = @as(usize, W) * @sizeOf(FLVec(V));

        /// The number of bytes required to encode the given elements.
        pub fn encodedSizeInBytes(length: usize) usize {
            const ntranches = length / elemsPerTranche;
            const remainder = length % elemsPerTranche;
            const remainderBytes = ((W * remainder) + 7) / 8;
            return (ntranches * bytesPerTranche) + remainderBytes;
        }

        pub fn encode(elems: []const V, allocator: Allocator) CodecError!Encoded {
            const out = try allocator.alignedAlloc(u8, Alignment, encodedSizeInBytes(elems.len));
            errdefer allocator.free(out);

            // when calling encodeRaw over FFI, we'll generally have this as a precomputed stat
            const minVal: ?V = if (codec == .ffor) simd_math.min(V, elems) else null;

            // first pass
            const numExceptions = try encodeRaw(elems, minVal, out);
            var encoded = Encoded{
                .allocator = allocator,
                .bytes = out,
                .elems_len = elems.len,
                .min_val = minVal,
                .num_exceptions = numExceptions,
            };
            // errdefer encoded.deinit();
            // we don't want this^, since we handle partial deinit via individual errdefers in this function

            // second pass to gather exceptions, if necessary
            if (encoded.num_exceptions > 0) {
                var excPositionsBitset = try std.bit_set.DynamicBitSetUnmanaged.initEmpty(allocator, elems.len);
                errdefer excPositionsBitset.deinit(allocator);
                var excPositions = patch.toPackedSlice(excPositionsBitset); // a view on the bitset

                const exceptions = try allocator.alignedAlloc(V, Alignment, encoded.num_exceptions);
                errdefer allocator.free(exceptions);

                try collectExceptions(
                    elems,
                    encoded.min_val,
                    numExceptions,
                    exceptions,
                    &excPositions,
                );
                encoded.exception_indices = excPositionsBitset;
                encoded.exceptions = exceptions;
            }
            return encoded;
        }

        pub fn encodeRaw(
            elems: []const V,
            minVal: ?V,
            encoded: []align(Alignment) u8,
        ) CodecError!usize {
            if (encoded.len < encodedSizeInBytes(elems.len)) {
                std.debug.print("PackedIntsImpl.encodeRaw: out.len = {}, elems.len = {}\n", .{ encoded.len, elems.len });
                return CodecError.OutputBufferTooSmall;
            }
            if (comptime codec == .ffor) {
                if (minVal == null and elems.len > 0) {
                    std.debug.print("PackedIntsImpl.encodeRaw: codec == .ffor and minVal == null, elems.len = {}\n", .{elems.len});
                    return CodecError.InvalidInput;
                }
            } else {
                if (minVal != null) {
                    std.debug.print("PackedIntsImpl.encodeRaw: codec == .packed_ints and minVal != null, elems.len = {}\n", .{elems.len});
                    return CodecError.InvalidInput;
                }
            }

            // Encode as many tranches as we can, and then fallback to scalar?
            const ntranches = elems.len / elemsPerTranche;

            const in: []const FLVec(V) = @alignCast(std.mem.bytesAsSlice(FLVec(V), std.mem.sliceAsBytes(elems[0 .. ntranches * elemsPerTranche])));
            var out: []FLVec(UV) = @alignCast(std.mem.bytesAsSlice(FLVec(UV), encoded[0 .. ntranches * bytesPerTranche]));
            var num_exceptions: usize = 0;

            for (0..ntranches) |i| {
                num_exceptions += encodeTranche(in[T * i ..][0..T], minVal, out[W * i ..][0..W]);
            }

            // Is there a nicer fallback to have?
            const remaining = elems[ntranches * elemsPerTranche ..];
            if (remaining.len > 0) {
                num_exceptions += countRemainingExceptions(remaining, minVal);
                var packedInts = std.PackedIntSlice(P){
                    .bytes = encoded[ntranches * bytesPerTranche ..],
                    .bit_offset = 0,
                    .len = remaining.len,
                };
                for (remaining, 0..) |e, i| {
                    packedInts.set(i, @truncate(maybe_frame_encode(UV, e, minVal)));
                }
            }

            return num_exceptions;
        }

        /// A single tranche takes T input vectors and produces W output vectors.
        /// Returns the number of elements unable to be encoded in W bits.
        fn encodeTranche(in: *const [T]FLVec(V), minVal: ?V, out: *[W]FLVec(UV)) usize {
            comptime var bitIdx = 0;
            comptime var outIdx = 0;
            const ones = @as(@Vector(vlen, u1), @splat(1));
            const zeroes = @as(@Vector(vlen, u1), @splat(0));

            if (comptime codec == .ffor) {
                std.debug.assert(minVal != null);
            } else {
                std.debug.assert(minVal == null);
            }
            const minVec: ?FLVec(V) = if (codec == .ffor) @as(FLVec(V), @splat(minVal.?)) else null;

            var tmp: FLVec(UV) = undefined;
            var num_exceptions: @Vector(vlen, usize) = @splat(0);
            inline for (0..T) |t| {
                // Grab the next input vector and mask out the bits of W
                var src = maybe_frame_encode(FLVec(UV), in[t], minVec);
                num_exceptions += @select(u1, @clz(src) < TminusW, ones, zeroes);
                src = src & bitmask(W);

                // Either we reset tmp, or we OR it into tmp.
                // If we didn't assign, we would need to reset to zero which
                // adds an extra instruction.
                if (comptime bitIdx == 0) {
                    tmp = src;
                } else {
                    tmp |= src << @splat(bitIdx);
                }
                bitIdx += W;

                if (comptime bitIdx == T) {
                    // We've exactly filled tmp with packed ints
                    out[outIdx] = tmp;
                    outIdx += 1;
                    bitIdx = 0;
                } else if (comptime bitIdx > T) {
                    // The last value didn't completely fit, so store what
                    // we have and carry forward the remainder to the next
                    // loop using tmp.
                    out[outIdx] = tmp;
                    outIdx += 1;
                    bitIdx -= T;

                    tmp = src >> @splat(W - bitIdx);
                }
            }
            return @reduce(.Add, num_exceptions);
        }

        pub usingnamespace ScatterPatches;

        pub fn decode(encoded: Encoded, allocator: Allocator) CodecError![]align(Alignment) V {
            const decoded = try allocator.alignedAlloc(V, Alignment, encoded.elems_len);
            errdefer allocator.free(decoded);

            try decodeRaw(encoded.bytes, encoded.elems_len, encoded.min_val, decoded);

            // check if we have exceptions/patches to overlay
            const num_exceptions: usize = if (encoded.exceptions) |ex| ex.len else 0;
            if (num_exceptions == 0) {
                return decoded;
            }

            // we have exceptions! patch them
            if (encoded.exception_indices) |idx| {
                if (idx.count() != num_exceptions) {
                    std.debug.print("PackedIntsImpl.decode: idx.capacity: {}, idx.count: {}, num_exceptions: {}\n", .{
                        idx.capacity(),
                        idx.count(),
                        num_exceptions,
                    });
                    return CodecError.InvalidInput;
                }

                if (encoded.exceptions == null) {
                    std.debug.print("PackedIntsImpl.decode: encoded.exceptions == null, num_exceptions: {}\n", .{num_exceptions});
                    return CodecError.InvalidInput;
                } else if (encoded.exceptions.?.len != num_exceptions) {
                    std.debug.print("PackedIntsImpl.decode: encoded.exceptions.len: {}, num_exceptions: {}\n", .{
                        encoded.exceptions.?.len,
                        num_exceptions,
                    });
                    return CodecError.InvalidInput;
                }
            } else {
                std.debug.print("PackedIntsImpl.decode: encoded.exception_indices == null, num_exceptions: {}\n", .{num_exceptions});
                return CodecError.InvalidInput;
            }

            try ScatterPatches.patch(V, encoded.exception_indices.?, encoded.exceptions.?, decoded);
            return decoded;
        }

        pub fn decodeRaw(
            encoded_bytes: []align(Alignment) const u8,
            elems_len: usize,
            minVal: ?V,
            decoded: []align(Alignment) V,
        ) CodecError!void {
            const ntranches = elems_len / elemsPerTranche;

            if (decoded.len < elems_len) {
                std.debug.print("PackedIntsImpl.decodeRaw: out.len = {}, input.len = {}\n", .{ decoded.len, elems_len });
                return CodecError.OutputBufferTooSmall;
            }
            if (comptime codec == .ffor) {
                if (minVal == null and elems_len > 0) {
                    std.debug.print("PackedIntsImpl.decodeRaw: codec == .ffor and minVal == null, elems_len = {}\n", .{elems_len});
                    return CodecError.InvalidInput;
                }
            } else {
                if (minVal != null) {
                    std.debug.print("PackedIntsImpl.decodeRaw: codec == .packed_ints and minVal != null, elems_len = {}\n", .{elems_len});
                    return CodecError.InvalidInput;
                }
            }

            // vectorized decoding for most of the data (except very small arrays)
            const in: []const FLVec(UV) = @alignCast(std.mem.bytesAsSlice(FLVec(UV), encoded_bytes[0 .. ntranches * bytesPerTranche]));
            var out: []FLVec(V) = @alignCast(std.mem.bytesAsSlice(FLVec(V), std.mem.sliceAsBytes(decoded[0 .. ntranches * elemsPerTranche])));
            for (0..ntranches) |i| {
                decodeTranche(in[W * i ..][0..W], minVal, out[T * i ..][0..T]);
            }

            // fallback logic to unpack the tail
            const remaining = decoded[ntranches * elemsPerTranche ..];
            const packedInts = std.PackedIntSlice(P){
                .bytes = @constCast(encoded_bytes[ntranches * bytesPerTranche ..]),
                .bit_offset = 0,
                .len = remaining.len,
            };
            for (0..remaining.len) |i| {
                const val: UV = @intCast(packedInts.get(i));
                remaining[i] = maybe_frame_decode(V, val, minVal);
            }
        }

        fn decodeTranche(in: *const [W]FLVec(UV), minVal: ?V, out: *[T]FLVec(V)) void {
            if (comptime codec == .ffor) {
                std.debug.assert(minVal != null);
            } else {
                std.debug.assert(minVal == null);
            }
            const minVec: ?FLVec(V) = if (comptime codec == .ffor) @as(FLVec(V), @splat(minVal.?)) else null;

            // Construct a bit-mask to extract integers of width W
            var src = in[0];
            comptime var inIdx = 1;
            comptime var bitIdx: usize = 0;
            inline for (0..T) |t| {
                // Take as many bits as we can without overflowing T
                const bits = @min(T - bitIdx, W);

                var tmp = and_rshift(src, bitIdx, bitmask(bits));
                bitIdx += bits;

                // IMPORTANT: since codec and bitIdx are comptime variables, all of the branches in this loop
                // are evaluated at comptime (not on the runtime hot path)
                if (comptime bitIdx < T) {
                    // We have all the bits for the output t
                    out[t] = maybe_frame_decode(FLVec(V), tmp, minVec);
                } else {
                    // Otherwise, we may need to load some bits from the next input
                    if (comptime inIdx == in.len) {
                        // No more input
                        out[t] = maybe_frame_decode(FLVec(V), tmp, minVec);
                        return;
                    }

                    src = in[inIdx];
                    inIdx += 1;

                    // TODO(ngates): check that this gets optimized away if W == bits
                    tmp |= and_lshift(src, bits, bitmask(W - bits));
                    out[t] = maybe_frame_decode(FLVec(V), tmp, minVec);
                    bitIdx = W - bits;
                }
            }
        }

        // not vectorized since this is only used on the tail
        fn countRemainingExceptions(elems: []const V, minVal: ?V) usize {
            var count: usize = 0;
            for (elems) |elem| {
                const value = maybe_frame_encode(UV, elem, minVal);
                count += @intFromBool(@clz(value) < T - W);
            }
            return count;
        }

        pub fn collectExceptions(elems: []const V, minVal: ?V, numExceptions: usize, exceptions: []V, excPositions: *std.PackedIntSlice(u1)) CodecError!void {
            if (comptime codec == .ffor) {
                if (minVal == null and elems.len > 0) {
                    std.debug.print("PackedIntsImpl.collectExceptions: codec == .ffor and minVal == null, elems.len = {}\n", .{elems.len});
                    return CodecError.InvalidInput;
                }
            } else {
                if (minVal != null) {
                    std.debug.print("PackedIntsImpl.collectExceptions: codec == .packed_ints and minVal != null, elems.len = {}\n", .{elems.len});
                    return CodecError.InvalidInput;
                }
            }
            if (exceptions.len < numExceptions or excPositions.len < elems.len) {
                std.debug.print(
                    "PackedIntsImpl.collectExceptions: exceptions.len = {}, numExceptions = {}, excPositions.len = {}, elems.len = {}\n",
                    .{ exceptions.len, numExceptions, excPositions.len, elems.len },
                );
                return CodecError.OutputBufferTooSmall;
            }

            const ntranches = elems.len / elemsPerTranche;
            var excCount: usize = 0;
            if (ntranches > 0) {
                const vecs: []const FLVec(V) = @alignCast(std.mem.bytesAsSlice(FLVec(V), std.mem.sliceAsBytes(elems[0 .. ntranches * elemsPerTranche])));
                const minVec: ?FLVec(V) = if (codec == .ffor) @as(FLVec(V), @splat(minVal.?)) else null;

                var offset: usize = 0;
                for (0..ntranches) |tranche_idx| {
                    inline for (vecs[T * tranche_idx ..][0..T]) |vec| {
                        const is_exception_vec = @clz(maybe_frame_encode(FLVec(UV), vec, minVec)) < TminusW;
                        inline for (0..vlen) |i| {
                            excPositions.set(offset + i, @intFromBool(is_exception_vec[i]));
                            exceptions[excCount] = vec[i];
                            excCount += @intFromBool(is_exception_vec[i]);
                        }
                        offset += vlen;
                    }
                }
            }

            const offset = ntranches * elemsPerTranche;
            const remaining = elems[offset..];
            for (remaining, offset..) |elem, idx| {
                const value = maybe_frame_encode(UV, elem, minVal);
                const is_exception = @clz(value) < T - W;
                excPositions.set(idx, @intFromBool(is_exception));
                exceptions[excCount] = elem;
                excCount += @intFromBool(is_exception);
            }
        }

        /// Subtract min_val if FFOR, otherwise no-op
        inline fn maybe_frame_encode(comptime R: type, val: anytype, min_val: ?@TypeOf(val)) R {
            // the branch is evaluated at comptime
            if (codec == .ffor) {
                return @bitCast(val -% min_val.?);
            } else {
                return val;
            }
        }

        /// Add min_val if FFOR, otherwise no-op
        inline fn maybe_frame_decode(comptime R: type, val: anytype, min_val: ?R) R {
            if (codec == .ffor) {
                return @bitCast(val +% @as(@TypeOf(val), @bitCast(min_val.?)));
            } else {
                return val;
            }
        }

        inline fn bitmask(comptime bits: comptime_int) FLVec(UV) {
            return @splat((1 << bits) - 1);
        }

        // forall T−bit lanes i in REG return (i & MASK) << N
        inline fn and_lshift(vec: FLVec(UV), n: anytype, mask: FLVec(UV)) FLVec(UV) {
            // TODO(ngates): can we make this more efficient?
            const nVec: FLVec(UV) = @splat(n);
            return (vec & mask) << @intCast(nVec);
        }

        // forall T−bit lanes i in REG return (i & (MASK << N)) >> N
        inline fn and_rshift(vec: FLVec(UV), n: anytype, mask: FLVec(UV)) FLVec(UV) {
            const nVec: FLVec(V) = @splat(n);
            return (vec & (mask << nVec)) >> @intCast(nVec);
        }
    };
}

test "fastlanes packedints encodedSize" {
    // Pack 8 bit ints into 2 bit ints.
    try std.testing.expectEqual(@as(usize, 256), PackedInts(8, 2).encodedSizeInBytes(1024));

    // Pack 8 bit ints into 6 bit ints
    try std.testing.expectEqual(@as(usize, 768), PackedInts(8, 6).encodedSizeInBytes(1024));
}

test "packed ints round trips" {
    try testPackedInts(.unsigned, .packed_ints);
}

test "signed ffor round trips" {
    try testPackedInts(.signed, .ffor);
}

test "unsigned ffor round trips" {
    try testPackedInts(.unsigned, .ffor);
}

fn testPackedInts(comptime signedness: std.builtin.Signedness, comptime codec: Codec) !void {
    const ally = std.testing.allocator;
    const Ns = [_]usize{ 0, 6, 100, fastlanes.FLWidth, 10_000 };
    const Ts = [_]u8{ 8, 64 };

    inline for (Ts) |T| {
        const W = comptime try std.math.divCeil(u8, T, 2);
        const ints = PackedIntsImpl(signedness, T, W, codec);

        for (Ns) |N| {
            // Setup N values cycling through 0..T
            var values = try ally.alignedAlloc(ints.V, 128, N);
            defer ally.free(values);

            if (N >= 1) {
                values[0] = std.math.minInt(ints.V);
                values[N - 1] = std.math.maxInt(ints.V);
                const step: i128 = @intCast(try std.math.divCeil(u64, std.math.maxInt(ints.UV), N));
                const base: i128 = @intCast(std.math.minInt(ints.V));
                const maxVal: i128 = @intCast(std.math.maxInt(ints.V));
                for (1..N - 1) |i| {
                    // Cycle through all valid input values
                    values[i] = @intCast(@mod(base +% (step *% i), maxVal));
                }
            }

            var encoded = try ints.encode(values, ally);
            defer encoded.deinit();
            const result = try ints.decode(encoded, ally);
            defer ally.free(result);

            try std.testing.expectEqualSlices(ints.V, values, result);
        }
    }
}

test "simple packed int benchmark" {
    try bitpackingIntegers("Packed Ints", PackedInts, 32, 1, 10_000_000, 1);
}

test "simple signed ffor benchmark" {
    try bitpackingIntegers("Signed FFOR", SignedFFOR, 32, 1, 10_000_000, -100);
}

test "simple unsigned ffor benchmark" {
    try bitpackingIntegers("Unsigned FFOR", UnsignedFFOR, 32, 1, 10_000_000, 100);
}

fn bitpackingIntegers(comptime name: []const u8, comptime codec_fn: fn (comptime T: u8, comptime W: u8) type, comptime T: u8, comptime W: u8, N: usize, comptime value: comptime_int) !void {
    const ally = std.testing.allocator;
    const ints = codec_fn(T, W);

    // Setup N values. Can be constant, has no impact on performance.
    const values = try ally.alignedAlloc(ints.V, 128, N);
    defer ally.free(values);
    @memset(values, value);

    // Encode the ints
    var timer = try std.time.Timer.start();
    var encoded = try ints.encode(values, ally);
    defer encoded.deinit();
    const encode_ns = timer.lap();
    std.debug.print("FL {s} ENCODE u{} -> u{}: {} ints in {}ms, {} million ints per second\n", .{
        name,
        T,
        W,
        N,
        encode_ns / 1_000_000,
        1000 * N / encode_ns,
    });

    // no patches in the benchmark
    try std.testing.expect(encoded.exception_indices == null and encoded.exceptions == null);

    timer.reset();
    const result = try ints.decode(encoded, ally);
    defer ally.free(result);
    const decode_ns = timer.lap();
    std.debug.print("FL {s} DECODE u{} -> u{}: {} ints in {}ms, {} million ints per second\n", .{
        name,
        T,
        W,
        N,
        decode_ns / 1_000_000,
        1000 * N / decode_ns,
    });

    try std.testing.expectEqualSlices(ints.V, values, result);
}
