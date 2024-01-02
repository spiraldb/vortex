const std = @import("std");
const Allocator = std.mem.Allocator;
const zimd = @import("zimd");
const fastlanes = @import("fastlanes.zig");
const Alignment = fastlanes.Alignment;
const FLVec = fastlanes.FLVec;

const Codec = enum {
    packed_ints, // bitpacking only
    ffor, // fused frame of reference
};

pub fn PackedInts(comptime T: u8, comptime W: u8) type {
    return PackedIntsImpl(.unsigned, T, W, .packed_ints);
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

        allocator: ?Allocator = null,
        bytes: []align(Alignment) const u8,
        elems_len: usize,
        min_val: ?V = null,
        exception_indices: ?std.bit_set.DynamicBitSet = null,
        exceptions: ?[]align(Alignment) const V = null,

        pub fn deinit(self: *Self) void {
            if (self.allocator) |ally| {
                ally.free(self.bytes);
                if (self.exceptions) |ex| {
                    ally.free(ex);
                }
                if (self.exception_indices) |expos| {
                    @constCast(&expos).deinit();
                }
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
        const TminusW = @as(@Vector(vlen, @TypeOf(T)), @splat(T - W));
        const elemsPerTranche = fastlanes.FLWidth;
        const bytesPerTranche = @as(usize, W) * @sizeOf(FLVec(V));

        /// The number of bytes required to encode the given elements.
        fn encodedSize(length: usize) usize {
            const ntranches = length / elemsPerTranche;
            const remainder = length % elemsPerTranche;
            const remainderBytes = ((W * remainder) + 7) / 8;
            return (ntranches * bytesPerTranche) + remainderBytes;
        }

        pub fn encode(elems: []const V, allocator: Allocator) !Encoded {
            // Encode as many tranches as we can, and then fallback to scalar?
            const ntranches = elems.len / elemsPerTranche;

            var encoded = try allocator.alignedAlloc(u8, Alignment, encodedSize(elems.len));
            errdefer allocator.free(encoded);

            const in: []const FLVec(V) = @alignCast(std.mem.bytesAsSlice(FLVec(V), std.mem.sliceAsBytes(elems[0 .. ntranches * elemsPerTranche])));
            var out: []FLVec(UV) = @alignCast(std.mem.bytesAsSlice(FLVec(UV), encoded[0 .. ntranches * bytesPerTranche]));
            var num_exceptions: usize = 0;

            const minVal: ?V = if (codec == .ffor) zimd.math.min(V, elems) else null;
            for (0..ntranches) |i| {
                num_exceptions += encode_tranche(in[T * i ..][0..T], minVal, out[W * i ..][0..W]);
            }

            // Is there a nicer fallback to have?
            const remaining = elems[ntranches * elemsPerTranche ..];
            if (remaining.len > 0) {
                num_exceptions += count_remaining_exceptions(remaining, minVal);
                var packedInts = std.PackedIntSlice(P){
                    .bytes = encoded[ntranches * bytesPerTranche ..],
                    .bit_offset = 0,
                    .len = remaining.len,
                };
                for (remaining, 0..) |e, i| {
                    packedInts.set(i, @truncate(maybe_frame_encode(UV, e, minVal)));
                }
            }

            if (num_exceptions == 0) {
                return Encoded{
                    .allocator = allocator,
                    .bytes = encoded,
                    .elems_len = elems.len,
                    .min_val = minVal,
                    .exceptions = null,
                    .exception_indices = null,
                };
            }

            const exceptions = try allocator.alignedAlloc(V, Alignment, num_exceptions);
            errdefer allocator.free(exceptions);
            var exception_indices = try std.bit_set.DynamicBitSet.initEmpty(allocator, elems.len);
            errdefer exception_indices.deinit();
            try collect_exceptions(elems, &exception_indices, exceptions, minVal);

            return Encoded{
                .allocator = allocator,
                .bytes = encoded,
                .elems_len = elems.len,
                .min_val = minVal,
                .exceptions = exceptions,
                .exception_indices = exception_indices,
            };
        }

        /// A single tranche takes T input vectors and produces W output vectors.
        /// Returns the number of elements unable to be encoded in W bits.
        fn encode_tranche(in: *const [T]FLVec(V), minVal: ?V, out: *[W]FLVec(UV)) usize {
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

        pub fn decode(encoded: Encoded, allocator: Allocator) ![]align(Alignment) V {
            var elems = try allocator.alignedAlloc(V, Alignment, encoded.elems_len);
            errdefer allocator.free(elems);

            const ntranches = encoded.elems_len / elemsPerTranche;

            if (comptime codec == .ffor) {
                if (encoded.min_val == null and encoded.elems_len > 0) {
                    return error.MismatchedCodecs;
                }
            } else {
                if (encoded.min_val != null) {
                    return error.MismatchedCodecs;
                }
            }

            // vectorized decoding for most of the data (except very small arrays)
            const in: []const FLVec(UV) = @alignCast(std.mem.bytesAsSlice(FLVec(UV), encoded.bytes[0 .. ntranches * bytesPerTranche]));
            var out: []FLVec(V) = @alignCast(std.mem.bytesAsSlice(FLVec(V), std.mem.sliceAsBytes(elems[0 .. ntranches * elemsPerTranche])));
            for (0..ntranches) |i| {
                decode_tranche(in[W * i ..][0..W], encoded.min_val, out[T * i ..][0..T]);
            }

            // fallback logic to unpack the tail
            const remaining = elems[ntranches * elemsPerTranche ..];
            const packedInts = std.PackedIntSlice(P){
                .bytes = @constCast(encoded.bytes[ntranches * bytesPerTranche ..]),
                .bit_offset = 0,
                .len = remaining.len,
            };
            for (0..remaining.len) |i| {
                const val: UV = @intCast(packedInts.get(i));
                remaining[i] = maybe_frame_decode(V, val, encoded.min_val);
            }

            // check if we have exceptions/patches to overlay
            const num_exceptions: usize = if (encoded.exceptions) |ex| ex.len else 0;
            if (num_exceptions == 0) {
                return elems;
            }
            if (encoded.exception_indices) |idx| {
                if (idx.count() != num_exceptions) {
                    std.debug.print("idx.capacity: {}, idx.count: {}, num_exceptions: {}\n", .{
                        idx.capacity(),
                        idx.count(),
                        num_exceptions,
                    });
                    return error.MisalignedCodecPatches;
                }
            } else {
                return error.MisalignedCodecPatches;
            }

            // unpack the patches
            var pos_iter = encoded.exception_indices.?.iterator(.{});
            var i: usize = 0;
            while (pos_iter.next()) |pos| {
                elems[pos] = encoded.exceptions.?[i];
                i += 1;
            }
            return elems;
        }

        fn decode_tranche(in: *const [W]FLVec(UV), minVal: ?V, out: *[T]FLVec(V)) void {
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
        fn count_remaining_exceptions(elems: []const V, minVal: ?V) usize {
            var count: usize = 0;
            for (elems) |elem| {
                const value = maybe_frame_encode(UV, elem, minVal);
                count += @intFromBool(@clz(value) < T - W);
            }
            return count;
        }

        fn collect_exceptions(elems: []const V, exception_indices: *std.bit_set.DynamicBitSet, exceptions: []V, minVal: ?V) !void {
            if (comptime codec == .ffor) {
                std.debug.assert(minVal != null);
            } else {
                std.debug.assert(minVal == null);
            }

            const ntranches = elems.len / elemsPerTranche;
            if (ntranches > 0) {
                const vecs: []const FLVec(V) = @alignCast(std.mem.bytesAsSlice(FLVec(V), std.mem.sliceAsBytes(elems[0 .. ntranches * elemsPerTranche])));
                const minVec: ?FLVec(V) = if (codec == .ffor) @as(FLVec(V), @splat(minVal.?)) else null;

                var offset: usize = 0;
                for (0..ntranches) |tranche_idx| {
                    inline for (vecs[T * tranche_idx ..][0..T]) |vec| {
                        const is_exception_vec: @Vector(vlen, bool) = @clz(maybe_frame_encode(FLVec(UV), vec, minVec)) < TminusW;
                        inline for (0..vlen) |i| {
                            exception_indices.setValue(offset + i, is_exception_vec[i]);
                        }
                        offset += vlen;
                    }
                }
            }

            const offset = ntranches * elemsPerTranche;
            const remaining = elems[offset..];
            for (remaining, offset..) |elem, idx| {
                const value = maybe_frame_encode(UV, elem, minVal);
                exception_indices.setValue(idx, @clz(value) < T - W);
            }

            var pos_iter = exception_indices.iterator(.{});
            var i: usize = 0;
            while (pos_iter.next()) |pos| {
                exceptions[i] = elems[pos];
                i += 1;
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
    try std.testing.expectEqual(@as(usize, 256), PackedInts(8, 2).encodedSize(1024));

    // Pack 8 bit ints into 6 bit ints
    try std.testing.expectEqual(@as(usize, 768), PackedInts(8, 6).encodedSize(1024));
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

const benchmarks = @import("benchmarks.zig");
test "simple packed int benchmark" {
    try benchmarks.bitpackingIntegers("Packed Ints", PackedInts, 32, 1, 10_000_000, 1);
}

test "simple signed ffor benchmark" {
    try benchmarks.bitpackingIntegers("Signed FFOR", SignedFFOR, 32, 1, 10_000_000, -100);
}

test "simple unsigned ffor benchmark" {
    try benchmarks.bitpackingIntegers("Unsigned FFOR", UnsignedFFOR, 32, 1, 10_000_000, 100);
}
