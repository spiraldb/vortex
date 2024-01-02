const std = @import("std");
const Allocator = std.mem.Allocator;
const roaring = @import("roaring");
const codecmath = @import("codecmath.zig");

pub fn PseudoDecimal(comptime F: type) type {
    codecmath.comptimeCheckFloat(F);
    return struct {
        // for benchmarking against ALP, we use same number of bits per float (in some sense, ALP has a per-float ET of u0)
        const ET = u5;
        const SDT = std.meta.Int(.signed, codecmath.coveringIntBits(F) - @bitSizeOf(ET));
        const DT = std.meta.Int(.unsigned, @bitSizeOf(SDT) - 1); // strip off the sign

        const F10 = codecmath.powersOfTen(F);
        const i_F10 = codecmath.inversePowersOfTen(F);

        const PseudoDecimalEncoded = struct {
            const Self = @This();

            allocator: Allocator,
            signs: *roaring.Bitmap,
            fractions_b10: []const DT,
            exponents_b10: []const ET,
            patch_values: []const F,
            patch_indices: *roaring.Bitmap,

            pub fn exceptionCount(self: Self) usize {
                return self.patch_values.len;
            }

            pub fn deinit(self: Self) void {
                self.signs.free();
                self.allocator.free(self.fractions_b10);
                self.allocator.free(self.exponents_b10);
                self.allocator.free(self.patch_values);
                self.patch_indices.free();
            }
        };

        const PackedDecimal = packed struct {
            sign: u1,
            fraction_b10: DT,
            exponent_b10: ET,
        };

        pub fn encode(allocator: Allocator, elems: []const F) !PseudoDecimalEncoded {
            if (elems.len > std.math.maxInt(u32)) {
                return error.OutOfBounds;
            }
            var fractions_b10 = try allocator.alloc(DT, elems.len);
            var exponents_b10 = try allocator.alloc(ET, elems.len);
            var patches = std.ArrayList(F).init(allocator);
            defer patches.deinit();

            var signs = try roaring.Bitmap.create();
            errdefer signs.free();
            var patch_indices = try roaring.Bitmap.create();
            errdefer patch_indices.free();

            for (elems, 0..) |elem, i| {
                if (findDecimal(elem)) |packedDecimal| {
                    if (packedDecimal.sign == 1) {
                        signs.add(@intCast(i));
                    }
                    fractions_b10[i] = packedDecimal.fraction_b10;
                    exponents_b10[i] = packedDecimal.exponent_b10;
                } else {
                    try patches.append(elem);
                    patch_indices.add(@intCast(i));
                }
            }

            _ = signs.runOptimize();
            _ = signs.shrinkToFit();
            _ = patch_indices.runOptimize();
            _ = patch_indices.shrinkToFit();
            return .{
                .allocator = allocator,
                .signs = signs,
                .fractions_b10 = fractions_b10,
                .exponents_b10 = exponents_b10,
                .patch_values = try patches.toOwnedSlice(),
                .patch_indices = patch_indices,
            };
        }

        fn findDecimal(val: F) ?PackedDecimal {
            // Special case -0.0 is always handled as exception
            if (val == -0.0) {
                return null;
            }

            // strip out the sign & assume non-negative for the comparison loop
            const sign: u1 = @intFromBool(std.math.signbit(val));
            const positive_val = if (val < 0.0) -val else val;
            for (0..codecmath.maxExponentToTry(F)) |exp| {
                const encoded_float: F = @round(positive_val * F10[exp]);
                if (encoded_float > std.math.maxInt(DT)) {
                    return null;
                }
                const encoded: DT = @intFromFloat(encoded_float);
                const decoded: F = @as(F, @floatFromInt(encoded)) * i_F10[exp];
                if (decoded == positive_val) {
                    return .{
                        .sign = sign,
                        .fraction_b10 = encoded,
                        .exponent_b10 = @intCast(exp),
                    };
                }
            }
            return null;
        }

        pub fn decode(allocator: Allocator, encoded: PseudoDecimalEncoded) ![]const F {
            const len: usize = encoded.fractions_b10.len;
            var decoded: []F = try allocator.alloc(F, len);
            errdefer allocator.free(decoded);

            var patch_count: u32 = 0;
            for (0..len) |i| {
                if (encoded.patch_indices.contains(@intCast(i))) {
                    decoded[i] = encoded.patch_values[patch_count];
                    patch_count += 1;
                } else {
                    var signed_fraction: SDT = @intCast(encoded.fractions_b10[i]);
                    if (encoded.signs.contains(@intCast(i))) {
                        signed_fraction = try std.math.negate(signed_fraction);
                    }
                    decoded[i] = @as(F, @floatFromInt(signed_fraction)) * i_F10[encoded.exponents_b10[i]];
                }
            }

            return decoded;
        }
    };
}

const benchmarks = @import("benchmarks.zig");

test "pde round trip" {
    try benchmarks.testFloatsRoundTrip(PseudoDecimal);
}

test "pde benchmark" {
    try benchmarks.generatedDecimals(PseudoDecimal, "PDE");
}
