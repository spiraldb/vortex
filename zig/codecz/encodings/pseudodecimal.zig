const std = @import("std");
const Allocator = std.mem.Allocator;
const codecmath = @import("../codecmath.zig");

pub fn PseudoDecimal(comptime F: type) type {
    codecmath.comptimeCheckFloat(F);
    return struct {
        // for benchmarking against ALP, we use same number of bits per float (in some sense, ALP has a per-float ET of u0)
        const ET = u5;
        const DT = std.meta.Int(.signed, @bitSizeOf(codecmath.coveringIntTypePowerOfTwo(F)) - @bitSizeOf(ET));

        const F10 = codecmath.powersOfTen(F);
        const i_F10 = codecmath.inversePowersOfTen(F);

        const PseudoDecimalEncoded = struct {
            const Self = @This();

            allocator: Allocator,
            fractions_b10: []const DT,
            exponents_b10: []const ET,
            patch_values: []const F,
            patch_indices: std.bit_set.DynamicBitSetUnmanaged,

            pub fn exceptionCount(self: Self) usize {
                return self.patch_values.len;
            }

            pub fn deinit(self: Self) void {
                self.allocator.free(self.fractions_b10);
                self.allocator.free(self.exponents_b10);
                self.allocator.free(self.patch_values);
                @constCast(&self.patch_indices).deinit(self.allocator);
            }
        };

        const PackedDecimal = packed struct {
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

            var patch_indices = try std.bit_set.DynamicBitSetUnmanaged.initEmpty(allocator, elems.len);
            errdefer patch_indices.deinit(allocator);

            for (elems, 0..) |elem, i| {
                if (findDecimal(elem)) |packedDecimal| {
                    fractions_b10[i] = packedDecimal.fraction_b10;
                    exponents_b10[i] = packedDecimal.exponent_b10;
                } else {
                    try patches.append(elem);
                    patch_indices.set(@intCast(i));
                }
            }

            return .{
                .allocator = allocator,
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
            for (0..codecmath.maxExponentToTry(F)) |exp| {
                const encoded_float: F = @round(val * F10[exp]);
                if (encoded_float > std.math.maxInt(DT) or encoded_float < std.math.minInt(DT)) {
                    return null;
                }
                const encoded: DT = @intFromFloat(encoded_float);
                const decoded: F = @as(F, @floatFromInt(encoded)) * i_F10[exp];
                if (decoded == val) {
                    return .{
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
                if (encoded.patch_indices.isSet(@intCast(i))) {
                    decoded[i] = encoded.patch_values[patch_count];
                    patch_count += 1;
                } else {
                    decoded[i] = @as(F, @floatFromInt(encoded.fractions_b10[i])) * i_F10[encoded.exponents_b10[i]];
                }
            }

            return decoded;
        }
    };
}

const benchmarks = @import("../benchmarks.zig");

test "pde round trip" {
    try benchmarks.testFloatsRoundTrip(PseudoDecimal);
}

test "pde benchmark" {
    try benchmarks.generatedDecimals(PseudoDecimal, "PDE");
}
