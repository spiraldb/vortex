const std = @import("std");
const Allocator = std.mem.Allocator;

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

pub fn coveringIntTypePowerOfTwo(comptime F: type) type {
    // we round up coveringIntBits to next power of 2, since we might as well use every physically allocated bit
    return std.meta.Int(.signed, std.math.ceilPowerOfTwoAssert(u8, coveringIntBits(F)));
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

test "covering int bit-width" {
    try std.testing.expectEqual(12, coveringIntBits(f16));
    try std.testing.expectEqual(25, coveringIntBits(f32));
    try std.testing.expectEqual(54, coveringIntBits(f64));
    try std.testing.expectEqual(65, coveringIntBits(f80));
    try std.testing.expectEqual(114, coveringIntBits(f128));

    try std.testing.expectEqual(@sizeOf(i25), @sizeOf(i32));
    try std.testing.expectEqual(@sizeOf(i54), @sizeOf(i64));
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
    const FloatTypes = [_]type{ f16, f32, f64, f80, f128 };
    inline for (FloatTypes) |F| {
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
