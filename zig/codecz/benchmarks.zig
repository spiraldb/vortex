const std = @import("std");

pub const SignedIntTypes = [_]type{ i8, i16, i32, i64, i128 };
pub const UnsignedIntTypes = [_]type{ u8, u16, u32, u64, u128 };
pub const FloatTypes = [_]type{ f16, f32, f64, f80, f128 };

fn DecimalGenerator(comptime F: type, comptime n: usize, comptime precision: u5) type {
    return struct {
        pub fn generate(ally: std.mem.Allocator, rand: std.rand.Random) ![]F {
            var result = try ally.alloc(F, n);
            errdefer ally.free(result);

            var buf: [64]u8 = [_]u8{0} ** 64; // 64 of something is enough for anyone
            var fbs = std.io.fixedBufferStream(&buf);
            for (0..n) |i| {
                const value64: f64 = 100.0 * rand.floatNorm(f64);
                try std.fmt.formatFloatDecimal(value64, .{ .precision = precision }, fbs.writer());
                result[i] = try std.fmt.parseFloat(F, buf[0..fbs.pos]);
                fbs.reset();
            }
            return result;
        }
    };
}

pub fn generatedDecimals(comptime codec_fn: fn (comptime F: type) type, comptime name: []const u8) !void {
    const ally = std.testing.allocator;

    // std.fmt.parseFloat doesn't support f80
    const FPTypes = [_]type{ f32, f64, f128 };
    const precisions = [_]u5{ 3, 8, 16 };
    const N = 1_000_000;

    var R = std.rand.DefaultPrng.init(42); // a very deterministic but meaningful universe
    const rand = R.random();
    inline for (FPTypes, precisions) |F, precision| {
        const values: []F = try DecimalGenerator(F, N, precision).generate(ally, rand);
        defer ally.free(values);

        const codec = codec_fn(F);
        var timer = try std.time.Timer.start();
        var result = try codec.encode(ally, values);
        defer result.deinit();
        const encode_nanos = timer.lap();
        std.debug.print("{s} ENCODE: {} million floats {} per second ({}ms)\n", .{
            name,
            1000 * N / encode_nanos,
            F,
            encode_nanos / 1_000_000,
        });
        const success_count = N - result.exceptionCount();
        const success_rate = @as(f64, @floatFromInt(success_count)) / N * 100.0;
        std.debug.print("{s} ENCODE: Success rate of {d}% ({} of {} were converted to decimals)\n", .{
            name,
            success_rate,
            success_count,
            N,
        });

        timer.reset();
        var decoded = try codec.decode(ally, result);
        if (@hasDecl(codec, "patch")) {
            try codec.patch(F, values, result.exceptionPositions, decoded[0..decoded.len]);
        }
        const decode_nanos = timer.lap();
        std.debug.print("{s} DECODE: {} million floats {} per second ({}ms)\n", .{
            name,
            1000 * N / decode_nanos + 1,
            F,
            decode_nanos / 1_000_000,
        });
        defer ally.free(decoded);

        try std.testing.expectEqualSlices(F, values, decoded);
    }
}

pub fn testFloatsRoundTrip(comptime codec_fn: fn (comptime F: type) type) !void {
    inline for (FloatTypes) |F| {
        const ally = std.testing.allocator;
        const vals: [12]F = .{ -10.0, -5.5, -0.0, 0.0, 1.99, 2.0, 2.1, 2.4, 2.5, 2.6, 3000.0, -2_500_000_000_000_000.0 };

        const codec = codec_fn(F);
        var encoded = try codec.encode(ally, &vals);
        defer encoded.deinit();

        const decoded = try codec.decode(ally, encoded);
        defer ally.free(decoded);
        if (@hasDecl(codec, "patch")) {
            try codec.patch(F, &vals, encoded.exceptionPositions, decoded[0..decoded.len]);
        }
        try std.testing.expectEqualSlices(F, &vals, decoded);
    }
}
