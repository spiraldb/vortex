const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn ZigZag(comptime V: type) type {
    if (@typeInfo(V) != .Int or @typeInfo(V).Int.signedness != .signed) {
        @compileError("Only call ZigZag encoding on signed integers");
    }
    const U = std.meta.Int(.unsigned, @bitSizeOf(V));

    return struct {
        pub const Signed = V;
        pub const Unsigned = U;

        const shift_for_sign_bit = @bitSizeOf(V) - 1;

        pub fn encode(elems: []const V, out: []U) void {
            for (elems, out) |elem, *o| {
                o.* = encode_single(elem);
            }
        }

        pub fn encodeAlloc(allocator: std.mem.Allocator, elems: []const V) ![]const U {
            const out = try allocator.alloc(U, elems.len);
            encode(elems, out);
            return out;
        }

        pub inline fn encode_single(val: V) U {
            return @bitCast((val +% val) ^ (val >> shift_for_sign_bit));
        }

        pub fn decode(encoded: []const U, out: []V) void {
            for (encoded, out) |elem, *o| {
                o.* = decode_single(elem);
            }
        }

        pub fn decodeAlloc(allocator: std.mem.Allocator, encoded: []const U) ![]const V {
            const out = try allocator.alloc(V, encoded.len);
            decode(encoded, out);
            return out;
        }

        pub inline fn decode_single(val: U) V {
            return @bitCast((val >> 1) ^ (0 -% (val & 1)));
        }
    };
}

test "zigzag encode yields small ints" {
    const ally = std.testing.allocator;
    const zz = ZigZag(i32);

    // maxInt(i32) is 2_147_483_647, minInt(i32) is -2_147_483_648
    const vals = [_]i32{ 0, -1, 1, -2, 2, std.math.maxInt(i32), std.math.minInt(i32) };
    const expected_enc = [_]u32{ 0, 1, 2, 3, 4, std.math.maxInt(u32) - 1, std.math.maxInt(u32) };

    const encoded = try zz.encodeAlloc(ally, &vals);
    defer ally.free(encoded);
    try std.testing.expectEqualSlices(u32, &expected_enc, encoded);

    const decoded = try zz.decodeAlloc(ally, encoded);
    defer ally.free(decoded);
    try std.testing.expectEqualSlices(i32, &vals, decoded);
}

test "zigzag benchmark" {
    const ally = std.testing.allocator;
    const Ts: [5]type = .{ i8, i16, i32, i64, i128 };
    const N = 20_000_000;

    var R = std.rand.DefaultPrng.init(0);
    var rand = R.random();
    inline for (Ts) |T| {
        const zz = ZigZag(T);

        var values = try ally.alloc(T, N);
        defer ally.free(values);
        for (0..values.len) |i| {
            values[i] = rand.int(T);
        }

        const encoded = try ally.alloc(zz.Unsigned, values.len);
        defer ally.free(encoded);

        var timer = try std.time.Timer.start();
        zz.encode(values, encoded);
        const encode_ns = timer.lap();
        std.debug.print("ZIGZAG ENCODE: {} million ints per second ({}ms)\n", .{ 1000 * N / (encode_ns + 1), encode_ns / 1_000_000 });

        const decoded = try ally.alloc(T, values.len);
        defer ally.free(decoded);

        timer.reset();
        zz.decode(encoded, decoded);
        const decode_ns = timer.lap();
        std.debug.print("ZIGZAG DECODE: {} million ints per second ({}ms)\n", .{ 1000 * N / (decode_ns + 1), decode_ns / 1_000_000 });

        try std.testing.expectEqualSlices(T, values, decoded);
    }
}
