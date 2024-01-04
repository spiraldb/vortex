const std = @import("std");
const Allocator = std.mem.Allocator;
const zimd = @import("zimd");
const builtin = @import("builtin");

const ByteVec = @Vector(16, u8);
const MaskVec = @Vector(16, i8);
const CodeVec = @Vector(4, u2);

const svb = StreamVByte(u32);

const encode_masks: [256]MaskVec = blk: {
    var masks_: [256]MaskVec = undefined;

    for (0..256) |code_idx| {
        masks_[code_idx] = encode_shuffle_mask(code_idx);
    }
    break :blk masks_;
};

const decode_table: [256]MaskVec = blk: {
    var decodeTable: [256]MaskVec = undefined;
    for (0..255) |c| {
        decodeTable[c] = decode_shuffle_mask(c);
    }
    break :blk decodeTable;
};

fn encode_shuffle_mask(comptime code_idx: u8) MaskVec {
    @setEvalBranchQuota(8 * 16 * 256);

    const code = @as(CodeVec, @bitCast(@as(u8, @truncate(code_idx))));

    var mask: [16]i8 = undefined;

    // We want all input indices to be present in the mask so that
    // the compiler will optimise us into a vectorized shuffle instruction.

    // So for each integer, we put the bytes we want at the front of the mask
    // and the bytes we don't at the back.
    var front = 0;
    var back = 15;
    for (0..4) |int| {
        // For each integer, we find out how many bytes we want from it.
        const byteLen = @as(u8, @intCast(code[int])) + 1;
        // The position of the int in the input vector
        const pos = int * 4;

        for (0..4) |b| {
            if (b < byteLen) {
                mask[front] = pos + b;
                front += 1;
            } else {
                mask[back] = pos + b;
                back -= 1;
            }
        }
    }

    return mask;
}

fn decode_shuffle_mask(comptime code_idx: u8) MaskVec {
    @setEvalBranchQuota(8 * 16 * 256);

    const code = @as(CodeVec, @bitCast(@as(u8, @truncate(code_idx))));

    var mask: [16]i8 = .{-1} ** 16;

    var pos: u8 = 0;
    inline for (@as([4]u2, code), 0..) |code_u2, int| {
        const c: u8 = @intCast(code_u2);
        const offset = int * 4;
        inline for (0..c + 1) |byte| {
            const u8Byte: u8 = @intCast(byte);
            mask[offset + u8Byte] = pos + u8Byte;
        }
        pos += c + 1;
    }

    return mask;
}

pub const Result = struct { control: []const u8, data: []const u8 };

pub fn StreamVByte(comptime V: type) type {
    const lengths: [256]u8 = blk: {
        var lengths_: [256]u8 = undefined;
        for (0..256) |code_idx| {
            const code = @as(CodeVec, @bitCast(@as(u8, @truncate(code_idx))));
            lengths_[code_idx] = @reduce(.Add, @as(@Vector(4, u8), code)) + 4;
        }
        break :blk lengths_;
    };

    return struct {
        const PADDING: usize = 16;

        pub fn maxCompressedSize(length: usize) usize {
            return controlBytesSize(length) + maxCompressedDataSize(length);
        }

        pub fn maxCompressedDataSize(length: usize) usize {
            return (length * @sizeOf(V)) + PADDING;
        }

        pub fn controlBytesSize(length: usize) usize {
            return (length + 3) / 4;
        }

        /// Encode the elements into control and data result arrays.
        /// Returns the number of bytes used of the data array.
        pub fn encode(elems: []const V, control: []u8, data: []u8) usize {
            // Process groups of 4.
            var count: usize = 0;
            var idx: usize = 0;
            var written: usize = 0;

            const quadCount = elems.len / 4;
            for (0..quadCount) |_| {
                written += encode_quad(elems[count..][0..4], control[idx..], data[written..]);
                count += 4;
                idx += 1;
            }

            if (count < elems.len) {
                written += encode_scalar(elems[count..], control[elems.len / 4 ..], data[written..]);
            }

            // Add some padding so that the decoder can SIMD process all the bytes?
            // TODO(ngates): possibly not
            @memset(data[written..][0..PADDING], 0);
            written += PADDING;

            return written;
        }

        inline fn encode_quad(elems: *const [4]V, control: []u8, data: []u8) usize {
            const ValVec = @Vector(4, V);

            const elemsVec = @as(ValVec, elems.*);

            // Immediately casting to a i16 enables us to use intrinsics for CLZ (vs Zig's builtin which returns u(n - 1))
            const leadingZeros = @as(@Vector(4, i32), @clz(elemsVec));

            // Shift by 3 to divide by 8 and get a leading byte count
            const leadingZerosBytes = leadingZeros >> @splat(3);
            // Saturated subtract from 3 (saturated => meaning overflow sticks to zero).
            // TODO(ngates): Zig does have a builtin, but it may be faster by hand?
            const codeBytesOverflow = @as(@Vector(4, i32), @splat(3)) - leadingZerosBytes;
            const codeBytes = @select(
                i32,
                codeBytesOverflow < @as(@Vector(4, i32), @splat(0)),
                @as(@Vector(4, i32), @splat(0)),
                codeBytesOverflow,
            );

            // We now have a 2-bit code for each integer. Just need to get them in the right place.
            // Left-shift the bits in each the relative positions, then reduce.
            const shifted = @as(@Vector(4, u32), @bitCast(codeBytes)) << @Vector(4, u8){ 0, 2, 4, 6 };
            const code = @as(u8, @truncate(@reduce(.Or, shifted)));
            control[0] = code;

            // Grab the input bytes we need by computing (looking up) our shuffle mask.
            const outputLength = lengths[code];

            const input = @as(@Vector(16, u8), @bitCast(elems.*));
            data[0..16].* = zimd.tableLookupBytesOr0(input, encode_masks[code]);

            return outputLength;
        }

        fn encode_scalar(elems: []const V, control: []u8, data: []u8) usize {
            if (elems.len == 0) {
                return 0;
            }

            // We build up the control data into a var u8 to avoid too many loads/stores.
            var controlBuffer = control;
            var shift: u8 = 0; // cycles 0, 2, 4, 6, 0, 2, 4, 6, ...
            var key: u8 = 0;
            var written: usize = 0;
            for (elems) |e| {
                const code = encode_data(e, data[written..]);
                written += code + 1;
                key = key | (code << @truncate(shift));
                shift += 2;
            }

            controlBuffer[0] = key;
            return written;
        }

        fn encode_data(elem: V, data: []u8) u8 {
            const elemBytes = @as([4]u8, @bitCast(elem));
            if (elem < (1 << 8)) { // 1 byte
                data[0] = elemBytes[0];
                return 0;
            } else if (elem < (1 << 16)) { // 2 bytes
                @memcpy(data[0..2], elemBytes[0..2]);
                return 1;
            } else if (elem < (1 << 24)) { // 3 bytes
                @memcpy(data[0..3], elemBytes[0..3]);
                return 2;
            } else { // 4 bytes
                @memcpy(data[0..4], elemBytes[0..4]);
                return 3;
            }
        }

        pub fn decode(control: []const u8, data: []const u8, elems: []V) void {
            // Process groups of 4.
            var count: usize = 0;
            var idx: usize = 0;
            var consumed: usize = 0;
            while (count + 3 < elems.len) {
                consumed += decode_quad(control[idx], data[consumed..][0..16].*, elems[count..][0..4]);
                count += 4;
                idx += 1;
            }

            // Decode the remainder
            const remainder = elems.len - count;
            if (remainder > 0) {
                // We need to shift each of the 2bit values out of the control byte
                const ctrl_byte = control[idx];

                for (0..remainder) |r| {
                    const shift = 6 - (r * 2);
                    const control_bits: u8 = (ctrl_byte >> @intCast(shift)) & 0x03;
                    consumed += decode_data(control_bits, data[consumed..], &elems[count]);
                    count += 1;
                }
            }
        }

        fn decode_quad(control: u8, data: [16]u8, elems: []V) usize {
            const dataVec = @as(ByteVec, @bitCast(data));
            const decoded = zimd.tableLookupBytesOr0(dataVec, decode_table[control]);
            std.mem.sliceAsBytes(elems[0..4])[0..16].* = decoded;
            return lengths[control];
        }

        fn decode_data(control: u8, data: []const u8, elem: *V) u8 {
            elem.* = 0;
            var elemBytes: *[4]u8 = @ptrCast(elem);

            if (control == 0) { // 1 bytes
                elemBytes[0] = data[0];
                return 1;
            } else if (control == 1) {
                @memcpy(elemBytes[0..2], data[0..2]);
                return 2;
            } else if (control == 2) {
                @memcpy(elemBytes[0..3], data[0..3]);
                return 3;
            } else {
                @memcpy(elemBytes, data[0..4]);
                return 4;
            }
        }
    };
}

test "encode zero elems" {
    var data: [svb.PADDING]u8 = .{0} ** svb.PADDING;
    const compressed_size = svb.encode(&.{}, &.{}, @constCast(data[0..svb.PADDING]));
    try std.testing.expectEqual(svb.PADDING, compressed_size);
}

test "encode non-quad elems" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const ally = gpa.allocator();

    const ctrl = try ally.alloc(u8, svb.controlBytesSize(3));
    defer ally.free(ctrl);
    const data = try ally.alloc(u8, svb.maxCompressedDataSize(3));
    defer ally.free(data);

    const compressed_size = svb.encode(&.{ 1, 2, 3 }, ctrl, data);

    const decoded = try ally.alloc(u32, 3);
    defer ally.free(decoded);
    svb.decode(ctrl, data[0..compressed_size], decoded);

    try std.testing.expectEqualSlices(u32, &.{ 1, 2, 3 }, decoded);
}

fn runtest() !void {
    const testing = std.testing;
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const ally = gpa.allocator();

    const T = u32; // This is fixed for now.
    const N = 100_000 * 16;

    // Lol, constant seed
    var R = std.rand.DefaultPrng.init(0);
    var rand = R.random();
    var values = try ally.alloc(T, N);
    defer ally.free(values);
    for (0..values.len) |i| {
        values[i] = rand.intRangeAtMostBiased(T, 0, 100_000);
    }

    const result = try ally.alloc(u8, svb.maxCompressedSize(N));
    defer ally.free(result);
    const control = result[0 .. values.len / 4];
    const data = result[control.len..];

    var timer = try std.time.Timer.start();
    const written = svb.encode(values, control, data);
    const encode_ns = timer.lap();
    std.debug.print("OUR SVB ENCODE: {} million ints per second ({}ms)\n", .{
        1000 * N / (encode_ns + 1),
        encode_ns / 1_000_000,
    });

    const decoded = try ally.alloc(T, values.len);
    defer ally.free(decoded);
    timer.reset();
    svb.decode(control, data, decoded);
    const decode_ns = timer.lap();
    std.debug.print("OUR SVB DECODE: {} million ints per second ({}ms)\n", .{
        1000 * N / decode_ns,
        decode_ns / 1_000_000,
    });

    try testing.expectEqualSlices(u32, values, decoded);

    const all_features = builtin.cpu.arch.allFeaturesList();
    var populated_cpu_features = builtin.cpu.model.features;
    populated_cpu_features.populateDependencies(all_features);
    std.debug.print("CPU FEATURES: [ ", .{});
    for (all_features, 0..) |feature, i_usize| {
        const i = @as(std.Target.Cpu.Feature.Set.Index, @intCast(i_usize));
        const in_cpu_set = populated_cpu_features.isEnabled(i);
        if (in_cpu_set) {
            std.debug.print("{s}, ", .{feature.name});
        }
    }
    std.debug.print("]\n", .{});

    // For whatever reason, SVB doesn't work on Github Actions. Seems like they don't correctly check feature set?
    if (builtin.cpu.arch.isAARCH64() or (builtin.cpu.arch.isX86() and std.Target.x86.featureSetHas(builtin.cpu.features, .ssse3))) {
        const c_svb = @cImport({
            @cInclude("streamvbyte.h");
        });

        const svbResult = try ally.alloc(u8, result.len);
        defer ally.free(svbResult);

        timer.reset();
        const compressedSize = c_svb.streamvbyte_encode(values.ptr, @intCast(values.len), svbResult.ptr);
        const svb_encode_ns = timer.lap();
        std.debug.print("LEMIRE SVB ENCODE: {} million ints per second ({}ms)\n", .{
            1000 * N / svb_encode_ns,
            svb_encode_ns / 1_000_000,
        });

        try testing.expectEqual(compressedSize, written + control.len - 16);
        try testing.expectEqualSlices(u8, svbResult[0..compressedSize], result[0..compressedSize]);

        const decodedSvb = try ally.alloc(T, values.len);
        defer ally.free(decodedSvb);
        timer.reset();
        _ = c_svb.streamvbyte_decode(svbResult.ptr, decodedSvb.ptr, @intCast(values.len));
        const svb_decode_ns = timer.lap();
        std.debug.print("LEMIRE SVB DECODE: {} million ints per second ({}ms)\n", .{
            1000 * N / svb_decode_ns,
            svb_decode_ns / 1_000_000,
        });

        try testing.expectEqualSlices(u32, values, decodedSvb);
    }
}

pub fn main() void {
    runtest() catch std.debug.panic("DOH", .{});
}

test "svb test" {
    try runtest();
}
