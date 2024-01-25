const std = @import("std");
const builtin = @import("builtin");
const abi = @import("abi");

const c = @cImport({
    @cInclude("wrapper.h");
});

test "math" {
    const ints = [_]i32{ 1, 2, 3, 4, 5 };
    try std.testing.expectEqual(c.codecz_math_max_i32(@ptrCast(&ints), ints.len), 5);
    try std.testing.expectEqual(c.codecz_math_min_i32(@ptrCast(&ints), ints.len), 1);
    try std.testing.expectEqual(c.codecz_math_isConstant_i32(@ptrCast(&ints), ints.len), false);
    try std.testing.expectEqual(c.codecz_math_isSorted_i32(@ptrCast(&ints), ints.len), true);

    const runstats = c.codecz_math_runLengthStats_i32(@ptrCast(&ints), ints.len);
    try std.testing.expectEqual(runstats.runCount, 0);
    try std.testing.expectEqual(runstats.runElementCount, 0);
}

test "alignment 128" {
    try std.testing.expectEqual(c.SPIRAL_ALIGNMENT, 128);
}

test "run end encoding" {
    const gpa = std.testing.allocator;
    const ints = [_]i32{ 1, 1, 1, 2, 3, 4, 4, 5 };
    const numRuns = 5;

    const valuesOut: []align(128) i32 = try gpa.alignedAlloc(i32, c.SPIRAL_ALIGNMENT, 5);
    defer gpa.free(valuesOut);
    const valuesBuf = c.ByteBuffer_t{ .ptr = @ptrCast(valuesOut.ptr), .len = valuesOut.len * @sizeOf(i32) };

    const runEndsOut: []align(128) u32 = try gpa.alignedAlloc(u32, c.SPIRAL_ALIGNMENT, 5);
    defer gpa.free(runEndsOut);
    const runEndsBuf = c.ByteBuffer_t{ .ptr = @ptrCast(runEndsOut.ptr), .len = runEndsOut.len * @sizeOf(u32) };

    const result = c.codecz_ree_encode_i32_u32(@ptrCast(&ints), ints.len, valuesBuf, runEndsBuf);

    try std.testing.expectEqual(result.status, c.Ok);

    try std.testing.expectEqualDeep(result.firstBuffer.buffer, valuesBuf);
    try std.testing.expect(std.mem.isAligned(@intFromPtr(result.firstBuffer.buffer.ptr), 128));
    try std.testing.expectEqual(result.firstBuffer.numElements, numRuns);
    try std.testing.expectEqual(result.firstBuffer.bitSizePerElement, 32);
    try std.testing.expectEqual(result.firstBuffer.inputBytesUsed, valuesOut.len * @sizeOf(i32));

    try std.testing.expectEqualDeep(result.secondBuffer.buffer, runEndsBuf);
    try std.testing.expect(std.mem.isAligned(@intFromPtr(result.secondBuffer.buffer.ptr), 128));
    try std.testing.expectEqual(result.secondBuffer.numElements, numRuns);
    try std.testing.expectEqual(result.secondBuffer.bitSizePerElement, 32);
    try std.testing.expectEqual(result.secondBuffer.inputBytesUsed, runEndsOut.len * @sizeOf(u32));

    const values = [_]i32{ 1, 2, 3, 4, 5 };
    try std.testing.expectEqualSlices(i32, &values, valuesOut);

    const runEnds = [_]u32{ 3, 4, 5, 7, 8 };
    try std.testing.expectEqualSlices(u32, &runEnds, runEndsOut);
}

test "alp encoding" {
    const gpa = std.testing.allocator;
    const floats = [_]f64{
        1.0,
        1.1,
        1.11,
        2.73,
        3.14159,
        42.000001,
        400_000.12,
        -1.23456,
        4.123457612347956123084712340569871234, // this will be an exception that needs patching
    };

    const valuesOut: []align(128) i64 = try gpa.alignedAlloc(i64, c.SPIRAL_ALIGNMENT, floats.len);
    defer gpa.free(valuesOut);
    const valuesBuf = blk: {
        const buf = c.ByteBuffer_t{ .ptr = @ptrCast(valuesOut.ptr), .len = valuesOut.len * @sizeOf(i64) };
        break :blk try abi.ByteBuffer.from(buf);
    };

    const bitsetOut: []align(128) u8 = try gpa.alignedAlloc(u8, c.SPIRAL_ALIGNMENT, (floats.len + 7) / 8);
    defer gpa.free(bitsetOut);
    const bitsetBuf = blk: {
        const buf = c.ByteBuffer_t{ .ptr = @ptrCast(bitsetOut.ptr), .len = bitsetOut.len * @sizeOf(u8) };
        break :blk try abi.ByteBuffer.from(buf);
    };

    const expResult = c.codecz_alp_sampleFindExponents_f64(@ptrCast(&floats), floats.len);
    try std.testing.expect(expResult.status == c.Ok);
    const exponents = expResult.exponents;
    try std.testing.expectEqual(exponents.e, 8);
    try std.testing.expectEqual(exponents.f, 2);

    const encoded = try abi.TwoBufferResult.from(c.codecz_alp_encode_f64(
        @ptrCast(&floats),
        floats.len,
        exponents,
        valuesBuf.into(),
        bitsetBuf.into(),
    ));
    try std.testing.expectEqual(encoded.status, abi.ResultStatus.Ok);

    try std.testing.expectEqualDeep(encoded.firstBuffer.buffer, valuesBuf);
    try std.testing.expect(std.mem.isAligned(@intFromPtr(encoded.firstBuffer.buffer.ptr), 128));
    try std.testing.expectEqual(encoded.firstBuffer.bitSizePerElement, @bitSizeOf(i64));
    try std.testing.expectEqual(encoded.firstBuffer.inputBytesUsed, valuesOut.len * @sizeOf(i64));
    try std.testing.expectEqual(encoded.firstBuffer.numElements, floats.len);

    try std.testing.expectEqualDeep(encoded.secondBuffer.buffer, bitsetBuf);
    try std.testing.expect(std.mem.isAligned(@intFromPtr(encoded.secondBuffer.buffer.ptr), 128));
    try std.testing.expectEqual(encoded.secondBuffer.bitSizePerElement, 1);
    try std.testing.expectEqual(encoded.secondBuffer.inputBytesUsed, bitsetOut.len * @sizeOf(u8));
    try std.testing.expectEqual(encoded.secondBuffer.numElements, 1); // in this case, this is num exceptions

    const values = blk: {
        var values_: [floats.len]i64 = undefined;
        const pow: f64 = @floatFromInt(std.math.pow(usize, 10, exponents.e - exponents.f));
        for (0..floats.len) |i| {
            values_[i] = @intFromFloat(@round(floats[i] * pow));
        }
        break :blk values_;
    };
    try std.testing.expectEqualSlices(i64, &values, valuesOut);

    const bitset = std.PackedIntSlice(u1){
        .bytes = bitsetOut,
        .bit_offset = 0,
        .len = floats.len,
    };
    for (0..floats.len - 1) |i| {
        try std.testing.expectEqual(bitset.get(i), 0);
    }
    try std.testing.expectEqual(bitset.get(floats.len - 1), 1);

    const decodeOut: []align(128) f64 = try gpa.alignedAlloc(f64, c.SPIRAL_ALIGNMENT, floats.len);
    defer gpa.free(decodeOut);
    const decodeBuf = blk: {
        const buf = c.ByteBuffer_t{ .ptr = @ptrCast(decodeOut.ptr), .len = decodeOut.len * @sizeOf(f64) };
        break :blk try abi.ByteBuffer.from(buf);
    };

    const decoded = try abi.OneBufferResult.from(c.codecz_alp_decode_f64(
        @ptrCast(valuesOut.ptr),
        valuesOut.len,
        exponents,
        decodeBuf.into(),
    ));
    try std.testing.expectEqual(decoded.status, abi.ResultStatus.Ok);

    try std.testing.expectEqualDeep(decoded.buffer.buffer, decodeBuf);
    try std.testing.expect(std.mem.isAligned(@intFromPtr(decoded.buffer.buffer.ptr), 128));
    try std.testing.expectEqual(decoded.buffer.bitSizePerElement, @bitSizeOf(f64));
    try std.testing.expectEqual(decoded.buffer.inputBytesUsed, decodeOut.len * @sizeOf(f64));
    try std.testing.expectEqual(decoded.buffer.numElements, floats.len);
    try std.testing.expectEqualSlices(f64, floats[0 .. floats.len - 1], decodeOut[0 .. decodeOut.len - 1]);
    // last one doesn't round trip, but it's close
    try std.testing.expectApproxEqAbs(floats[floats.len - 1], decodeOut[decodeOut.len - 1], 1e-6);
}
