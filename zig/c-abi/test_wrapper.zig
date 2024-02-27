const std = @import("std");
const builtin = @import("builtin");
const abi = @import("types.zig");

const c = @cImport({
    @cInclude("wrapper.h");
});

test "alignment 128" {
    try std.testing.expectEqual(c.VORTEX_ALIGNMENT, 128);
}

test "result status" {
    const ResultStatus = abi.ResultStatus;
    const CodecError = abi.CodecError;

    try std.testing.expectEqual(ResultStatus.Ok, ResultStatus.from(c.Ok));
    try std.testing.expectEqual(ResultStatus.InvalidEncodingParameter, ResultStatus.from(c.InvalidEncodingParameter));
    try std.testing.expectEqual(ResultStatus.InvalidInput, ResultStatus.from(c.InvalidInput));
    try std.testing.expectEqual(ResultStatus.IncorrectAlignment, ResultStatus.from(c.IncorrectAlignment));
    try std.testing.expectEqual(ResultStatus.EncodingFailed, ResultStatus.from(c.EncodingFailed));
    try std.testing.expectEqual(ResultStatus.OutputBufferTooSmall, ResultStatus.from(c.OutputBufferTooSmall));
    try std.testing.expectEqual(ResultStatus.OutOfMemory, ResultStatus.from(c.OutOfMemory));
    try std.testing.expectEqual(ResultStatus.ShouldBeUnreachable, ResultStatus.from(c.ShouldBeUnreachable));
    try std.testing.expectEqual(ResultStatus.UnknownCodecError, ResultStatus.from(c.UnknownCodecError));

    try std.testing.expectEqual(c.Ok, @intFromEnum(ResultStatus.Ok));
    try std.testing.expectEqual(c.InvalidEncodingParameter, @intFromEnum(ResultStatus.InvalidEncodingParameter));
    try std.testing.expectEqual(c.InvalidInput, @intFromEnum(ResultStatus.InvalidInput));
    try std.testing.expectEqual(c.IncorrectAlignment, @intFromEnum(ResultStatus.IncorrectAlignment));
    try std.testing.expectEqual(c.EncodingFailed, @intFromEnum(ResultStatus.EncodingFailed));
    try std.testing.expectEqual(c.OutputBufferTooSmall, @intFromEnum(ResultStatus.OutputBufferTooSmall));
    try std.testing.expectEqual(c.OutOfMemory, @intFromEnum(ResultStatus.OutOfMemory));
    try std.testing.expectEqual(c.ShouldBeUnreachable, @intFromEnum(ResultStatus.ShouldBeUnreachable));
    try std.testing.expectEqual(c.UnknownCodecError, @intFromEnum(ResultStatus.UnknownCodecError));

    try std.testing.expectEqual(ResultStatus.InvalidEncodingParameter, ResultStatus.fromCodecError(CodecError.InvalidEncodingParameter));
    try std.testing.expectEqual(ResultStatus.InvalidInput, ResultStatus.fromCodecError(CodecError.InvalidInput));
    try std.testing.expectEqual(ResultStatus.IncorrectAlignment, ResultStatus.fromCodecError(CodecError.IncorrectAlignment));
    try std.testing.expectEqual(ResultStatus.EncodingFailed, ResultStatus.fromCodecError(CodecError.EncodingFailed));
    try std.testing.expectEqual(ResultStatus.OutputBufferTooSmall, ResultStatus.fromCodecError(CodecError.OutputBufferTooSmall));
    try std.testing.expectEqual(ResultStatus.OutOfMemory, ResultStatus.fromCodecError(CodecError.OutOfMemory));
    try std.testing.expectEqual(ResultStatus.ShouldBeUnreachable, ResultStatus.fromCodecError(CodecError.ShouldBeUnreachable));
}

test "struct field offsets" {
    try checkStructFieldOffsets(abi.ByteBuffer, c.ByteBuffer_t);
    try checkStructFieldOffsets(abi.WrittenBuffer, c.WrittenBuffer_t);
    try checkStructFieldOffsets(abi.OneBufferResult, c.OneBufferResult_t);
    try checkStructFieldOffsets(abi.TwoBufferResult, c.TwoBufferResult_t);
    try checkStructFieldOffsets(abi.AlpExponents, c.AlpExponents_t);
    try checkStructFieldOffsets(abi.AlpExponentsResult, c.AlpExponentsResult_t);
    try checkStructFieldOffsets(abi.RunLengthStats, c.RunLengthStats_t);
    try checkStructFieldOffsets(abi.OneBufferNumExceptionsResult, c.OneBufferNumExceptionsResult_t);
}

fn checkStructFieldOffsets(zigType: type, cType: type) !void {
    comptime abi.checkStructABI(zigType, cType);
    const zigTypeInfo = @typeInfo(zigType);
    const cTypeInfo = @typeInfo(cType);

    const ztZero = std.mem.zeroes(zigType);
    const ztZeroPtr = @intFromPtr(&ztZero);
    const ctZero = std.mem.zeroes(cType);
    const ctZeroPtr = @intFromPtr(&ctZero);
    if (zigTypeInfo == .Struct and cTypeInfo == .Struct) {
        inline for (zigTypeInfo.Struct.fields, cTypeInfo.Struct.fields) |zf, cf| {
            const zfOffset = @intFromPtr(@fieldParentPtr(zigType, zf.name, &@field(ztZero, zf.name))) - ztZeroPtr;
            const cfOffset = @intFromPtr(@fieldParentPtr(cType, cf.name, &@field(ctZero, cf.name))) - ctZeroPtr;
            try std.testing.expectEqual(zfOffset, cfOffset);

            if (@typeInfo(zf.type) == .Struct and @typeInfo(cf.type) == .Struct) {
                try checkStructFieldOffsets(zf.type, cf.type);
            }
        }
    }
}

test "math" {
    const ints = [_]i32{ 1, 2, 3, 4, 5 };
    try std.testing.expectEqual(c.codecz_math_max_i32(@ptrCast(&ints), ints.len), 5);
    try std.testing.expectEqual(c.codecz_math_min_i32(@ptrCast(&ints), ints.len), 1);
    try std.testing.expectEqual(c.codecz_math_isConstant_i32(@ptrCast(&ints), ints.len), false);
    try std.testing.expectEqual(c.codecz_math_isSorted_i32(@ptrCast(&ints), ints.len), true);

    var runstats: c.RunLengthStats_t = undefined;
    c.codecz_math_runLengthStats_i32(@ptrCast(&ints), ints.len, @ptrCast(&runstats));
    try std.testing.expectEqual(runstats.runCount, 0);
    try std.testing.expectEqual(runstats.runElementCount, 0);
}

test "run end encoding" {
    const gpa = std.testing.allocator;
    const V = i32;
    const ints = [_]V{ 1, 1, 1, 2, 3, 4, 4, 5 };
    const numRuns = 5;

    const valuesOut: []align(128) V = try gpa.alignedAlloc(V, c.VORTEX_ALIGNMENT, 5);
    defer gpa.free(valuesOut);
    const valuesBuf = abi.ByteBuffer.initFromSlice(valuesOut);

    const runEndsOut: []align(128) u32 = try gpa.alignedAlloc(u32, c.VORTEX_ALIGNMENT, 5);
    defer gpa.free(runEndsOut);
    const runEndsBuf = abi.ByteBuffer.initFromSlice(runEndsOut);

    var encoded = abi.TwoBufferResult.empty(valuesBuf, runEndsBuf);
    c.codecz_ree_encode_i32_u32(@ptrCast(&ints), ints.len, @ptrCast(&encoded));

    try std.testing.expectEqual(encoded.status, abi.ResultStatus.Ok);

    try std.testing.expectEqualDeep(encoded.first.buffer, valuesBuf);
    try std.testing.expect(std.mem.isAligned(@intFromPtr(encoded.first.buffer.ptr), 128));
    try std.testing.expectEqual(encoded.first.numElements, numRuns);
    try std.testing.expectEqual(encoded.first.bitSizePerElement, @bitSizeOf(V));
    try std.testing.expectEqual(encoded.first.inputBytesUsed, valuesOut.len * @sizeOf(V));

    try std.testing.expectEqualDeep(encoded.second.buffer, runEndsBuf);
    try std.testing.expect(std.mem.isAligned(@intFromPtr(encoded.second.buffer.ptr), 128));
    try std.testing.expectEqual(encoded.second.numElements, numRuns);
    try std.testing.expectEqual(encoded.second.bitSizePerElement, @bitSizeOf(V));
    try std.testing.expectEqual(encoded.second.inputBytesUsed, runEndsOut.len * @sizeOf(V));

    const values = [_]V{ 1, 2, 3, 4, 5 };
    try std.testing.expectEqualSlices(V, &values, valuesOut);

    const runEnds = [_]u32{ 3, 4, 5, 7, 8 };
    try std.testing.expectEqualSlices(u32, &runEnds, runEndsOut);

    const decodeOut: []align(128) V = try gpa.alignedAlloc(V, c.VORTEX_ALIGNMENT, ints.len);
    defer gpa.free(decodeOut);
    const decodeBuf = abi.ByteBuffer.initFromSlice(decodeOut);

    var decoded = abi.OneBufferResult.empty(decodeBuf);
    c.codecz_ree_decode_i32_u32(
        @ptrCast(&values),
        @ptrCast(&runEnds),
        runEnds.len,
        @ptrCast(&decoded),
    );
    try std.testing.expectEqual(decoded.status, abi.ResultStatus.Ok);

    try std.testing.expectEqualDeep(decoded.buf.buffer, decodeBuf);
    try std.testing.expect(std.mem.isAligned(@intFromPtr(decoded.buf.buffer.ptr), 128));
    try std.testing.expectEqual(decoded.buf.numElements, ints.len);
    try std.testing.expectEqual(decoded.buf.bitSizePerElement, @bitSizeOf(V));
    try std.testing.expectEqual(decoded.buf.inputBytesUsed, decodeOut.len * @sizeOf(V));
    try std.testing.expectEqualSlices(V, &ints, decodeOut);
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

    const valuesOut: []align(128) i64 = try gpa.alignedAlloc(i64, c.VORTEX_ALIGNMENT, floats.len);
    defer gpa.free(valuesOut);
    const valuesBuf = abi.ByteBuffer.initFromSlice(valuesOut);

    const bitsetOut: []align(128) u8 = try gpa.alignedAlloc(u8, c.VORTEX_ALIGNMENT, (floats.len + 7) / 8);
    defer gpa.free(bitsetOut);
    const bitsetBuf = abi.ByteBuffer.initFromSlice(bitsetOut);

    var expResult = abi.AlpExponentsResult.empty();
    c.codecz_alp_sampleFindExponents_f64(@ptrCast(&floats), floats.len, @ptrCast(&expResult));
    try std.testing.expect(expResult.status == abi.ResultStatus.Ok);
    const exponents = expResult.exponents;
    try std.testing.expectEqual(exponents.e, 8);
    try std.testing.expectEqual(exponents.f, 2);

    var encoded = abi.TwoBufferResult.empty(valuesBuf, bitsetBuf);
    c.codecz_alp_encode_f64(
        @ptrCast(&floats),
        floats.len,
        @ptrCast(&exponents),
        @ptrCast(&encoded),
    );
    try std.testing.expectEqual(encoded.status, abi.ResultStatus.Ok);

    try std.testing.expectEqualDeep(encoded.first.buffer, valuesBuf);
    try std.testing.expect(std.mem.isAligned(@intFromPtr(encoded.first.buffer.ptr), 128));
    try std.testing.expectEqual(encoded.first.bitSizePerElement, @bitSizeOf(i64));
    try std.testing.expectEqual(encoded.first.inputBytesUsed, valuesOut.len * @sizeOf(i64));
    try std.testing.expectEqual(encoded.first.numElements, floats.len);

    try std.testing.expectEqualDeep(encoded.second.buffer, bitsetBuf);
    try std.testing.expect(std.mem.isAligned(@intFromPtr(encoded.second.buffer.ptr), 128));
    try std.testing.expectEqual(encoded.second.bitSizePerElement, 1);
    try std.testing.expectEqual(encoded.second.inputBytesUsed, bitsetOut.len * @sizeOf(u8));
    try std.testing.expectEqual(encoded.second.numElements, 1); // in this case, this is num exceptions

    const values = blk: {
        var values_: [floats.len]i64 = undefined;
        const pow: f64 = @floatFromInt(std.math.pow(usize, 10, exponents.e - exponents.f));
        for (0..floats.len) |i| {
            values_[i] = @intFromFloat(@round(floats[i] * pow));
        }
        break :blk values_;
    };
    try std.testing.expectEqualSlices(i64, &values, valuesOut);

    const bitset = try bitsetBuf.bits(floats.len);
    for (0..floats.len - 1) |i| {
        try std.testing.expectEqual(bitset.get(i), 0);
    }
    try std.testing.expectEqual(bitset.get(floats.len - 1), 1);

    const decodeOut: []align(128) f64 = try gpa.alignedAlloc(f64, c.VORTEX_ALIGNMENT, floats.len);
    defer gpa.free(decodeOut);
    const decodeBuf = abi.ByteBuffer.initFromSlice(decodeOut);

    var decoded = abi.OneBufferResult.empty(decodeBuf);
    c.codecz_alp_decode_f64(
        @ptrCast(valuesOut.ptr),
        valuesOut.len,
        @ptrCast(&exponents),
        @ptrCast(&decoded),
    );
    try std.testing.expectEqual(decoded.status, abi.ResultStatus.Ok);

    try std.testing.expectEqualDeep(decoded.buf.buffer, decodeBuf);
    try std.testing.expect(std.mem.isAligned(@intFromPtr(decoded.buf.buffer.ptr), 128));
    try std.testing.expectEqual(decoded.buf.bitSizePerElement, @bitSizeOf(f64));
    try std.testing.expectEqual(decoded.buf.inputBytesUsed, decodeOut.len * @sizeOf(f64));
    try std.testing.expectEqual(decoded.buf.numElements, floats.len);
    try std.testing.expectEqualSlices(f64, floats[0 .. floats.len - 1], decodeOut[0 .. decodeOut.len - 1]);
    // last one doesn't round trip, but it's close
    try std.testing.expectApproxEqAbs(floats[floats.len - 1], decodeOut[decodeOut.len - 1], 1e-6);
}

test "fastlanes ffor encoding" {
    const gpa = std.testing.allocator;
    const ints = blk: {
        const stack = [_]i32{ 1, -2, 3, -4, 5, 100_000 };
        const aligned = try gpa.alignedAlloc(i32, c.VORTEX_ALIGNMENT, 6);
        @memcpy(aligned, &stack);
        break :blk aligned;
    };
    defer gpa.free(ints);
    const minVal = std.mem.min(i32, ints);
    const numBits = 4;

    const bytesNeeded = c.codecz_flbp_encodedSizeInBytes_u32(ints.len, numBits);
    const encodedOut: []align(128) u8 = try gpa.alignedAlloc(u8, c.VORTEX_ALIGNMENT, bytesNeeded);
    defer gpa.free(encodedOut);
    const encodedBuf = abi.ByteBuffer.initFromSlice(encodedOut);

    var encodeResult = abi.OneBufferNumExceptionsResult.empty(encodedBuf);
    c.codecz_ffor_encode_i32(
        @ptrCast(ints.ptr),
        ints.len,
        numBits,
        minVal,
        @ptrCast(&encodeResult),
    );

    try std.testing.expectEqual(encodeResult.status, abi.ResultStatus.Ok);
    try std.testing.expectEqualDeep(encodeResult.encoded.buffer, encodedBuf);
    try std.testing.expectEqual(encodeResult.encoded.numElements, bytesNeeded);
    try std.testing.expectEqual(encodeResult.encoded.bitSizePerElement, 8);
    try std.testing.expectEqual(encodeResult.encoded.inputBytesUsed, bytesNeeded);
    try std.testing.expectEqual(encodeResult.num_exceptions, 1);

    const exceptionsOut: []align(128) u32 = try gpa.alignedAlloc(
        u32,
        c.VORTEX_ALIGNMENT,
        encodeResult.num_exceptions + 1,
    );
    defer gpa.free(exceptionsOut);
    const exceptionsBuf = abi.ByteBuffer.initFromSlice(exceptionsOut);
    const bitsetOut: []align(128) u8 = try gpa.alignedAlloc(u8, c.VORTEX_ALIGNMENT, (ints.len + 7) / 8);
    defer gpa.free(bitsetOut);
    const bitsetBuf = abi.ByteBuffer.initFromSlice(bitsetOut);

    var exceptionsResult = abi.TwoBufferResult.empty(exceptionsBuf, bitsetBuf);
    c.codecz_ffor_collectExceptions_i32(
        @ptrCast(ints.ptr),
        ints.len,
        numBits,
        minVal,
        encodeResult.num_exceptions,
        @ptrCast(&exceptionsResult),
    );

    try std.testing.expectEqual(exceptionsResult.status, abi.ResultStatus.Ok);
    try std.testing.expectEqualDeep(exceptionsResult.first.buffer, exceptionsBuf);
    try std.testing.expect(std.mem.isAligned(@intFromPtr(exceptionsResult.first.buffer.ptr), 128));
    try std.testing.expectEqual(exceptionsResult.first.numElements, encodeResult.num_exceptions);
    try std.testing.expectEqual(exceptionsResult.first.bitSizePerElement, @bitSizeOf(i32));
    try std.testing.expectEqual(exceptionsResult.first.inputBytesUsed, encodeResult.num_exceptions * @sizeOf(i32));
    try std.testing.expectEqualDeep(exceptionsResult.second.buffer, bitsetBuf);
    try std.testing.expect(std.mem.isAligned(@intFromPtr(exceptionsResult.second.buffer.ptr), 128));
    try std.testing.expectEqual(exceptionsResult.second.numElements, encodeResult.num_exceptions);
    try std.testing.expectEqual(exceptionsResult.second.bitSizePerElement, @bitSizeOf(u1));
    try std.testing.expectEqual(exceptionsResult.second.inputBytesUsed, bitsetOut.len);

    try std.testing.expectEqual(exceptionsOut[0], 100_000);
    const bitset = try bitsetBuf.bits(ints.len);
    for (0..ints.len - 1) |i| {
        try std.testing.expectEqual(bitset.get(i), 0);
    }
    try std.testing.expectEqual(bitset.get(ints.len - 1), 1);

    const decodeOut: []align(128) i32 = try gpa.alignedAlloc(i32, c.VORTEX_ALIGNMENT, ints.len);
    defer gpa.free(decodeOut);
    const decodeBuf = abi.ByteBuffer.initFromSlice(decodeOut);

    var decoded = abi.OneBufferResult.empty(decodeBuf);
    c.codecz_ffor_decode_i32(
        @ptrCast(&encodedBuf),
        ints.len,
        numBits,
        minVal,
        @ptrCast(&decoded),
    );
    try std.testing.expectEqual(decoded.status, abi.ResultStatus.Ok);

    try std.testing.expectEqualDeep(decoded.buf.buffer, decodeBuf);
    try std.testing.expect(std.mem.isAligned(@intFromPtr(decoded.buf.buffer.ptr), 128));
    try std.testing.expectEqual(decoded.buf.bitSizePerElement, @bitSizeOf(i32));
    try std.testing.expectEqual(decoded.buf.inputBytesUsed, ints.len * @sizeOf(i32));
    try std.testing.expectEqual(decoded.buf.numElements, ints.len);
    try std.testing.expectEqualSlices(i32, ints[0 .. ints.len - 1], decodeOut[0 .. decodeOut.len - 1]);
    // last one doesn't round trip, gets truncated instead
    try std.testing.expectEqual(
        decodeOut[decodeOut.len - 1],
        @as(i4, @truncate(ints[ints.len - 1])),
    );
}
