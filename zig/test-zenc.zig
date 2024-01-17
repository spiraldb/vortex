const std = @import("std");
const builtin = @import("builtin");
const zimd = @import("zimd");
const codecz = @import("codecs");
const CodecError = codecz.CodecError;

const c = @cImport({
    @cInclude("zenc.h");
});

test "math" {
    const ints = [_]i32{ 1, 2, 3, 4, 5 };
    try std.testing.expectEqual(c.zimd_max_i32(@ptrCast(&ints), ints.len), 5);
    try std.testing.expectEqual(c.zimd_min_i32(@ptrCast(&ints), ints.len), 1);
    try std.testing.expectEqual(c.zimd_isConstant_i32(@ptrCast(&ints), ints.len), false);
    try std.testing.expectEqual(c.zimd_isSorted_i32(@ptrCast(&ints), ints.len), true);

    const runstats = c.zimd_runLengthStats_i32(@ptrCast(&ints), ints.len);
    try std.testing.expectEqual(runstats.runCount, 0);
    try std.testing.expectEqual(runstats.runElementCount, 0);
}

test "RunLengthStats_t conversion" {
    try std.testing.expectEqual(@bitSizeOf(c.RunLengthStats_t), @bitSizeOf(zimd.math.RunLengthStats));

    const cti = @typeInfo(c.RunLengthStats_t);
    const zti = @typeInfo(zimd.math.RunLengthStats);
    try std.testing.expectEqual(cti.Struct.fields.len, 2);
    try std.testing.expectEqual(cti.Struct.fields[0], zti.Struct.fields[0]);
    try std.testing.expectEqual(cti.Struct.fields[1], zti.Struct.fields[1]);
}

test "extern ree" {
    const ints = [_]i32{ 1, 1, 1, 2, 3, 4, 4, 5 };
    const numRuns = 5;

    var valuesOut: [5]i32 = [_]i32{0} ** 5;
    const valuesBuf = c.ByteBuffer_t{ .ptr = @ptrCast(&valuesOut), .len = valuesOut.len * @sizeOf(i32) };
    var runEndsOut: [5]u32 = [_]u32{0} ** 5;
    const runEndsBuf = c.ByteBuffer_t{ .ptr = @ptrCast(&runEndsOut), .len = runEndsOut.len * @sizeOf(u32) };

    const result = c.codecz_ree_encode_i32_u32(@ptrCast(&ints), ints.len, valuesBuf, runEndsBuf);

    try std.testing.expectEqual(result.status, c.Ok);
    try std.testing.expectEqualDeep(result.firstBuffer.buffer, valuesBuf);
    try std.testing.expectEqualDeep(result.secondBuffer.buffer, runEndsBuf);
    try std.testing.expectEqual(result.firstBuffer.numElements, numRuns);
    try std.testing.expectEqual(result.secondBuffer.numElements, numRuns);
    try std.testing.expectEqual(result.firstBuffer.inputBytesUsed, valuesOut.len * @sizeOf(i32));
    try std.testing.expectEqual(result.secondBuffer.inputBytesUsed, runEndsOut.len * @sizeOf(u32));

    const values = [_]i32{ 1, 2, 3, 4, 5 };
    try std.testing.expectEqualSlices(i32, &values, &valuesOut);

    const runEnds = [_]u32{ 3, 4, 5, 7, 8 };
    try std.testing.expectEqualSlices(u32, &runEnds, &runEndsOut);
}
