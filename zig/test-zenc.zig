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
