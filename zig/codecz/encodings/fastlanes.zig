const std = @import("std");
const Allocator = std.mem.Allocator;
const abi = @import("abi");

// Apache Arrow is only 64 byte aligned, meaning we cannot always be zero-copy with 1024 bit vectors
// unless we convince Arrow (fork? Recompile?) to align to 128 bytes.
// So instead, for now, we use FastLanes vectors with 64 byte alignment, and require only 64 byte
// alignment for *inputs* to FastLanes codecs.
pub const FLMinAlign = abi.FastLanesMinAlignment;
pub const FLWidth = FLMinAlign * @bitSizeOf(u8);

pub fn fastLanesVecLen(comptime V: type) comptime_int {
    return FLWidth / @bitSizeOf(V);
}

pub fn FLVec(comptime V: type) type {
    return @Vector(fastLanesVecLen(V), V);
}

test "FLVec" {
    try std.testing.expectEqual(fastLanesVecLen(u8), 64);
    try std.testing.expectEqual(FLMinAlign, 64);
    try std.testing.expectEqual(abi.Alignment, 128);
    try std.testing.expectEqual(abi.Alignment % FLMinAlign, 0);
}
