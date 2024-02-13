const std = @import("std");
const Allocator = std.mem.Allocator;
const abi = @import("abi");

// Apache Arrow buffers are only 64 byte aligned, meaning we cannot always be zero-copy with 1024 bit vectors.
// So instead, for now, we use FastLanes vectors with 64 byte (512 bit) alignment.
pub const FLWidth = 512;
pub const InputAlignment = @alignOf(FLVec(u8));

pub fn fastLanesVecLen(comptime V: type) comptime_int {
    return FLWidth / @bitSizeOf(V);
}

pub fn FLVec(comptime V: type) type {
    return @Vector(fastLanesVecLen(V), V);
}

test "FLVec" {
    try std.testing.expectEqual(fastLanesVecLen(u8), 64);
    try std.testing.expectEqual(@typeInfo((FLVec(u8))).Vector.len, 64);
}

test "fastlanes alignment" {
    try std.testing.expect(InputAlignment >= 64);
    try std.testing.expect(std.math.isPowerOfTwo(InputAlignment));

    // this is verifying assumed zig compiler behavior
    // see https://github.com/ziglang/zig/issues/11856
    try std.testing.expectEqual(@alignOf(FLVec(u8)), @sizeOf(FLVec(u8)));

    try std.testing.expectEqual(abi.Alignment, 128);
    try std.testing.expect(abi.Alignment >= InputAlignment);
    try std.testing.expectEqual(abi.Alignment % InputAlignment, 0);
}
