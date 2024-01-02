const std = @import("std");
const Allocator = std.mem.Allocator;

// Apache Arrow is only 64 byte aligned, meaning we cannot always be zero-copy with 1024 bit vectors
// unless we convince Arrow (fork? Recompile?) to align to 128 bytes.
pub const FLWidth = 512;
pub const Alignment = 128;

pub fn vecLen(comptime V: type) comptime_int {
    return FLWidth / @bitSizeOf(V);
}

pub fn FLVec(comptime V: type) type {
    return @Vector(vecLen(V), V);
}
