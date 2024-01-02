/// TableLookupBytesOr0
///
/// returns bytes[indices[i]], or 0 if indices[i] & 0x80.
///
/// Based on https://google.github.io/highway/en/master/quick_reference.html#blockwise
const builtin = @import("builtin");
const std = @import("std");
const zimd = @import("zimd.zig");

const TableLookupBytesOr0 = fn (bytes: @Vector(16, u8), indices: @Vector(16, i8)) callconv(.Inline) @Vector(16, u8);

pub fn GetTableLookupBytesOr0(comptime cpu: std.Target.Cpu) TableLookupBytesOr0 {
    if (comptime cpu.arch.isAARCH64() and std.Target.aarch64.featureSetHas(cpu.features, .neon)) {
        return Aarch64_Neon;
    }
    if (comptime cpu.arch.isX86() and std.Target.x86.featureSetHas(cpu.features, .ssse3)) {
        return X64_SSE3;
    }
    return Scalar;
}

pub const tableLookupBytesOr0 = GetTableLookupBytesOr0(builtin.cpu);

// For all vector widths; Arm anyway zeroes if >= 0x10.
inline fn Aarch64_Neon(bytes: @Vector(16, u8), indices: @Vector(16, i8)) @Vector(16, u8) {
    return asm ("tbl.16b %[ret], { %[v0] }, %[v1]"
        : [ret] "=w" (-> @Vector(16, u8)),
        : [v0] "w" (bytes),
          [v1] "w" (indices),
    );
}

inline fn X64_SSE3(bytes: @Vector(16, u8), indices: @Vector(16, i8)) @Vector(16, u8) {
    var result = bytes;
    asm volatile ("pshufb %[indices], %[bytes]"
        : [bytes] "+x" (result),
        : [indices] "x" (indices),
    );
    return result;
}

inline fn Scalar(bytes: @Vector(16, u8), indices: @Vector(16, i8)) @Vector(16, u8) {
    var result: @Vector(16, u8) = undefined;
    inline for (0..16) |i| {
        result[i] = if (indices[i] < 0) 0 else bytes[@intCast(indices[i])];
    }
    return result;
}

test "table lookup" {
    const bytes: @Vector(16, u8) = .{ 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30 };
    const indices: @Vector(16, i8) = .{ 0, 4, 8, 12, 1, 5, 9, 13, 2, 6, 10, 14, -1, -1, -1, -1 };

    const expected: @Vector(16, u8) = .{ 0, 8, 16, 24, 2, 10, 18, 26, 4, 12, 20, 28, 0, 0, 0, 0 };

    const nativeResult = tableLookupBytesOr0(bytes, indices);
    try std.testing.expectEqual(expected, nativeResult);

    const scalarResult = GetTableLookupBytesOr0(zimd.baselineCpu)(bytes, indices);
    try std.testing.expectEqual(expected, scalarResult);
}
