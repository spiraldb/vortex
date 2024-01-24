pub usingnamespace @import("error.zig");

// the codecs themselves
pub const encodings = @import("encodings/encodings.zig");
pub const simd_math = @import("simd_math.zig");

comptime {
    _ = @import("benchmarks.zig");
    _ = @import("sampling.zig");
    _ = @import("patch.zig");
}

test {
    const std = @import("std");
    std.testing.refAllDeclsRecursive(@This());
}
