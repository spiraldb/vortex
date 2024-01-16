pub usingnamespace @import("error.zig");

// the codecs themselves
pub usingnamespace @import("alp.zig");
pub usingnamespace @import("delta.zig");
pub usingnamespace @import("dictionary.zig");
pub usingnamespace @import("packedints.zig");
pub usingnamespace @import("pseudodecimal.zig");
pub usingnamespace @import("runend.zig");
pub usingnamespace @import("streamvbyte.zig");
pub usingnamespace @import("zigzag.zig");

comptime {
    // ensure that the tests run, but don't re-export in the namespace
    _ = @import("benchmarks.zig");
}
