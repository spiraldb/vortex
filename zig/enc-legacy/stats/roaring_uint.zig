const std = @import("std");
const enc = @import("../enc.zig");

pub fn compute(array: *const enc.RoaringUIntArray, allocator: std.mem.Allocator, stat: enc.Stats.Stat) !enc.Scalar {
    _ = allocator;
    roaringIntStats(array, array.array.stats);
    return array.array.stats.get(stat) orelse enc.Error.StatisticNotSupported;
}

fn roaringIntStats(array: *const enc.RoaringUIntArray, stats: *enc.Stats) void {
    std.debug.assert(array.array.dtype == .uint);
    if (array.array.len == 0) {
        stats.put(.avg_run_length, enc.Scalar.init(0.0));
        return;
    }

    stats.put(.min, enc.Scalar.init(array.bitmap.minimum()));
    stats.put(.max, enc.Scalar.init(array.bitmap.maximum()));
    stats.put(.is_constant, enc.Scalar.init(false)); // only true if empty
    stats.put(.is_sorted, enc.Scalar.init(true)); // by definition, it is a sorted set
}
