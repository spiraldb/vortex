const std = @import("std");
const enc = @import("../enc.zig");

pub fn compute(array: *const enc.RoaringBoolArray, allocator: std.mem.Allocator, stat: enc.Stats.Stat) !enc.Scalar {
    _ = allocator;
    roaringBoolStats(array, array.array.stats);
    return array.array.stats.get(stat) orelse enc.Error.StatisticNotSupported;
}

fn roaringBoolStats(array: *const enc.RoaringBoolArray, stats: *enc.Stats) void {
    std.debug.assert(array.array.dtype == .bool);
    if (array.array.len == 0) {
        stats.put(.true_count, enc.Scalar.init(0));
        stats.put(.avg_run_length, enc.Scalar.init(0.0));
        return;
    }

    const cardinality = array.bitmap.cardinality();
    stats.put(.true_count, enc.Scalar.init(cardinality));
    stats.put(.min, enc.Scalar.init(cardinality == array.array.len));
    stats.put(.max, enc.Scalar.init(cardinality > 0));
    stats.put(.is_constant, enc.Scalar.init(cardinality == array.array.len or cardinality == 0));
}
