const std = @import("std");
const enc = @import("../enc.zig");

// TODO(robert): Compute bit_width_frequency? It's probably irrelevant since it's only useful for the child arrays.
pub fn compute(array: *const enc.DictionaryArray, allocator: std.mem.Allocator, stat: enc.Stats.Stat) !enc.Scalar {
    switch (stat) {
        .min, .max, .is_constant, .is_sorted, .avg_run_length => {},
        else => return enc.Error.StatisticNotSupported,
    }

    const stats = array.array.stats;
    const dictionary = array.dictionary;
    const codes = array.codes;
    stats.put(.min, try dictionary.getScalar(allocator, (try codes.computeStatistic(allocator, .min)).as(usize)));
    stats.put(.max, try dictionary.getScalar(allocator, (try codes.computeStatistic(allocator, .max)).as(usize)));
    stats.put(.is_constant, try codes.computeStatistic(allocator, .is_constant));
    stats.put(.is_sorted, try codes.computeStatistic(allocator, .is_sorted));
    stats.put(.avg_run_length, try codes.computeStatistic(allocator, .avg_run_length));

    return array.array.stats.get(stat) orelse enc.Error.StatisticNotSupported;
}
