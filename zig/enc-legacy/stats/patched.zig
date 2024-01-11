const std = @import("std");
const enc = @import("../enc.zig");

pub fn compute(array: *const enc.PatchedArray, allocator: std.mem.Allocator, stat: enc.Stats.Stat) !enc.Scalar {
    const ptype = array.base.dtype.toPType() orelse return enc.Error.StatisticNotSupported;
    switch (stat) {
        .min, .max, .is_constant => {},
        else => return enc.Error.StatisticNotSupported,
    }
    switch (ptype) {
        inline else => |p| {
            const T = p.astype();
            const baseMin = (try array.base.computeStatistic(allocator, .min)).as(T);
            const patchMin = (try array.patchValues.computeStatistic(allocator, .min)).as(T);
            array.array.stats.put(.min, enc.Scalar.init(@min(baseMin, patchMin)));

            const baseMax = (try array.base.computeStatistic(allocator, .max)).as(T);
            const patchMax = (try array.patchValues.computeStatistic(allocator, .max)).as(T);
            array.array.stats.put(.max, enc.Scalar.init(@max(baseMax, patchMax)));

            const baseIsConstant = (try array.base.computeStatistic(allocator, .is_constant)).as(bool);
            const patchIsConstant = (try array.patchValues.computeStatistic(allocator, .is_constant)).as(bool);
            array.array.stats.put(.is_constant, enc.Scalar.init(baseIsConstant and patchIsConstant and baseMin == patchMin));
        },
    }
    return array.array.stats.get(stat) orelse return enc.Error.StatisticNotSupported;
}
