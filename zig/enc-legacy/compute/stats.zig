const std = @import("std");
const enc = @import("../enc.zig");

pub fn ReturnStatsScalar(comptime stat: enc.Stats.Stat) type {
    return struct {
        pub fn getStat(ctx: enc.Ctx, array: *const enc.Array, options: *const anyopaque) !enc.Scalar {
            _ = options;
            return try array.computeStatistic(ctx.gpa, stat);
        }
    };
}
