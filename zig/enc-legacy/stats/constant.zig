const std = @import("std");
const enc = @import("../enc.zig");

pub fn compute(array: *const enc.ConstantArray, allocator: std.mem.Allocator, stat: enc.Stats.Stat) !enc.Scalar {
    return switch (stat) {
        .min, .max => array.scalar.clone(allocator),
        .is_constant, .is_sorted => enc.Scalar.init(true),
        .avg_run_length => enc.Scalar.init(array.array.len),
        .true_count => switch (array.scalar) {
            .bool => |b| enc.Scalar.init(array.array.len * @intFromBool(b.value)),
            else => enc.Error.StatisticNotSupported,
        },
        else => enc.Error.StatisticNotSupported,
    };
}
