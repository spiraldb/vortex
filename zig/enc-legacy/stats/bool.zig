const std = @import("std");
const enc = @import("../enc.zig");

pub fn compute(array: *const enc.BoolArray, allocator: std.mem.Allocator, stat: enc.Stats.Stat) !enc.Scalar {
    _ = allocator;
    switch (stat) {
        .min, .max, .avg_run_length, .true_count => {},
        else => return enc.Error.StatisticNotSupported,
    }
    boolStats(array, array.array.stats);
    return array.array.stats.get(stat) orelse enc.Error.StatisticNotSupported;
}

fn boolStats(array: *const enc.BoolArray, stats: *enc.Stats) void {
    if (array.array.len == 0) {
        stats.put(.true_count, enc.Scalar.init(0));
        stats.put(.avg_run_length, enc.Scalar.init(0.0));
        return;
    }

    // TOOD(ngates): loop over byte at-a-time, and verify that LLDB auto-vectorizes.
    const bits = array.asSlice();

    var prev_bit = bits.get(0);

    var true_count: u64 = if (prev_bit == 1) 1 else 0;
    var run_count: u64 = 0;

    for (1..bits.len) |i| {
        const bit = bits.get(i);
        if (bit == 1) true_count += 1;
        if (bit != prev_bit) run_count += 1;
        prev_bit = bit;
    }
    run_count += 1;

    stats.put(.true_count, enc.Scalar.init(true_count));
    stats.put(.min, enc.Scalar.init(true_count == array.array.len));
    stats.put(.max, enc.Scalar.init(true_count > 0));
    stats.put(
        .avg_run_length,
        enc.Scalar.init(@as(f32, @floatFromInt(array.array.len)) / @as(f32, @floatFromInt(run_count))),
    );
}
