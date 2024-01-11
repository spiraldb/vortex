const std = @import("std");
const enc = @import("../enc.zig");

pub fn compute(array: *const enc.PrimitiveArray, allocator: std.mem.Allocator, stat: enc.Stats.Stat) !enc.Scalar {
    switch (stat) {
        .min, .max, .is_constant, .is_sorted, .avg_run_length, .bit_width_freq => {},
        else => return enc.Error.StatisticNotSupported,
    }
    switch (array.ptype) {
        inline else => |p| try numericStats(p.astype(), array.asSlice(p.astype()), allocator, array.array.stats),
    }
    return array.array.stats.get(stat) orelse enc.Error.StatisticNotSupported;
}

fn numericStats(comptime T: type, elems: []const T, allocator: std.mem.Allocator, stats: *enc.Stats) !void {
    const typeInfo = @typeInfo(T);

    var is_sorted = true;
    var is_constant = true;
    var lastValue: T = elems[0];
    var runs: u64 = 0;
    var min = elems[0];
    var max = elems[0];
    var bit_width_freq = [_]u64{0} ** (@bitSizeOf(T) + 1);

    for (elems) |elem| {
        if (elem != lastValue) {
            is_constant = false;
            if (is_sorted and elem < lastValue) {
                is_sorted = false;
            }
            lastValue = elem;
            runs += 1;
        }

        min = @min(min, elem);
        max = @max(max, elem);

        if (typeInfo == .Int) {
            bit_width_freq[@bitSizeOf(T) - @clz(elem)] += 1;
        }
    }
    runs += 1;

    const avg_run_length = @as(f32, @floatFromInt(elems.len)) / @as(f32, @floatFromInt(runs));

    stats.put(.min, enc.Scalar.init(min));
    stats.put(.max, enc.Scalar.init(max));
    stats.put(.is_constant, enc.Scalar.init(is_constant));
    stats.put(.is_sorted, enc.Scalar.init(is_sorted));
    stats.put(.avg_run_length, enc.Scalar.init(avg_run_length));
    if (typeInfo == .Int) {
        stats.put(.bit_width_freq, try enc.Scalar.initComplex(allocator, &bit_width_freq));
    }
}
