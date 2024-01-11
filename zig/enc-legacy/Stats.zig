const std = @import("std");
const enc = @import("./enc.zig");

const Self = @This();

pub const Stat = enum {
    min,
    max,
    is_constant,
    is_sorted,
    avg_run_length, // TODO(ngates): change to run_count
    bit_width_freq,
    true_count,
};

const StatsMap = std.EnumMap(enc.Stats.Stat, enc.Scalar);

values: StatsMap = StatsMap{},

pub fn deinit(self: *Self) void {
    var iter = self.values.iterator();
    while (iter.next()) |entry| {
        entry.value.deinit();
    }
}

pub fn get(self: Self, stat: Stat) ?enc.Scalar {
    return self.values.get(stat);
}

pub fn put(self: *Self, stat: Stat, scalar: enc.Scalar) void {
    self.values.put(stat, scalar);
}
