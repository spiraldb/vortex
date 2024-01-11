const std = @import("std");
const enc = @import("../enc.zig");

pub fn compute(array: *const enc.BinaryArray, allocator: std.mem.Allocator, stat: enc.Stats.Stat) !enc.Scalar {
    switch (stat) {
        .min, .max, .is_constant, .is_sorted, .avg_run_length => {},
        else => return enc.Error.StatisticNotSupported,
    }

    var min: []const u8 = undefined;
    var max: []const u8 = undefined;
    var is_sorted = true;
    var is_constant = true;
    var lastValue: []const u8 = undefined;
    var runs: u64 = 0;
    for (array.viewsSlice(), 0..) |view, i| {
        switch (view) {
            .inlined => |ib| {
                min = if (std.mem.order(u8, &ib.data, min) == .lt) &ib.data else min;
                max = if (std.mem.order(u8, &ib.data, max) == .gt) &ib.data else max;
                switch (std.mem.order(u8, &ib.data, lastValue)) {
                    .eq => continue,
                    .lt => {},
                    .gt => {
                        is_sorted = false;
                    },
                }
                is_constant = false;
                lastValue = &ib.data;
                runs += 1;
            },
            .ref => {
                const dataBytes = (try array.array.getScalar(allocator, i)).binary.bytes;
                min = if (std.mem.order(u8, dataBytes, min) == .lt) dataBytes else min;
                max = if (std.mem.order(u8, dataBytes, max) == .gt) dataBytes else max;
                switch (std.mem.order(u8, dataBytes, lastValue)) {
                    .eq => continue,
                    .lt => {},
                    .gt => {
                        is_sorted = false;
                    },
                }
                is_constant = false;
                lastValue = dataBytes;
                runs += 1;
            },
        }
    }

    runs += 1;

    const avg_run_length = @as(f32, @floatFromInt(array.viewsSlice().len)) / @as(f32, @floatFromInt(runs));
    const stats = array.array.stats;
    stats.put(.min, try enc.Scalar.initComplex(allocator, min));
    stats.put(.max, try enc.Scalar.initComplex(allocator, max));
    stats.put(.is_constant, enc.Scalar.init(is_constant));
    stats.put(.is_sorted, enc.Scalar.init(is_sorted));
    stats.put(.avg_run_length, enc.Scalar.init(avg_run_length));
    return array.array.stats.get(stat) orelse enc.Error.StatisticNotSupported;
}
