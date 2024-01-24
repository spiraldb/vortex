const std = @import("std");
const CodecError = @import("error.zig").CodecError;

pub fn patch(comptime T: type, orig: []const T, indices: anytype, out: []T) CodecError!void {
    return doPatch(T, @TypeOf(indices), orig, indices, out);
}

fn doPatch(comptime T: type, comptime IndicesType: type, orig: []const T, indices: IndicesType, out: []T) CodecError!void {
    if (out.len < orig.len) {
        return CodecError.OutputBufferTooSmall;
    }
    if (indices.capacity() > orig.len) {
        return CodecError.InvalidInput;
    }

    var iter = indices.iterator(.{});
    while (iter.next()) |idx| {
        out[idx] = orig[idx];
    }
}

test "patch" {
    var bs = try std.bit_set.DynamicBitSet.initEmpty(std.testing.allocator, 10);
    defer bs.deinit();
    bs.set(1);
    bs.set(3);

    const values = [_]u8{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    var decoded = [_]u8{ 0, 0, 2, 0, 4, 5, 6, 7, 8, 9 };
    try patch(u8, &values, bs, &decoded);
    try std.testing.expectEqualSlices(u8, &values, &decoded);
}
