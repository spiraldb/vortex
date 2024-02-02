const std = @import("std");
const CodecError = @import("error.zig").CodecError;

pub const CopyPatchesMixin = struct {
    pub fn patch(comptime T: type, orig: []const T, indices: anytype, out: []T) CodecError!void {
        if (out.len < orig.len) {
            return CodecError.OutputBufferTooSmall;
        }
        if (orig.len != out.len) {
            return CodecError.InvalidInput;
        }
        if (indices.capacity() > orig.len) {
            return CodecError.InvalidInput;
        }

        var iter = indices.iterator(.{});
        while (iter.next()) |idx| {
            out[idx] = orig[idx];
        }
    }
};

pub const ScatterPatchesMixin = struct {
    pub fn patch(comptime T: type, indices: anytype, patches: []const T, out: []T) CodecError!void {
        if (indices.capacity() > out.len) {
            return CodecError.InvalidInput;
        }

        var iter = indices.iterator(.{});
        var i: usize = 0;
        while (iter.next()) |idx| : (i += 1) {
            out[idx] = patches[i];
        }
        if (i != patches.len) {
            return CodecError.InvalidInput;
        }
    }
};

pub fn toPackedSlice(bitSet: anytype) std.PackedIntSlice(u1) {
    const numMasks = std.math.divCeil(usize, bitSet.capacity(), @bitSizeOf(@TypeOf(bitSet).MaskInt)) catch unreachable;
    return std.PackedIntSlice(u1).init(
        std.mem.sliceAsBytes(bitSet.masks[0..numMasks]),
        bitSet.capacity(),
    );
}

test "copy patches" {
    var bs = try std.bit_set.DynamicBitSet.initEmpty(std.testing.allocator, 10);
    defer bs.deinit();
    bs.set(1);
    bs.set(3);

    const values = [_]u8{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    var decoded = [_]u8{ 0, 0, 2, 0, 4, 5, 6, 7, 8, 9 };
    try CopyPatchesMixin.patch(u8, &values, bs, &decoded);
    try std.testing.expectEqualSlices(u8, &values, &decoded);
}

test "scatter patches" {
    var bs = try std.bit_set.DynamicBitSet.initEmpty(std.testing.allocator, 10);
    defer bs.deinit();
    bs.set(1);
    bs.set(3);

    const values = [_]u8{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    const patches = [_]u8{ 1, 3 };
    var decoded = [_]u8{ 0, 0, 2, 0, 4, 5, 6, 7, 8, 9 };
    try ScatterPatchesMixin.patch(u8, bs, &patches, &decoded);
    try std.testing.expectEqualSlices(u8, &values, &decoded);
}
