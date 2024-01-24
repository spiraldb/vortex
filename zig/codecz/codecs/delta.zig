const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn Delta(comptime V: type) type {
    return struct {
        pub fn encode(allocator: Allocator, elems: []const V) ![]const V {
            var deltas = std.ArrayList(V).init(allocator);
            defer deltas.deinit();

            var current = elems[0];
            try deltas.append(current);

            for (elems[1..]) |elem| {
                try deltas.append(elem - current);
                current = elem;
            }

            return try deltas.toOwnedSlice();
        }

        pub fn decode(allocator: Allocator, deltas: []const V) ![]const V {
            var out = try allocator.alloc(V, deltas.len);

            out[0] = deltas[0];
            for (deltas[1..], 1..) |d, i| {
                out[i] = out[i - 1] + d;
            }

            return out;
        }
    };
}

const testing = std.testing;

test "delta" {
    const ally = testing.allocator;

    const values = [_]i32{ -1, -1, 2, 3, -1 };
    const encoder = Delta(i32);

    const deltas = try encoder.encode(ally, &values);
    defer ally.free(deltas);
    try testing.expectEqualSlices(i32, &.{ -1, 0, 3, 1, -4 }, deltas);

    const decoded = try encoder.decode(ally, deltas);
    defer ally.free(decoded);
    try testing.expectEqualSlices(i32, &values, decoded);
}
