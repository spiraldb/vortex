const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn RunEnd(comptime V: type, comptime E: type) type {
    return struct {
        const Result = struct { values: []const V, runends: []const E };

        pub fn encode(allocator: Allocator, elems: []const V) !Result {
            var values = std.ArrayList(V).init(allocator);
            defer values.deinit();

            var runEnds = std.ArrayList(E).init(allocator);
            defer runEnds.deinit();

            var current: V = elems[0];
            var run: E = 1;
            for (elems[1..]) |elem| {
                if (current != elem) {
                    try values.append(current);
                    try runEnds.append(run);
                    current = elem;
                }
                run += 1;
            }
            try values.append(current);
            try runEnds.append(run);

            return .{ .values = try values.toOwnedSlice(), .runends = try runEnds.toOwnedSlice() };
        }

        pub fn decode(allocator: Allocator, values: []const V, runends: []const E) ![]const V {
            const length = runends[runends.len - 1];

            var out = try allocator.alloc(V, length);

            var idx: usize = 0;
            for (values, runends) |v, e| {
                while (idx < e) {
                    out[idx] = v;
                    idx += 1;
                }
            }

            return out;
        }
    };
}
