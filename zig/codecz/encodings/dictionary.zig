const std = @import("std");

pub fn Dictionary(comptime V: type, comptime C: type, comptime alignment: u29) type {
    return struct {
        const DictType = []align(alignment) const V;
        const CodesType = []align(alignment) const C;
        const Result = struct { dictionary: DictType, codes: CodesType };

        /// Dictionary encode values
        pub fn encode(gpa: std.mem.Allocator, elems: []const V) !Result {
            const dictionary = try unique(gpa, elems);
            const codes = try encodeUsingDict(gpa, elems, dictionary);
            return .{ .dictionary = dictionary, .codes = codes };
        }

        /// Encodes values using the dictionary, assumes the dictionary is sorted
        pub fn encodeUsingDict(gpa: std.mem.Allocator, elems: []const V, dictionary: []const V) !CodesType {
            var codes = try gpa.alignedAlloc(C, alignment, elems.len);

            for (elems, 0..) |e, idx| {
                if (std.sort.binarySearch(V, e, dictionary, {}, struct {
                    pub fn compare(ctx: void, key: V, mid_item: V) std.math.Order {
                        _ = ctx;
                        return std.math.order(key, mid_item);
                    }
                }.compare)) |c| {
                    codes[idx] = @intCast(c);
                }
            }

            return codes;
        }

        pub fn decode(gpa: std.mem.Allocator, codes: []const C, dictionary: []const V) !DictType {
            const values = try gpa.alignedAlloc(V, alignment, codes.len);

            for (codes, 0..) |c, idx| {
                values[idx] = dictionary[c];
            }

            return values;
        }

        pub fn unique(gpa: std.mem.Allocator, elems: []const V) !DictType {
            var localElems = try gpa.alignedAlloc(V, alignment, elems.len);
            @memcpy(localElems, elems);
            std.sort.pdq(V, localElems, {}, struct {
                pub fn lessThan(ctx: void, a: V, b: V) bool {
                    _ = ctx;
                    return a < b;
                }
            }.lessThan);

            var lastIdx: usize = 0;
            for (0..localElems.len) |i| {
                if (localElems[i] != localElems[lastIdx]) {
                    lastIdx += 1;
                    localElems[lastIdx] = localElems[i];
                }
            }

            if (!gpa.resize(localElems, lastIdx + 1)) {
                const res = try gpa.alignedAlloc(V, alignment, lastIdx);
                @memcpy(res, localElems[0 .. lastIdx + 1]);
                return res;
            } else {
                return localElems[0 .. lastIdx + 1];
            }
        }
    };
}

test "dictionary" {
    const gpa = std.testing.allocator;

    const values = [_]i32{ -1, -1, 2, 3, -1 };
    const encoder = Dictionary(i32, u32, 128);

    const dict = try encoder.unique(gpa, &values);
    defer gpa.free(dict);
    try std.testing.expectEqualSlices(i32, &.{ -1, 2, 3 }, dict);

    const encoded = try encoder.encodeUsingDict(gpa, &values, dict);
    defer gpa.free(encoded);
    try std.testing.expectEqualSlices(u32, &.{ 0, 0, 1, 2, 0 }, encoded);

    const encoded2 = try encoder.encode(gpa, &values);
    defer gpa.free(encoded2.dictionary);
    defer gpa.free(encoded2.codes);
    try std.testing.expectEqualSlices(u32, &.{ 0, 0, 1, 2, 0 }, encoded2.codes);

    const decoded = try encoder.decode(gpa, encoded, dict);
    defer gpa.free(decoded);
    try std.testing.expectEqualSlices(i32, &values, decoded);
}
