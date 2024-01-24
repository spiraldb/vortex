const std = @import("std");
const Allocator = std.mem.Allocator;
const CodecError = @import("../error.zig").CodecError;

pub fn RunEnd(comptime V: type, comptime E: type, comptime A: u29) type {
    return struct {
        pub const Encoded = struct {
            const Self = @This();
            values: []align(A) const V,
            runends: []align(A) const E,
            numRuns: usize = 0,

            pub fn numElements(self: *const Self) usize {
                if (self.runends.len == 0) {
                    return 0;
                }
                return self.runends[self.runends.len - 1];
            }
        };

        pub fn valuesBufferSizeInBytes(maxNumRuns: usize) usize {
            return maxNumRuns * @sizeOf(V);
        }

        pub fn runEndsBufferSize(maxNumRuns: usize) usize {
            return maxNumRuns * @sizeOf(E);
        }

        pub fn encode(elems: []const V, values: []align(A) V, runends: []align(A) E) CodecError!Encoded {
            if (elems.len == 0) {
                return CodecError.InvalidInput;
            }

            const maxNumRuns: usize = @min(values.len, runends.len);
            if (maxNumRuns == 0) {
                return CodecError.OutputBufferTooSmall;
            }

            var current: V = elems[0];
            var runCount: usize = 0;
            for (elems[1..], 1..) |elem, i| {
                if (elem == current) {
                    continue;
                } else if (runCount + 1 == maxNumRuns) {
                    // this isn't the last run, but it will fill the last slot, so bail "early"
                    return CodecError.OutputBufferTooSmall;
                }

                values[runCount] = current;
                runends[runCount] = @intCast(i);

                runCount += 1;
                current = elem;
            }

            std.debug.assert(runCount < maxNumRuns);
            values[runCount] = current;
            runends[runCount] = @intCast(elems.len);
            runCount += 1;

            return Encoded{ .values = values[0..runCount], .runends = runends[0..runCount], .numRuns = runCount };
        }

        pub fn decode(encoded: Encoded, out: []align(A) V) CodecError!void {
            if (encoded.runends.len < encoded.numRuns or encoded.values.len < encoded.numRuns) {
                return CodecError.InvalidInput;
            }
            if (encoded.numElements() > out.len) {
                return CodecError.OutputBufferTooSmall;
            }

            const values = encoded.values[0..encoded.numRuns];
            const runends = encoded.runends[0..encoded.numRuns];
            var prevEnd: E = 0;
            for (values, runends) |v, end| {
                @memset(out[prevEnd..end], v);
                prevEnd = end;
            }
        }
    };
}
