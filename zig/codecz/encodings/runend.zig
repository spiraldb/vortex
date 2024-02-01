const std = @import("std");
const Allocator = std.mem.Allocator;
const CodecError = @import("../error.zig").CodecError;

pub fn RunEnd(comptime V: type, comptime E: type, comptime A: u29) type {
    return struct {
        pub fn valuesBufferSizeInBytes(maxNumRuns: usize) usize {
            return maxNumRuns * @sizeOf(V);
        }

        pub fn runEndsBufferSize(maxNumRuns: usize) usize {
            return maxNumRuns * @sizeOf(E);
        }

        pub fn encode(elems: []const V, outValues: []align(A) V, outRunEnds: []align(A) E) CodecError!usize {
            if (elems.len == 0) {
                return CodecError.InvalidInput;
            }

            const maxNumRuns: usize = @min(outValues.len, outRunEnds.len);
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

                outValues[runCount] = current;
                outRunEnds[runCount] = @intCast(i);

                runCount += 1;
                current = elem;
            }

            std.debug.assert(runCount < maxNumRuns);
            outValues[runCount] = current;
            outRunEnds[runCount] = @intCast(elems.len);
            runCount += 1;

            return runCount;
        }

        pub fn decode(values: []const V, runEnds: []const E, out: []align(A) V) CodecError!void {
            if (values.len != runEnds.len) {
                return CodecError.InvalidInput;
            }
            if (numDecodedElements(runEnds) > out.len) {
                return CodecError.OutputBufferTooSmall;
            }

            var prevEnd: E = 0;
            for (values, runEnds) |v, end| {
                @memset(out[prevEnd..end], v);
                prevEnd = end;
            }
        }

        pub fn numDecodedElements(runEnds: []const E) usize {
            if (runEnds.len == 0) {
                return 0;
            }
            return runEnds[runEnds.len - 1];
        }
    };
}
