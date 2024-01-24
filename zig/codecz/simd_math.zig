const std = @import("std");
const Allocator = std.mem.Allocator;
const abi = @import("abi");

fn vecLen(comptime E: type) u16 {
    // force vectorization of at least 8 elements for now
    return @max(2048 / @bitSizeOf(E), 8);
}

fn VecType(comptime E: type) type {
    return @Vector(vecLen(E), E);
}

pub fn min(comptime E: type, elems: []const E) E {
    return vecReduce(E, .Min, maxVal(E), elems);
}

pub fn max(comptime E: type, elems: []const E) E {
    return vecReduce(E, .Max, minVal(E), elems);
}

inline fn vecReduce(comptime E: type, comptime op: std.builtin.ReduceOp, comptime defaultVal: E, elems: []const E) E {
    const vlen: u16 = comptime vecLen(E);
    if (comptime vlen < 2) {
        return scalarReduce(op, E, elems);
    }
    if (elems.len == 0) {
        return defaultVal;
    }

    const numBatches = elems.len / vlen;
    const remainderResult: E = if (elems.len % vlen == 0) defaultVal else scalarReduce(op, E, elems[numBatches * vlen ..]);
    const vectorizedResult: E = blk: {
        if (numBatches == 0) {
            break :blk defaultVal;
        }
        const batches: []const VecType(E) = @alignCast(std.mem.bytesAsSlice(VecType(E), std.mem.sliceAsBytes(elems[0 .. numBatches * vlen])));
        var resultVec: VecType(E) = batches[0];
        for (batches[1..]) |batch| {
            resultVec = applyReduceOp(op, VecType(E), resultVec, batch);
        }
        break :blk @reduce(op, resultVec);
    };

    // don't forget the tail of scalars
    return applyReduceOp(op, E, remainderResult, vectorizedResult);
}

inline fn maxVal(comptime E: type) E {
    return switch (@typeInfo(E)) {
        .Int => std.math.maxInt(E),
        .Float => std.math.inf(E),
        else => @compileError("Max val is only available for integer and float types, found " ++ @typeName(E)),
    };
}

inline fn minVal(comptime E: type) E {
    return switch (@typeInfo(E)) {
        .Int => std.math.minInt(E),
        .Float => -std.math.inf(E),
        else => @compileError("Min val is only available for integer and float types, found " ++ @typeName(E)),
    };
}

inline fn applyReduceOp(comptime op: std.builtin.ReduceOp, comptime E: type, v1: E, v2: E) E {
    return switch (op) {
        .Min => @min(v1, v2),
        .Max => @max(v1, v2),
        inline else => @compileError("unsupported op " ++ op),
    };
}

inline fn scalarReduce(comptime op: std.builtin.ReduceOp, comptime E: type, elems: []const E) E {
    return switch (op) {
        .Min => std.mem.min(E, elems),
        .Max => std.mem.max(E, elems),
        inline else => @compileError("unsupported op " ++ op),
    };
}

pub fn isSorted(comptime E: type, elems: []const E) bool {
    return pairwise(.sorted, E, elems);
}

pub fn isConstant(comptime E: type, elems: []const E) bool {
    return pairwise(.constant, E, elems);
}

const PairwiseOp = enum { sorted, constant };

inline fn pairwise(comptime op: PairwiseOp, comptime E: type, elems: []const E) bool {
    const vlen: u16 = comptime vecLen(E);
    if (comptime vlen < 2) {
        return scalarPairwise(op, E, elems);
    }

    if (elems.len <= 1) {
        return true;
    }

    const numBatches = (elems.len - 1) / vlen;

    // first check the "tail" that doesn't fit in vectors; if it's not sorted, bail early
    const remIsSorted = if ((elems.len - 1) % vlen == 0) true else scalarPairwise(op, E, elems[numBatches * vlen ..]);
    if (!remIsSorted) {
        return false;
    }

    // it was a small input, so there are no vector batches to process
    if (numBatches == 0) {
        return true;
    }

    // we bulk compare each element to its next element
    for (0..numBatches) |i| {
        const left: VecType(E) = elems[i * vlen ..][0..vlen].*;
        const right: VecType(E) = elems[i * vlen ..][1 .. vlen + 1].*;

        if (comptime op == .sorted) {
            if (!@reduce(.And, left <= right)) {
                return false;
            }
        }
        if (comptime op == .constant) {
            if (!@reduce(.And, left == right)) {
                return false;
            }
        }
    }
    return true;
}

fn scalarIsSorted(comptime E: type, elems: []const E) bool {
    return scalarPairwise(.sorted, E, elems);
}

fn scalarIsConstant(comptime E: type, elems: []const E) bool {
    return scalarPairwise(.constant, E, elems);
}

inline fn scalarPairwise(comptime op: PairwiseOp, comptime E: type, elems: []const E) bool {
    var i: usize = 1;
    while (i < elems.len) : (i += 1) {
        if (comptime op == .sorted) {
            if (elems[i] < elems[i - 1]) {
                return false;
            }
        } else if (comptime op == .constant) {
            if (elems[i] != elems[i - 1]) {
                return false;
            }
        } else {
            @compileError("unknown pairwise comparison operation " ++ op);
        }
    }
    return true;
}

pub const RunLengthStats = abi.RunLengthStats;

pub fn runLengthStats(comptime E: type, elems: []const E) RunLengthStats {
    const vlen: u16 = comptime vecLen(E);
    if (comptime vlen < 4) {
        return scalarRunStats(E, elems);
    }
    if (elems.len < 2) {
        return RunLengthStats{ .runCount = elems.len, .runElementCount = elems.len };
    }

    const CountIntType = std.simd.VectorCount(VecType(E));
    const SumIntType = std.math.IntFittingRange(0, maxSumOfEveryOtherIndex(vlen + 1));
    const U1Vec = @Vector(vlen, u1);
    const BoolVec = @Vector(vlen, bool);

    const ones: @Vector(vlen, u1) = @splat(1);
    const zeroes: @Vector(vlen, u1) = @splat(0);
    const iota = std.simd.iota(CountIntType, vlen);

    const numBatches = (elems.len - 1) / vlen;
    var firstElementCanBeStartOfRun = true;
    var runCount: usize = 0;
    var runElementCount: i128 = 0;
    for (0..numBatches) |i| {
        const left: VecType(E) = elems[i * vlen ..][0..vlen].*;
        const right: VecType(E) = elems[i * vlen ..][1 .. vlen + 1].*;

        const eqNext: BoolVec = left == right;
        const eqPrev: BoolVec = std.simd.shiftElementsRight(eqNext, 1, !firstElementCanBeStartOfRun);

        const neqPrev: U1Vec = ~@as(U1Vec, @bitCast(eqPrev));
        const isRunStart: BoolVec = @bitCast(@as(U1Vec, @bitCast(eqNext)) & neqPrev);
        const numRunStarts = @reduce(.Add, @select(CountIntType, isRunStart, ones, zeroes));
        const sumRunStartLocalIndices = @reduce(.Add, @select(SumIntType, isRunStart, iota, zeroes));

        const neqNext: U1Vec = ~@as(U1Vec, @bitCast(eqNext));
        const isRunEndInclusive: BoolVec = @bitCast(@as(U1Vec, @bitCast(eqPrev)) & neqNext);
        const numRunEnds = @reduce(.Add, @select(CountIntType, isRunEndInclusive, ones, zeroes));
        const sumRunEndLocalIndices = @reduce(.Add, @select(SumIntType, isRunEndInclusive, iota, zeroes)) + numRunEnds;

        // the essential insight here is that for every run, we add (runEnd - runStart) elements
        // but for the total, that's equivalent to sum(allRunEnds) - sum(allRunStarts) because of commutativity
        runElementCount += sumRunEndLocalIndices;
        runElementCount -= sumRunStartLocalIndices;
        runElementCount += (@as(i128, numRunEnds) - numRunStarts) * i * vlen;

        runCount += numRunStarts;
        firstElementCanBeStartOfRun = !eqNext[vlen - 1];
    }
    // if last batch ended with an ongoing run, make sure we count that last batch element as a "run end" (inclusive)
    // for the purpose of run elements counting
    runElementCount += numBatches * vlen * @intFromBool(!firstElementCanBeStartOfRun);

    if ((elems.len - 1) % vlen > 0) {
        const scalarStats = doScalarRunStats(E, elems[numBatches * vlen ..], firstElementCanBeStartOfRun);
        runCount += scalarStats.runCount;
        runElementCount += scalarStats.runElementCount;
    }
    return RunLengthStats{ .runCount = runCount, .runElementCount = @intCast(runElementCount) };
}

fn maxSumOfEveryOtherIndex(comptime vlen: comptime_int) comptime_int {
    if (vlen % 2 == 0) {
        // if even, the sum of all even numbers in 0 to vlen (inclusive) is greater than sum of odds
        // general formula for sum of evens: (N / 2) * (N / 2 + 1), where division is integral (i.e., divFloor)
        const half: comptime_int = vlen / 2; // of course, this is exact because we know vlen is even
        return half * (half + 1);
    } else {
        // if odd, sum of all odd numbers in 0 to vlen (inclusive) is greater than sum of evens
        // general formula for sum of odds: ((N + 1) / 2)^2, where division is integral (i.e., divFloor)
        const plusOneHalved: comptime_int = (vlen + 1) / 2; // but again, of course, we know that (vlen + 1) is even
        return plusOneHalved * plusOneHalved;
    }
}

fn scalarRunStats(comptime E: type, elems: []const E) RunLengthStats {
    return doScalarRunStats(E, elems, true);
}

fn doScalarRunStats(comptime E: type, elems: []const E, firstElementCanBeStartOfRun: bool) RunLengthStats {
    if (elems.len <= 1) {
        return RunLengthStats{ .runCount = elems.len, .runElementCount = elems.len };
    }

    var numRuns: usize = 0;
    var numRunElements: usize = 0;
    var isRun = !firstElementCanBeStartOfRun;
    for (0..elems.len - 1) |i| {
        const eqNext = elems[i] == elems[i + 1];
        numRuns += @intFromBool(!isRun and eqNext); // run start
        numRunElements += @intFromBool(isRun or eqNext);
        isRun = eqNext;
    }
    numRunElements += @intFromBool(isRun); // don't forget the last element
    return RunLengthStats{ .runCount = numRuns, .runElementCount = numRunElements };
}

test "small vector operations" {
    const T = i64;
    const elems = [_]T{ 0, -1, 1 };
    try std.testing.expect(vecLen(T) > elems.len);
    try std.testing.expectEqual(min(T, &elems), -1);
    try std.testing.expectEqual(max(T, &elems), 1);

    const sorted = [_]T{ -1, 0, 1 };
    const constant = [_]T{ 1, 1, 1 };

    try std.testing.expect(!isSorted(T, &elems));
    try std.testing.expect(isSorted(T, &sorted));
    try std.testing.expect(isSorted(T, &constant));

    try std.testing.expect(!isConstant(T, &elems));
    try std.testing.expect(!isConstant(T, &sorted));
    try std.testing.expect(isConstant(T, &constant));

    try std.testing.expectEqual(RunLengthStats{ .runCount = 0, .runElementCount = 0 }, runLengthStats(T, &elems));
    try std.testing.expectEqual(RunLengthStats{ .runCount = 0, .runElementCount = 0 }, runLengthStats(T, &sorted));
    try std.testing.expectEqual(RunLengthStats{ .runCount = 1, .runElementCount = 3 }, runLengthStats(T, &constant));
}

const IntTypesToTest = [_]type{ u8, i16, i32, u64, i128 };

test "benchmark vectorized min" {
    inline for (IntTypesToTest) |T| {
        const values = try generate_random_array(T, 1_000_000, std.testing.allocator);
        defer std.testing.allocator.free(values);
        try run_vector_math_integer_benchmark(
            "MIN",
            T,
            ?T,
            vecLen(T),
            min,
            std.mem.min,
            values,
        );
    }
}

test "benchmark vectorized max" {
    inline for (IntTypesToTest) |T| {
        const values = try generate_random_array(T, 1_000_000, std.testing.allocator);
        defer std.testing.allocator.free(values);
        try run_vector_math_integer_benchmark(
            "MAX",
            T,
            ?T,
            vecLen(T),
            max,
            std.mem.max,
            values,
        );
    }
}

test "benchmark vectorized isSorted false" {
    inline for (IntTypesToTest) |T| {
        const vlen = vecLen(T);
        var values = try std.testing.allocator.alloc(T, 10_000_000);
        defer std.testing.allocator.free(values);
        for (values, 0..) |*v, i| {
            v.* = std.math.lossyCast(T, i);
        }
        values[values.len - vlen - 1] = 0;
        try std.testing.expect(!isSorted(T, values));
        try run_vector_math_integer_benchmark(
            "IS_SORTED_FALSE",
            T,
            bool,
            vecLen(T),
            isSorted,
            scalarIsSorted,
            values,
        );
    }
}

test "benchmark vectorized isSorted true" {
    inline for (IntTypesToTest) |T| {
        const values = try std.testing.allocator.alloc(T, 10_000_000);
        defer std.testing.allocator.free(values);
        for (values, 0..) |*v, i| {
            v.* = std.math.lossyCast(T, i);
        }
        try std.testing.expect(isSorted(T, values));
        try run_vector_math_integer_benchmark(
            "IS_SORTED_TRUE",
            T,
            bool,
            vecLen(T),
            isSorted,
            scalarIsSorted,
            values,
        );
    }
}

test "benchmark vectorized isConstant false" {
    inline for (IntTypesToTest) |T| {
        const vlen = vecLen(T);
        var values = try std.testing.allocator.alloc(T, 10_000_000);
        defer std.testing.allocator.free(values);
        @memset(values, 100);
        values[values.len - vlen - 1] = 0;
        try std.testing.expect(!isConstant(T, values));
        try std.testing.expect(!isSorted(T, values));
        try run_vector_math_integer_benchmark(
            "IS_CONSTANT_FALSE",
            T,
            bool,
            vecLen(T),
            isConstant,
            scalarIsConstant,
            values,
        );
    }
}

test "benchmark vectorized isConstant true" {
    inline for (IntTypesToTest) |T| {
        const values = try std.testing.allocator.alloc(T, 10_000_000);
        defer std.testing.allocator.free(values);
        @memset(values, 100);
        try std.testing.expect(isConstant(T, values));
        try std.testing.expect(isSorted(T, values));
        try run_vector_math_integer_benchmark(
            "IS_CONSTANT_TRUE",
            T,
            bool,
            vecLen(T),
            isConstant,
            scalarIsConstant,
            values,
        );
    }
}

test "basic runLengthStats" {
    const T = u8;
    const values = try std.testing.allocator.alloc(T, 10_000);
    defer std.testing.allocator.free(values);
    for (values, 0..) |*v, i| {
        v.* = std.math.lossyCast(T, i / 1_000);
    }
    try std.testing.expectEqual(RunLengthStats{ .runCount = 10, .runElementCount = values.len }, runLengthStats(T, values));
}

test "benchmark vectorized runLengthStats" {
    inline for (IntTypesToTest) |T| {
        const values = try generate_random_array(T, 10_000_000, std.testing.allocator);
        defer std.testing.allocator.free(values);

        try run_vector_math_integer_benchmark(
            "RUN_LENGTHS",
            T,
            RunLengthStats,
            vecLen(T),
            runLengthStats,
            scalarRunStats,
            values,
        );
    }
}

fn generate_random_array(comptime T: type, N: usize, ally: Allocator) ![]T {
    var R = std.rand.DefaultPrng.init(42); // a very deterministic but meaningful universe
    var rand = R.random();
    var values: []T = try ally.alloc(T, N);
    errdefer ally.free(values);
    for (0..N) |i| {
        values[i] = rand.int(T);
    }
    return values;
}

fn run_vector_math_integer_benchmark(
    comptime name: []const u8,
    comptime T: type,
    comptime RT: type,
    comptime vlen: ?u16,
    comptime vec_fn: fn (comptime T: type, elems: []const T) RT,
    comptime scalar_fn: fn (comptime T: type, elems: []const T) RT,
    values: []const T,
) !void {
    var timer = try std.time.Timer.start();
    var vec_nanos: u64 = 0;
    var scalar_nanos: u64 = 0;
    const num_runs = 5;
    for (0..num_runs) |_| {
        timer.reset();
        const vec_result = vec_fn(T, values);
        vec_nanos += timer.lap();

        timer.reset();
        const scalar_result = scalar_fn(T, values);
        scalar_nanos += timer.lap();

        try std.testing.expectEqual(scalar_result, vec_result);
    }
    vec_nanos /= num_runs;
    scalar_nanos /= num_runs;

    const N = values.len;
    std.debug.print(
        "VECTOR {s}: {d:.3} billion per second, SCALAR {s}: {d:.3} billion per second, TYPE: {}, VLEN: {?}, TIME: {}ms\n",
        .{
            name,
            @as(f64, @floatFromInt(N)) / @as(f64, @floatFromInt(vec_nanos)),
            name,
            @as(f64, @floatFromInt(N)) / @as(f64, @floatFromInt(scalar_nanos)),
            T,
            vlen,
            (vec_nanos + scalar_nanos) * num_runs / 1_000_000,
        },
    );

    //const fast_enough = vec_nanos * 4 / 5 < scalar_nanos;
    //try std.testing.expect(fast_enough);
}
