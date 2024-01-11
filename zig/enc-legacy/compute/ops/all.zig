const std = @import("std");
const enc = @import("../../enc.zig");
const ec = @import("../compute.zig");

pub fn all(ctx: enc.Ctx, array: *const enc.Array) !enc.BoolScalar {
    const result = try ctx.registry.call("all", ctx, &.{.{ .array = array }}, &struct {});
    return result.scalar.bool;
}

pub const All = ec.UnaryFunction(.{
    .name = "all",
    .doc = "Returns whether all of the elements in an array are true.",
    .Options = struct {},
    .Impls = &.{ TrueCountEqualsLength, Fallback },
});

const TrueCountEqualsLength = struct {
    pub fn all(ctx: enc.Ctx, array: *const enc.Array, options: *const anyopaque) !enc.Scalar {
        _ = options;
        const trueCount = try array.computeStatistic(ctx.gpa, .true_count);
        defer trueCount.deinit();
        return enc.Scalar.init(trueCount.as(usize) == array.len);
    }
};

const Fallback = ec.BooleanReducer("all", struct {
    pub inline fn reduce(a: enc.Scalar, b: enc.Scalar) enc.Scalar {
        if (a.isNull()) return b;
        if (b.isNull()) return a;
        return enc.Scalar.init(a.as(bool) and b.as(bool));
    }
});

test "bool all" {
    var ctx = enc.Ctx.testing();
    defer ctx.deinit();

    const allTrue = try enc.BoolArray.allocWithBools(std.testing.allocator, &.{ true, true, true });
    defer allTrue.release();
    const allTrueResult = try enc.ops.all(ctx, &allTrue.array);
    try std.testing.expect(allTrueResult.value);

    var mixed = try enc.BoolArray.allocWithBools(std.testing.allocator, &.{ true, false, true });
    defer mixed.release();
    const notAllTrue = try enc.ops.all(ctx, &mixed.array);
    try std.testing.expect(!notAllTrue.value);

    var empty = try enc.BoolArray.allocEmpty(std.testing.allocator, 0);
    defer empty.release();
    const emptyResult = try enc.ops.all(ctx, &empty.array);
    try std.testing.expect(emptyResult.value);
}

test "const all" {
    var ctx = enc.Ctx.testing();
    defer ctx.deinit();

    const allTrue = try enc.ConstantArray.allocWithOwnedScalar(std.testing.allocator, enc.Scalar.init(true), 3);
    defer allTrue.release();
    const allTrueResult = try enc.ops.all(ctx, &allTrue.array);
    try std.testing.expect(allTrueResult.value);

    var mixed = try enc.ConstantArray.allocWithOwnedScalar(std.testing.allocator, enc.Scalar.init(false), 3);
    defer mixed.release();
    const notAllTrue = try enc.ops.all(ctx, &mixed.array);
    try std.testing.expect(!notAllTrue.value);

    var empty = try enc.ConstantArray.allocWithOwnedScalar(std.testing.allocator, enc.Scalar.init(false), 0);
    defer empty.release();
    const emptyResult = try enc.ops.all(ctx, &empty.array);
    try std.testing.expect(emptyResult.value);
}
