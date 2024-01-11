const std = @import("std");
const enc = @import("../../enc.zig");
const ec = @import("../compute.zig");

pub fn max(ctx: enc.Ctx, array: *const enc.Array) !enc.Scalar {
    const result = try ctx.registry.call("max", ctx, &.{.{ .array = array }}, &struct {});
    return result.scalar;
}

pub const Max = ec.UnaryFunction(.{
    .name = "max",
    .doc = "Find the maximum numeric value in the array",
    .Options = struct {},
    .Impls = &.{
        ec.ConstantArrayReturnScalar(),
        ec.ReturnStatsScalar(.max),
        Fallback,
    },
});

const Fallback = ec.PrimitiveScalarReducer("max", struct {
    pub inline fn reduce(a: enc.Scalar, b: enc.Scalar, ptype: enc.PType) enc.Scalar {
        switch (ptype) {
            inline else => |p| {
                const T = p.astype();
                if (a.isNull()) return b;
                if (b.isNull()) return a;
                return enc.Scalar.init(@max(a.as(T), b.as(T)));
            },
        }
    }
});

test "primitive max" {
    var ctx = enc.Ctx.testing();
    defer ctx.deinit();

    const a = try enc.PrimitiveArray.allocWithCopy(ctx.gpa, i32, &.{ -1, 1, 10_000 });
    defer a.release();

    try std.testing.expectEqual(enc.Scalar.init(@as(i32, 10_000)), try enc.ops.max(ctx, &a.array));
}

test "boolean max" {
    var ctx = enc.Ctx.testing();
    defer ctx.deinit();

    const falsy = try enc.BoolArray.allocWithBools(ctx.gpa, &.{ false, false, false });
    defer falsy.release();
    try std.testing.expectEqual(enc.Scalar.init(false), try enc.ops.max(ctx, &falsy.array));

    const truthy = try enc.BoolArray.allocWithBools(ctx.gpa, &.{ true, true, true });
    defer truthy.release();
    try std.testing.expectEqual(enc.Scalar.init(true), try enc.ops.max(ctx, &truthy.array));

    const mixed = try enc.BoolArray.allocWithBools(ctx.gpa, &.{ true, false, false });
    defer mixed.release();
    try std.testing.expectEqual(enc.Scalar.init(true), try enc.ops.max(ctx, &mixed.array));
}
