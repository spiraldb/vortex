const std = @import("std");
const enc = @import("../../enc.zig");
const ec = @import("../compute.zig");

pub fn min(ctx: enc.Ctx, array: *const enc.Array) !enc.Scalar {
    const result = try ctx.registry.call("min", ctx, &.{.{ .array = array }}, &struct {});
    return result.scalar;
}

pub const Min = ec.UnaryFunction(.{
    .name = "min",
    .doc = "Find the minimum numeric value in the array",
    .Options = struct {},
    .Impls = &.{
        ec.ConstantArrayReturnScalar(),
        ec.ReturnStatsScalar(.min),
        Fallback,
    },
});

const Fallback = ec.PrimitiveScalarReducer("min", struct {
    pub inline fn reduce(a: enc.Scalar, b: enc.Scalar, ptype: enc.PType) enc.Scalar {
        switch (ptype) {
            inline else => |p| {
                const T = p.astype();
                if (a.isNull()) return b;
                if (b.isNull()) return a;
                return enc.Scalar.init(@min(a.as(T), b.as(T)));
            },
        }
    }
});
