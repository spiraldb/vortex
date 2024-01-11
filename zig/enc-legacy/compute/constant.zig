const std = @import("std");
const enc = @import("../enc.zig");
const ec = @import("./compute.zig");

/// Returns the constant array's scalar value.
pub fn ConstantArrayReturnScalar() type {
    return struct {
        pub fn getScalar(ctx: enc.Ctx, array: *const enc.ConstantArray, options: *const anyopaque) !enc.Scalar {
            _ = options;
            return array.scalar.clone(ctx.gpa);
        }
    };
}

pub fn BinaryConstantUnwrapping(comptime op: []const u8) type {
    return struct {
        pub fn unwrapLeft(ctx: enc.Ctx, left: *const enc.ConstantArray, right: *const enc.Array, options: *const anyopaque) !ec.Result {
            return try ctx.registry.call(
                op,
                ctx,
                &.{ .{ .scalar = left.scalar }, .{ .array = right } },
                options,
            );
        }

        pub fn unwrapLeftScalar(ctx: enc.Ctx, left: *const enc.ConstantArray, right: enc.Scalar, options: *const anyopaque) !*enc.Array {
            const result = try ctx.registry.call(op, ctx, &.{ .{ .scalar = left.scalar }, .{ .scalar = right } }, options);
            return &(try enc.ConstantArray.allocWithOwnedScalar(ctx.gpa, result.scalar, left.array.len)).array;
        }

        pub fn unwrapRight(ctx: enc.Ctx, left: *const enc.Array, right: *const enc.ConstantArray, options: *const anyopaque) !ec.Result {
            return ctx.registry.call(op, ctx, &.{ .{ .array = left }, .{ .scalar = right.scalar } }, options);
        }

        pub fn unwrapRightScalar(ctx: enc.Ctx, left: enc.Scalar, right: *const enc.ConstantArray, options: *const anyopaque) !*enc.Array {
            const result = try ctx.registry.call(op, ctx, &.{ .{ .scalar = left }, .{ .scalar = right.scalar } }, options);
            return &(try enc.ConstantArray.allocWithOwnedScalar(ctx.gpa, result.scalar, right.array.len)).array;
        }
    };
}
