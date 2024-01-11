const std = @import("std");
const enc = @import("../../enc.zig");
const ec = @import("../compute.zig");

pub fn add(ctx: enc.Ctx, left: *const enc.Array, right: *const enc.Array) !*enc.Array {
    const result = try ctx.registry.call("add", ctx, &.{ .{ .array = left }, .{ .array = right } }, &struct {});
    return result.array;
}

pub const Add = ec.BinaryFunction(.{
    .name = "add",
    .doc = "Add the arguments element-wise",
    .Options = struct {
        dtype: ?enc.DType,
    },
    .Impls = &.{
        Scalar,
        ScalarPrimitive,
        Primitive,
        ec.BinaryConstantUnwrapping("add"),
    },
});

const Scalar = struct {
    pub fn add(ctx: enc.Ctx, left: enc.Scalar, right: enc.Scalar, options: *const anyopaque) !enc.Scalar {
        const dtype = try resolveDType(ctx, &.{ .{ .scalar = left }, .{ .scalar = right } }, options);
        const ptype = dtype.toPType() orelse return error.InvalidArguments;
        switch (ptype) {
            inline else => |p| {
                const T = p.astype();
                const result = left.as(T) + right.as(T);
                return enc.Scalar.init(result);
            },
        }
    }
};

const ScalarPrimitive = struct {
    pub fn add(
        ctx: enc.Ctx,
        left: *const enc.PrimitiveArray,
        right: enc.Scalar,
        options: *const anyopaque,
    ) !*enc.Array {
        const dtype = try resolveDType(ctx, &.{ .{ .array = &left.array }, .{ .scalar = right } }, options);
        const ptype = dtype.toPType() orelse return error.InvalidArguments;

        var result = try enc.PrimitiveArray.allocEmpty(ctx.gpa, ptype, left.array.len);
        switch (ptype) {
            inline else => |out_p| {
                const R = out_p.astype();
                const rhs = right.as(R);
                switch (left.ptype) {
                    inline else => |a_p| {
                        const A = a_p.astype();
                        for (left.asSlice(A), result.asMutableSlice(R)) |in, *out| {
                            out.* = cast(R, in) + rhs;
                        }
                    },
                }
            },
        }
        return &result.array;
    }

    pub fn addInverse(ctx: enc.Ctx, left: enc.Scalar, right: *const enc.PrimitiveArray, options: *const anyopaque) !*enc.Array {
        return ScalarPrimitive.add(ctx, right, left, options);
    }
};

const Primitive = struct {
    pub fn add(ctx: enc.Ctx, left: *const enc.PrimitiveArray, right: *const enc.PrimitiveArray, options: *const anyopaque) !*enc.Array {
        @setEvalBranchQuota(10_000);

        std.debug.assert(left.array.len == right.array.len);
        const dtype = try resolveDType(ctx, &.{ .{ .array = &left.array }, .{ .array = &right.array } }, options);
        const resultPType = dtype.toPType() orelse return error.InvalidArgument;

        var result = try enc.PrimitiveArray.allocEmpty(ctx.gpa, resultPType, left.array.len);
        switch (resultPType) {
            inline else => |out_p| {
                const R = out_p.astype();

                switch (left.ptype) {
                    inline else => |a_p| {
                        const A = a_p.astype();
                        switch (right.ptype) {
                            inline else => |b_p| {
                                const B = b_p.astype();
                                for (left.asSlice(A), right.asSlice(B), result.asMutableSlice(R)) |in_a, in_b, *out| {
                                    out.* = cast(R, in_a) + cast(R, in_b);
                                }
                            },
                        }
                    },
                }
            },
        }
        return &result.array;
    }
};

fn cast(comptime R: type, value: anytype) R {
    const T = @typeInfo(@TypeOf(value));
    switch (@typeInfo(R)) {
        .Int => switch (T) {
            .Bool => return @intFromBool(value),
            .Int => return @intCast(value),
            .Float => return @intFromFloat(value),
            else => {},
        },
        .Float => switch (T) {
            .Int => return @floatFromInt(value),
            .Float => return @floatCast(value),
            else => {},
        },
        else => {},
    }
    @compileError("Cannot cast " ++ @typeName(T) ++ " into " ++ @typeName(R));
}

// TODO(ngates): simplify this function, pull some logic into enc.dtypes, etc.
fn resolveDType(ctx: enc.Ctx, params: []const ec.Param, options: *const anyopaque) !enc.DType {
    _ = options;
    std.debug.assert(params.len == 2);
    const left = params[0];
    const right = params[1];

    const leftDType = try left.getDType(ctx.gpa);
    defer leftDType.deinit();
    const rightDType = try right.getDType(ctx.gpa);
    defer rightDType.deinit();

    if (!leftDType.isNumeric() or !rightDType.isNumeric()) {
        return error.InvalidArguments;
    }

    // Check for empty arrays and return the opposite side.
    const leftIsEmpty = switch (left) {
        .array => |a| a.len == 0,
        .scalar => false,
    };
    const rightIsEmpty = switch (right) {
        .array => |a| a.len == 0,
        .scalar => false,
    };
    if (leftIsEmpty and rightIsEmpty) {
        return enc.dtypes.null_;
    } else if (leftIsEmpty) {
        return rightDType;
    } else if (rightIsEmpty) {
        return leftDType;
    }

    // Widen to the widest float if one of the arguments is floating
    if (leftDType == .float or rightDType == .float) {
        return enc.dtypes.widestFloat(leftDType, rightDType);
    }

    // Otherwise, both arguments are ints.
    // The simplest thing to do is to use the widest bit width + 1.
    const leftMax = switch (left) {
        .array => |a| try enc.ops.max(ctx, a),
        .scalar => |s| s,
    };
    const rightMax = switch (right) {
        .array => |a| try enc.ops.max(ctx, a),
        .scalar => |s| s,
    };
    const max = @max(leftMax.as(i64), @max(rightMax.as(i64), leftMax.as(i64) + rightMax.as(i64)));

    const leftMin = switch (left) {
        .array => |a| try enc.ops.min(ctx, a),
        .scalar => |s| s,
    };
    const rightMin = switch (right) {
        .array => |a| try enc.ops.min(ctx, a),
        .scalar => |s| s,
    };
    const min = @min(leftMin.as(i64), @min(rightMin.as(i64), leftMin.as(i64) + rightMin.as(i64)));

    return enc.dtypes.intForRange(min, max);
}

test "primitive add" {
    var ctx = enc.Ctx.testing();
    defer ctx.deinit();

    const a = try enc.PrimitiveArray.allocWithCopy(ctx.gpa, i32, &.{ -1, 1, 100 });
    defer a.release();

    const b = try enc.ConstantArray.allocWithOwnedScalar(ctx.gpa, enc.Scalar.init(1), 3);
    defer b.release();

    const result = try enc.ops.add(ctx, &a.array, &b.array);
    defer result.release();

    try std.testing.expectEqualSlices(i8, &.{ 0, 2, 101 }, enc.PrimitiveArray.from(result).asSlice(i8));
}
