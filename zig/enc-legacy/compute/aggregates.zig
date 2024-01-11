const std = @import("std");
const enc = @import("../enc.zig");
const ec = @import("./compute.zig");

pub fn PrimitiveScalarReducer(
    comptime op: []const u8,
    comptime Reducer: type,
) type {
    return struct {
        pub fn fallback_reducer(ctx: enc.Ctx, array: *const enc.Array, options: *const anyopaque) !enc.Scalar {
            const function = ctx.registry.findFunction(op) orelse return error.NoFunction;

            const dtype = try array.dtype.clone(ctx.gpa);
            errdefer dtype.deinit();
            const ptype = dtype.toPType() orelse return error.InvalidArguments;

            var cumulative: enc.Scalar = enc.NullableScalar.initAbsentOwned(dtype);

            var iter = try array.iterPlain(ctx.gpa);
            defer iter.deinit();

            while (try iter.next(ctx.gpa)) |chunk| {
                defer chunk.release();

                const result = try function.call(ctx, &.{.{ .array = chunk }}, options);
                const chunk_result = result.scalar;

                // We know the scalars are primitive, therefore we can skip the deinit.
                cumulative = Reducer.reduce(cumulative, chunk_result, ptype);
            }

            return cumulative;
        }
    };
}

pub fn BooleanReducer(
    comptime op: []const u8,
    comptime Reducer: type,
) type {
    return struct {
        pub fn fallback_reducer(ctx: enc.Ctx, array: *const enc.Array, options: *const anyopaque) !enc.Scalar {
            const function = ctx.registry.findFunction(op) orelse return error.NoFunction;
            var cumulative: enc.Scalar = enc.NullableScalar.initAbsentOwned(enc.dtypes.bool_);

            var iter = try array.iterPlain(ctx.gpa);
            defer iter.deinit();
            while (try iter.next(ctx.gpa)) |chunk| {
                defer chunk.release();
                const result = try function.call(ctx, &.{.{ .array = chunk }}, options);
                cumulative = Reducer.reduce(cumulative, result.scalar);
            }
            return cumulative;
        }
    };
}
