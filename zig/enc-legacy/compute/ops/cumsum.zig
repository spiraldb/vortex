const std = @import("std");
const enc = @import("../../enc.zig");
const ec = @import("../compute.zig");

const Options = struct {
    skip_nulls: bool = true,
};

const CumSum = ec.ComputeFunction(.{
    .doc = "Compute the cumulative sum over an array",
    .param_kinds = &.{.array},
    .result_kind = &.{.array},
    .param_dtypes = .{ .func = &paramDTypes },
    .result_dtype = .{ .func = &resultDType },
    .options = Options,
});

fn paramDTypes(array_dtype: enc.DType, options: Options) bool {
    _ = options;
    return enc.dtypes.is_numeric(array_dtype);
}

fn resultDType(ctx: enc.Ctx, array: *const enc.Array, options: Options) enc.DType {
    _ = ctx;
    _ = options;
    // TODO(ngates): how can we best handle overflow?
    // Maybe we find the dtype that holds (min*length) .. (max*length)

    // Seems like a reasonable default, the user can override this.
    return array.dtype;
}

pub const cumulative_sum = CumSum.init(PrimitiveCumSum);

const PrimitiveCumSum = struct {
    // Tells the compute dispatch that it's safe for us to overwrite the input array.
    // .can_reuse_inputs = true,

    pub fn cumsum(array: *const enc.PrimitiveArray, out: *enc.Array) !void {
        _ = out;
        _ = array;
    }
};
