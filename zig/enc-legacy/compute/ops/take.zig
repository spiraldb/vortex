const std = @import("std");
const enc = @import("../../enc.zig");
const ec = @import("../compute.zig");
const cloning = @import("../../cloning.zig");

// TODO(ngates): take anytype, and convert to array.
pub fn take(ctx: enc.Ctx, array: *const enc.Array, indices: *const enc.Array) !*enc.Array {
    const result = try ctx.registry.call("take", ctx, &.{ .{ .array = array }, .{ .array = indices } }, &struct {});
    return result.array;
}

pub const Take = ec.BinaryFunction(.{
    .name = "take",
    .doc = "Select values given integer selection indices.",
    .Options = struct {},
    .Impls = &.{
        Primitive,
        Constant,
        Extension,
        Struct,
    },
});

const Primitive = struct {
    pub fn take_primitive(ctx: enc.Ctx, array: *const enc.PrimitiveArray, indices: *const enc.PrimitiveArray, options: *const anyopaque) !*enc.Array {
        _ = options;
        var newArray = try enc.PrimitiveArray.allocEmpty(ctx.gpa, array.ptype, indices.array.len);
        errdefer newArray.release();

        switch (array.ptype) {
            inline else => |p| {
                const T = p.astype();
                const existingSlice = array.asSlice(T);
                const newSlice = newArray.asMutableSlice(T);

                // TODO(ngates): can we do this without N^2 ptype expansion?
                switch (indices.ptype) {
                    inline .u8, .u16, .u32, .u64, .i8, .i16, .i32, .i64 => |ip| {
                        for (newSlice, indices.asSlice(ip.astype())) |*newValue, index| {
                            newValue.* = existingSlice[@intCast(index)];
                        }
                    },
                    else => std.debug.panic("Unsupported indices dtype", .{}),
                }
            },
        }

        return &newArray.array;
    }
};

const Constant = struct {
    pub fn take_constant(ctx: enc.Ctx, array: *const enc.ConstantArray, indices: *const enc.PrimitiveArray, options: *const anyopaque) !*enc.Array {
        _ = options;
        const newArray = try enc.ConstantArray.allocWithOwnedScalar(ctx.gpa, try array.scalar.clone(ctx.gpa), indices.array.len);
        return &newArray.array;
    }

    pub fn take_const_index(ctx: enc.Ctx, array: *const enc.PrimitiveArray, indices: *const enc.ConstantArray, options: *const anyopaque) !*enc.Array {
        _ = options;
        const newValue = try array.array.getScalar(ctx.gpa, indices.scalar.as(usize));
        const newArray = try enc.ConstantArray.allocWithOwnedScalar(ctx.gpa, newValue, indices.array.len);
        return &newArray.array;
    }
};

const Extension = struct {
    pub fn take_extension(ctx: enc.Ctx, array: *const enc.Array, indices: *const enc.PrimitiveArray, options: *const anyopaque) !*enc.Array {
        _ = options;
        return try array.getElements(ctx.gpa, indices);
    }
};

const Struct = struct {
    pub fn take(ctx: enc.Ctx, array: *const enc.StructArray, indices: *const enc.PrimitiveArray, options: *const anyopaque) !*enc.StructArray {
        _ = options;
        const names = try cloning.cloneStrings(ctx.gpa, array.names);

        const fields = try ctx.gpa.alloc(*enc.Array, array.fields.len);
        for (array.fields, 0..) |field, i| {
            fields[i] = try enc.ops.take(ctx, field, &indices.array);
        }

        return try enc.StructArray.allocWithOwnedNamesAndFields(ctx.gpa, names, fields);
    }
};

const DecodeIndices = struct {
    pub fn decode(ctx: enc.Ctx, array: *const enc.Array, indices: *const enc.Array, options: *const anyopaque) !*enc.Array {
        _ = options;
        _ = array;
        _ = ctx;
        if (indices.kind == .primitive) {
            // Terminate the recursion in case we're called again
            return ec.Error.NoKernel;
        }
    }
};

test "primitive take" {
    var ctx = enc.Ctx.testing();
    defer ctx.deinit();

    const a = try enc.PrimitiveArray.allocWithCopy(ctx.gpa, u32, &.{ 1, 2, 3, 4, 5, 6, 7, 8 });
    defer a.release();

    const indices = try enc.PrimitiveArray.allocWithCopy(ctx.gpa, u8, &.{ 0, 2, 4, 6 });
    defer indices.release();

    const elems = enc.PrimitiveArray.from(try enc.ops.take(ctx, &a.array, &indices.array));
    defer elems.release();

    try std.testing.expectEqualSlices(u32, &.{ 1, 3, 5, 7 }, elems.asSlice(u32));
}
