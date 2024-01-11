const std = @import("std");
const arrow = @import("arrow");
const enc = @import("pyenc");
const pyenc = @import("./pyenc.zig");
const py = @import("pydust");

/// Convert a PyArrow type to an pyenc.DType.
pub fn dtypeFromPyArrow(pa_type: py.PyObject) !enc.DType {
    var schema: arrow.Schema = undefined;
    try pa_type.call(void, "_export_to_c", .{@intFromPtr(&schema)}, .{});
    const dtype = enc.arrow.dtypeFromArrow(pyenc.allocator(), schema) catch |err| switch (err) {
        error.ArrowConversionFailed => return py.ValueError.raiseFmt(
            "Failed to convert PyArrow type {s} to DType",
            .{try (try py.str(pa_type)).asSlice()},
        ),
        else => return err,
    };
    return dtype;
}

pub fn ptypeFromPyArrow(pa_type: py.PyObject) !enc.PType {
    // Easiest way seems to be switch over the strings.
    const type_name = try py.str(pa_type);
    defer type_name.decref();

    const paTypeMap = std.ComptimeStringMap(enc.PType, .{
        .{ "int8", .i8 },
        .{ "uint8", .u8 },
        .{ "int16", .i16 },
        .{ "uint16", .u16 },
        .{ "int32", .i32 },
        .{ "uint32", .u32 },
        .{ "int64", .i64 },
        .{ "uint64", .u64 },
        .{ "halffloat", .f16 },
        .{ "float", .f32 },
        .{ "double", .f64 },
    });

    const ptype = paTypeMap.get(try type_name.asSlice()) orelse return py.TypeError.raiseFmt(
        "Unsupported PyArrow type {s}",
        .{try type_name.asSlice()},
    );

    return ptype;
}

// Takes ownership of the array.
pub fn arrayToPyArrow(array: arrow.Array, pa_type: py.PyObject) !py.PyObject {
    const pa = try py.import("pyarrow");
    defer pa.decref();

    const pa_Array = try pa.get("Array");
    return try pa_Array.call(py.PyObject, "_import_from_c", .{ @intFromPtr(&array), pa_type }, .{});
}
