//! Python bindings for the pyenc library.
const std = @import("std");
const py = @import("pydust");
const enc = @import("pyenc");
const pybuffer = @import("./pybuffer.zig");

pub const Array = @import("./arrays/array.zig").Array;
pub const Stats = @import("./arrays/array.zig").Stats;
pub const ScalarIterator = @import("./arrays/array.zig").ScalarIterator;
pub const PlainIterator = @import("./arrays/array.zig").PlainIterator;
pub const PyArrowIterator = @import("./arrays/array.zig").PyArrowIterator;

pub const PrimitiveArray = @import("./arrays/primitive.zig").PrimitiveArray;
pub const BinaryArray = @import("./arrays/binary.zig").BinaryArray;
pub const BoolArray = @import("./arrays/bool.zig").BoolArray;
pub const ChunkedArray = @import("./arrays/chunked.zig").ChunkedArray;
pub const ConstantArray = @import("./arrays/constant.zig").ConstantArray;
pub const DictionaryArray = @import("./arrays/dictionary.zig").DictionaryArray;
pub const PatchedArray = @import("./arrays/patched.zig").PatchedArray;
pub const RoaringBoolArray = @import("./arrays/roaring_bool.zig").RoaringBoolArray;
pub const RoaringUIntArray = @import("./arrays/roaring_uint.zig").RoaringUIntArray;
pub const PyArray = @import("./arrays/python.zig").PyArray;
pub const PyExtensionArray = @import("./arrays/python.zig").PyExtensionArray;
pub const StructArray = @import("./arrays/struct.zig").StructArray;

pub const Buffer = pybuffer.Buffer;

pub const DType = @import("./dtype.zig").DType;
pub const PyDType = @import("./dtype.zig").PyDType;

pub const PType = @import("./ptype.zig").PType;

pub const Scalar = @import("./scalar.zig").Scalar;
pub const PyScalar = @import("./scalar.zig").PyScalar;

pub const dtypes = @import("./dtypes.zig").dtypes;
pub const ptypes = @import("./ptypes.zig").ptypes;
pub const scalars = @import("./scalars.zig").scalars;

pub const compute = @import("./compute.zig").compute;
pub const ComputeFunction = @import("./compute.zig").ComputeFunction;
pub const FunctionRegistration = @import("./compute.zig").FunctionRegistration;

pub usingnamespace @import("pycodecs.zig");

pub const trace = @import("pytrazy.zig").trace;
pub const PyTraceCtx = @import("pytrazy.zig").PyTrazyCtx;

const Self = @This();

const GPA = std.heap.GeneralPurposeAllocator(.{
    .stack_trace_frames = 16,
});

gpa: GPA,
use_gpa: bool = false,
ctx: enc.Ctx,

pub fn __init__(self: *Self) !void {
    self.* = .{
        .gpa = GPA{},
        .ctx = try enc.Ctx.init(py.allocator),
    };
}

pub usingnamespace py.zig(struct {
    pub fn allocator() std.mem.Allocator {
        const self: *Self = py.moduleState(Self) catch std.debug.panic("Cannot get allocator", .{});
        return if (self.use_gpa) self.gpa.allocator() else py.allocator;
    }

    pub fn tryAllocator() !std.mem.Allocator {
        const self: *Self = try py.moduleState(Self);
        return if (self.use_gpa) self.gpa.allocator() else py.allocator;
    }

    pub fn ctx() !enc.Ctx {
        const self: *Self = try py.moduleState(Self);
        return self.ctx;
    }
});

pub fn detect_leaks(self: *Self) bool {
    return self.gpa.detectLeaks();
}

pub fn enable_gpa(self: *Self) void {
    _ = self.gpa.deinit();
    self.gpa = GPA{};
    self.use_gpa = true;
}

pub fn disable_gpa(self: *Self) void {
    self.use_gpa = false;
}

pub fn __exec__(mod: py.PyModule) !void {
    // Add static attributes for each of the non-parameterized (void) dtypes.
    inline for (@typeInfo(enc.DType).Union.fields) |d| {
        if (@typeInfo(d.type) == .Void) {
            const fieldName = if (@hasDecl(enc.dtypes, d.name)) d.name else d.name ++ "_";
            try mod.addObjectRef(d.name ++ "", try mod.init(
                "DType",
                DType{ .wrapped = @field(enc.dtypes, fieldName) },
            ));
        }
    }
    // TODO(ngates): remove this when Pydust supports optional arguments. e.g. `pyenc.int()` and `pyenc.int(8)`
    try mod.addObjectRef("int_", try mod.init("DType", DType{ .wrapped = enc.dtypes.int }));
    try mod.addObjectRef("uint_", try mod.init("DType", DType{ .wrapped = enc.dtypes.uint }));
}

pub fn int(args: struct { width: u8, nullable: bool = false }) !*const DType {
    const width = enc.DType.IntWidth.fromInt(args.width) orelse return py.ValueError.raiseFmt("Invalid integer width {}", .{args.width});
    const dtype: enc.DType = .{ .int = width };
    return DType.wrapOwned(if (args.nullable) try dtype.toNullable(Self.allocator()) else dtype);
}

pub fn uint(args: struct { width: u8, nullable: bool = false }) !*const DType {
    const width = enc.DType.IntWidth.fromInt(args.width) orelse return py.ValueError.raiseFmt("Invalid integer width {}", .{args.width});
    const dtype: enc.DType = .{ .uint = width };
    return DType.wrapOwned(if (args.nullable) try dtype.toNullable(Self.allocator()) else dtype);
}

pub fn float(args: struct { width: u8, nullable: bool = false }) !*const DType {
    const width = enc.DType.FloatWidth.fromInt(args.width) orelse return py.ValueError.raiseFmt("Invalid float width {}", .{args.width});
    const dtype: enc.DType = .{ .float = width };
    return DType.wrapOwned(if (args.nullable) try dtype.toNullable(Self.allocator()) else dtype);
}

pub fn instant(args: struct { unit: [:0]const u8, nullable: bool = false }) !*const DType {
    const unit = enc.DType.TimeUnit.fromString(args.unit) orelse return py.ValueError.raiseFmt("Invalid time unit {s}", .{args.unit});
    const dtype: enc.DType = .{ .instant = unit };
    return DType.wrapOwned(if (args.nullable) try dtype.toNullable(Self.allocator()) else dtype);
}

pub fn @"struct"(args: struct { names: py.PyList, dtypes: py.PyList, nullable: bool = false }) !*const DType {
    if (args.names.length() != args.dtypes.length()) {
        return py.ValueError.raise("Names and dtypes are of different length");
    }
    const nfields = args.names.length();

    const names = try Self.allocator().alloc([]const u8, nfields);
    const fields = try Self.allocator().alloc(enc.DType, nfields);
    for (names, fields, 0..) |*name, *field, i| {
        const field_dtype = try args.dtypes.getItem(*const DType, @intCast(i));
        name.* = try Self.allocator().dupe(u8, try args.names.getItem([]const u8, @intCast(i)));
        field.* = try field_dtype.wrapped.clone(Self.allocator());
    }

    const dtype: enc.DType = .{ .struct_ = .{ .names = names, .fields = fields, .allocator = Self.allocator() } };
    return DType.wrapOwned(if (args.nullable) try dtype.toNullable(Self.allocator()) else dtype);
}

pub fn list(args: struct { child_dtype: *const DType, nullable: bool = false }) !*const DType {
    const child_dtype = try Self.allocator().create(enc.DType);
    child_dtype.* = try args.child_dtype.unwrap().clone(Self.allocator());

    const dtype: enc.DType = .{
        .list = .{
            .child = child_dtype,
            .allocator = Self.allocator(),
        },
    };
    return DType.wrapOwned(if (args.nullable) try enc.DType.toNullable(dtype, Self.allocator()) else dtype);
}

/// Construct a Buffer from a Python object supporting the buffer protocol.
pub fn buffer(args: struct { buffer_like: py.PyObject }) !*Buffer {
    const enc_buffer = try pybuffer.bufferFromPyBuffer(Self.allocator(), args.buffer_like);
    return try py.init(Buffer, .{ .wrapped = enc_buffer });
}

/// Construct an pyenc scalar from a Python object.
pub fn scalar(args: struct { obj: py.PyObject, dtype: ?*const DType = null }) !*const Scalar {
    return Scalar.wrapOwned(try Scalar.py_to_enc(args.obj, if (args.dtype) |dt| dt.unwrap() else null));
}

pub fn _incref(args: struct { obj: py.PyObject }) void {
    py.incref(args.obj);
}

pub fn _decref(args: struct { obj: py.PyObject }) void {
    py.decref(args.obj);
}

comptime {
    py.rootmodule(@This());
}
