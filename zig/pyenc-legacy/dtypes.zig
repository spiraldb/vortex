const std = @import("std");
const py = @import("pydust");
const arrow = @import("arrow");
const enc = @import("pyenc");
const pyenc = @import("./pyenc.zig");
const pyarrow = @import("./pyarrow.zig");
const pyio = @import("pyio.zig");

pub const dtypes = py.module(struct {
    const Self = @This();

    extension_registry: std.StringHashMap(py.PyType),

    pub fn __init__(self: *Self) void {
        self.* = .{ .extension_registry = std.StringHashMap(py.PyType).init(py.allocator) };
    }

    pub fn __del__(self: *Self) void {
        self.extension_registry.deinit();
    }

    // pub fn register_extension(self: *Self, args: struct { dtype: *const pyenc.ExtensionDType }) !void {
    //     const id = try py.allocator.dupe(u8, args.dtype.dtype.wrapped.extension.id);
    //     const cls = py.type_(args.dtype);
    //     py.incref(cls);
    //     try self.extension_registry.put(id, cls);
    // }

    /// Test whether a given DType is nullable.
    pub fn is_nullable(args: struct { dtype: *const pyenc.DType }) bool {
        return args.dtype.unwrap() == .nullable;
    }

    pub fn nullable(args: struct { dtype: *const pyenc.DType }) !*const pyenc.DType {
        const nullable_dtype = try (args.dtype.unwrap()).toNullable(pyenc.allocator());
        return pyenc.DType.wrapOwned(nullable_dtype);
    }

    pub fn non_nullable(args: struct { dtype: *const pyenc.DType }) !*const pyenc.DType {
        switch (args.dtype.unwrap()) {
            .nullable => |n| return pyenc.DType.wrap(n.child.*, pyenc.allocator()),
            else => {},
        }
        // Otherwise, return the non-nullable dtype
        py.incref(args.dtype);
        return args.dtype;
    }

    /// Test whether a given DType is null.
    pub fn is_null(args: struct { dtype: *const pyenc.DType }) bool {
        return args.dtype.unwrap() == .null;
    }

    /// Test whether a given DType is a boolean type.
    pub fn is_boolean(args: struct { dtype: *const pyenc.DType }) bool {
        return args.dtype.unwrap() == .bool;
    }

    /// Test whether a given DType is a signed or unsigned integer.
    pub fn is_integer(args: struct { dtype: *const pyenc.DType }) bool {
        return args.dtype.unwrap() == .int or args.dtype.unwrap() == .uint;
    }

    /// Test whether a given DType is a signed integer.
    pub fn is_signed_integer(args: struct { dtype: *const pyenc.DType }) bool {
        return args.dtype.unwrap() == .int;
    }

    /// Test whether a given DType is an unsigned integer.
    pub fn is_unsigned_integer(args: struct { dtype: *const pyenc.DType }) bool {
        return args.dtype.unwrap() == .uint;
    }

    /// Convert an unsigned integer DType to its signed equivalent.
    pub fn to_signed(args: struct { dtype: *const pyenc.DType }) !*const pyenc.DType {
        switch (args.dtype.unwrap()) {
            .uint => |width| return pyenc.DType.wrapOwned(.{ .int = width }),
            else => return py.TypeError.raise("Not an unsigned integer"),
        }
    }

    /// Convert a signed integer DType to its unsigned equivalent.
    pub fn to_unsigned(args: struct { dtype: *const pyenc.DType }) !*const pyenc.DType {
        switch (args.dtype.unwrap()) {
            .int => |width| return pyenc.DType.wrapOwned(.{ .uint = width }),
            else => return py.TypeError.raise("Not a signed integer"),
        }
    }

    /// Test whether a given DType is a float.
    pub fn is_float(args: struct { dtype: *const pyenc.DType }) bool {
        return args.dtype.unwrap() == .float;
    }

    /// Test whether a given DType is a utf8 type.
    pub fn is_utf8(args: struct { dtype: *const pyenc.DType }) bool {
        return args.dtype.unwrap() == .utf8;
    }

    /// Test whether a given DType is a binary type.
    pub fn is_binary(args: struct { dtype: *const pyenc.DType }) bool {
        return args.dtype.unwrap() == .binary;
    }

    /// Test whether a given DType is a localtime type.
    pub fn is_localtime(args: struct { dtype: *const pyenc.DType }) bool {
        return args.dtype.unwrap() == .localtime;
    }

    /// Test whether a given DType is a localdate type.
    pub fn is_localdate(args: struct { dtype: *const pyenc.DType }) bool {
        return args.dtype.unwrap() == .localdate;
    }

    /// Test whether a given DType is an instant type.
    pub fn is_instant(args: struct { dtype: *const pyenc.DType }) bool {
        return args.dtype.unwrap() == .instant;
    }

    /// Test whether a given DType is a struct type.
    pub fn is_struct(args: struct { dtype: *const pyenc.DType }) bool {
        return args.dtype.unwrap() == .struct_;
    }

    /// Test whether a given DType is a list type.
    pub fn is_list(args: struct { dtype: *const pyenc.DType }) bool {
        return args.dtype.unwrap() == .list;
    }

    /// Return the names of the struct fields.
    pub fn struct_names(args: struct { dtype: *const pyenc.DType }) !py.PyList {
        switch (args.dtype.unwrap()) {
            .struct_ => |s| {
                const names = try py.PyList.new(s.names.len);
                for (s.names, 0..) |name, i| {
                    try names.setOwnedItem(i, try py.PyString.create(name));
                }
                return names;
            },
            else => return py.TypeError.raise("Not a struct type"),
        }
    }

    /// Return the dtypes of the struct fields.
    pub fn struct_dtypes(args: struct { dtype: *const pyenc.DType }) !py.PyList {
        switch (args.dtype.unwrap()) {
            .struct_ => |s| {
                const types = try py.PyList.new(s.fields.len);
                for (s.fields, 0..) |field, i| {
                    try types.setOwnedItem(i, try pyenc.DType.wrap(field, pyenc.allocator()));
                }
                return types;
            },
            else => return py.TypeError.raise("Not a struct type"),
        }
    }

    /// Return the value type of a list.
    pub fn list_dtype(args: struct { dtype: *const pyenc.DType }) !*const pyenc.DType {
        switch (args.dtype.unwrap()) {
            .list => |l| return pyenc.DType.wrapOwned(try l.child.clone(pyenc.allocator())),
            else => return py.TypeError.raise("Not a list type"),
        }
    }

    pub fn is_extension(args: struct { dtype: *const pyenc.DType }) bool {
        return args.dtype.unwrap() == .extension;
    }

    pub fn from_pyarrow(args: struct { pa_type: py.PyObject }) !*const pyenc.DType {
        return pyenc.DType.wrapOwned(try pyarrow.dtypeFromPyArrow(args.pa_type));
    }

    /// Read DType from binary representation. The bytes should come from calling DType.to_bytes()
    pub fn from_bytes(args: struct { reader: py.PyObject }) !*const pyenc.DType {
        var pyReader = try pyio.pythonReader(args.reader);
        return pyenc.DType.wrapOwned(try enc.DType.fromBytes(pyReader.reader(), pyenc.allocator()));
    }
});
