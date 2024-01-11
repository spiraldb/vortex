const std = @import("std");
const py = @import("pydust");
const enc = @import("pyenc");
const pyarrow = @import("./pyarrow.zig");
const pyenc = @import("./pyenc.zig");
const pybuffer = @import("./pybuffer.zig");

pub const ptypes = py.module(struct {
    pub fn __exec__(mod: py.PyModule) !void {
        // Add static attributes for each of the PTypes
        for (std.enums.values(enc.PType)) |p| {
            try mod.addObjectRef(@tagName(p), try pyenc.PType.wrap(p));
        }
    }

    pub fn is_integer(args: struct { ptype: *const pyenc.PType }) bool {
        return args.ptype.wrapped.isInteger();
    }

    pub fn is_signed_integer(args: struct { ptype: *const pyenc.PType }) bool {
        return args.ptype.wrapped.isSignedInteger();
    }

    pub fn is_unsigned_integer(args: struct { ptype: *const pyenc.PType }) bool {
        return args.ptype.wrapped.isUnsignedInteger();
    }

    pub fn is_float(args: struct { ptype: *const pyenc.PType }) bool {
        return args.ptype.wrapped.isFloat();
    }

    // Convert an unsigned integer PType to its signed equivalent.
    pub fn to_signed(args: struct { ptype: *const pyenc.PType }) !*const pyenc.PType {
        return switch (args.ptype.wrapped) {
            inline .u8, .u16, .u32, .u64 => |p| try pyenc.PType.wrap(enc.PType.fromType(std.meta.Int(.signed, p.bitSizeOf()))),
            else => py.TypeError.raise("Not an unsigned integer"),
        };
    }

    // Convert a signed integer DType to its unsigned equivalent.
    pub fn to_unsigned(args: struct { ptype: *const pyenc.PType }) !*const pyenc.PType {
        return switch (args.ptype.wrapped) {
            inline .i8, .i16, .i32, .i64 => |p| try pyenc.PType.wrap(enc.PType.fromType(std.meta.Int(.unsigned, p.bitSizeOf()))),
            else => py.TypeError.raise("Not a signed integer"),
        };
    }

    /// Construct a PType from its ID.
    pub fn from_id(args: struct { id: u8 }) !*const pyenc.PType {
        if (enc.PType.fromId(args.id)) |ptype| {
            return pyenc.PType.wrap(ptype);
        }
        return py.ValueError.raise("Invalid PType id");
    }

    /// Convert a PyArrow DType to an pyenc PType.
    pub fn from_pyarrow(args: struct { pyarrow_type: py.PyObject }) !*const pyenc.PType {
        return pyenc.PType.wrap(try pyarrow.ptypeFromPyArrow(args.pyarrow_type));
    }

    pub fn from_format_code(args: struct { format: py.PyString }) !*const pyenc.PType {
        const fmt = try args.format.asSlice();
        const encPType = try pybuffer.ptypeFromCode(fmt);
        return pyenc.PType.wrap(encPType);
    }
});
