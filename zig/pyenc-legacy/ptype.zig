//! Python bindings for the pyenc library.
const std = @import("std");
const py = @import("pydust");
const enc = @import("pyenc");
const pyenc = @import("./pyenc.zig");

pub const PType = py.class(struct {
    const Self = @This();

    wrapped: enc.PType,

    id: py.property(struct {
        pub fn get(self: *const Self) u8 {
            return @intFromEnum(self.wrapped);
        }
    }) = .{},

    bit_width: py.property(struct {
        pub fn get(self: *const Self) u8 {
            return self.wrapped.bitSizeOf();
        }
    }) = .{},

    byte_width: py.property(struct {
        pub fn get(self: *const Self) u8 {
            return self.wrapped.sizeOf();
        }
    }) = .{},

    pub fn __eq__(self: *const Self, other: *const Self) bool {
        return @intFromEnum(self.wrapped) == @intFromEnum(other.wrapped);
    }

    pub fn __hash__(self: *const Self) usize {
        var hasher = std.hash.Wyhash.init(0);
        std.hash.autoHash(&hasher, @intFromEnum(self.wrapped));
        return hasher.final();
    }

    pub fn __repr__(self: *const Self) !py.PyString {
        // Defer to the Zig representation of PType.
        return try py.PyString.createFmt("{}", .{self.wrapped});
    }

    /// Convert the pyenc PType to a pyenc DType.
    pub fn to_dtype(self: *const Self) !*const pyenc.DType {
        return pyenc.DType.wrapOwned(enc.DType.fromPType(self.wrapped));
    }

    /// Convert the pyenc PType to a PyArrow DType.
    pub fn to_pyarrow(self: *const Self) !py.PyObject {
        const pa_dtype = switch (self.wrapped) {
            .i8 => "int8",
            .u8 => "uint8",
            .i16 => "int16",
            .u16 => "uint16",
            .i32 => "int32",
            .u32 => "uint32",
            .i64 => "int64",
            .u64 => "uint64",
            .f16 => "float16",
            .f32 => "float32",
            .f64 => "float64",
        };
        const pa = try py.import("pyarrow");
        defer pa.decref();

        return pa.call0(py.PyObject, pa_dtype);
    }

    /// Convert the pyenc PType to a PyArrow DType.
    pub fn to_format_code(self: *const Self) !py.PyString {
        const format_str = switch (self.wrapped) {
            .i8 => "b",
            .u8 => "B",
            .i16 => "h",
            .u16 => "H",
            .i32 => "i",
            .u32 => "I",
            .i64 => "l",
            .u64 => "L",
            .f16 => "e",
            .f32 => "f",
            .f64 => "d",
        };
        return py.PyString.create(format_str);
    }

    pub usingnamespace py.zig(struct {
        pub fn wrap(ptype: enc.PType) !*const Self {
            return py.init(Self, .{ .wrapped = ptype });
        }

        pub fn unwrap(self: *const Self) enc.PType {
            return self.wrapped;
        }
    });
});
