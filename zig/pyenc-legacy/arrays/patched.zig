//! Python bindings for the pyenc library.
const std = @import("std");
const py = @import("pydust");
const enc = @import("pyenc");
const pyarray = @import("./array.zig");
const pybuffer = @import("../pybuffer.zig");
const pyenc = @import("../pyenc.zig");
const pyio = @import("../pyio.zig");

/// A pyenc wrapper around a bool array.
pub const PatchedArray = py.class(struct {
    const Self = @This();

    pub usingnamespace pyarray.Subclass(Self, enc.PatchedArray);

    array: pyenc.Array,

    patch_count: py.property(struct {
        pub fn get(self: *const Self) !*const pyenc.Scalar {
            return pyenc.Scalar.wrapOwned(enc.Scalar.init(self.unwrap().patchValues.len));
        }
    }) = .{},

    pub fn __str__(self: *const Self) !py.PyString {
        _ = self;
        return try py.PyString.create("PatchedArray()");
    }
});
