//! Python bindings for the pyenc library.
const py = @import("pydust");
const enc = @import("pyenc");
const pyarray = @import("./array.zig");
const pyenc = @import("../pyenc.zig");

pub const ConstantArray = py.class(struct {
    const Self = @This();

    pub usingnamespace pyarray.Subclass(Self, enc.ConstantArray);

    array: pyenc.Array,

    scalar: py.property(struct {
        pub fn get(self: *const Self) !*const pyenc.Scalar {
            const scalar = self.unwrap().scalar;
            return pyenc.Scalar.wrapOwned(try scalar.clone(pyenc.allocator()));
        }
    }) = .{},

    pub fn __str__(self: *const Self) !py.PyString {
        return try py.PyString.createFmt("ConstantArray({})", .{self.unwrap().scalar});
    }

    /// Class method for constructing a PrimitiveArray from a Python buffer.
    pub fn from_scalar(args: struct { scalar: *const pyenc.Scalar, length: usize }) !*const Self {
        const gpa = pyenc.allocator();
        return Self.wrapOwned(try enc.ConstantArray.allocWithOwnedScalar(gpa, try args.scalar.unwrap().clone(gpa), args.length));
    }
});
