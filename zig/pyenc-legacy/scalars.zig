const py = @import("pydust");
const pyenc = @import("./pyenc.zig");
const pyio = @import("./pyio.zig");
const enc = @import("pyenc");

pub const scalars = py.module(struct {
    /// Read Scalar from binary representation. The bytes should come from calling Scalar.to_bytes()
    pub fn from_bytes(args: struct { reader: py.PyObject }) !*const pyenc.Scalar {
        var pyReader = try pyio.pythonReader(args.reader);
        return pyenc.Scalar.wrapOwned(try enc.Scalar.fromBytes(pyReader.reader(), py.allocator));
    }
});
