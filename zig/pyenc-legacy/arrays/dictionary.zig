//! Python bindings for the pyenc library.
const py = @import("pydust");
const enc = @import("pyenc");
const pyarray = @import("./array.zig");
const pyenc = @import("../pyenc.zig");

pub const DictionaryArray = py.class(struct {
    const Self = @This();

    pub usingnamespace pyarray.Subclass(Self, enc.DictionaryArray);

    array: pyenc.Array,

    dictionary: py.property(struct {
        pub fn get(self: *const Self) !*const pyenc.Array {
            return pyenc.Array.wrapOwned(self.unwrap().dictionary.retain());
        }
    }) = .{},

    codes: py.property(struct {
        pub fn get(self: *const Self) !*const pyenc.Array {
            return pyenc.Array.wrapOwned(self.unwrap().codes.retain());
        }
    }) = .{},

    pub fn __str__(self: *const Self) !py.PyString {
        _ = self;
        return try py.PyString.createFmt("DictionaryArray()", .{});
    }

    pub fn from_codes_and_dict(args: struct { codes: *const pyenc.Array, dict: *const pyenc.Array }) !*const Self {
        return Self.wrapOwned(try enc.DictionaryArray.fromOwnedCodesAndDict(pyenc.allocator(), args.codes.unwrap().retain(), args.dict.unwrap().retain()));
    }

    pub fn encode(args: struct { array: *const pyenc.Array }) !*const Self {
        return Self.wrapOwned(try enc.DictionaryArray.encode(pyenc.allocator(), args.array.unwrap()));
    }
});
