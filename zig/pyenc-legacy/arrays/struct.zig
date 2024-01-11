//! Python bindings for the pyenc library.
const std = @import("std");
const py = @import("pydust");
const arrow = @import("arrow");
const enc = @import("pyenc");
const pyarray = @import("./array.zig");
const pyarrow = @import("../pyarrow.zig");
const pybuffer = @import("../pybuffer.zig");
const pyenc = @import("../pyenc.zig");

/// A pyenc wrapper around a primitive array.
pub const StructArray = py.class(struct {
    const Self = @This();

    pub usingnamespace pyarray.Subclass(Self, enc.StructArray);

    array: pyenc.Array,

    names: py.property(struct {
        pub fn get(self: *const Self) !py.PyTuple {
            const names = try py.PyTuple.new(self.unwrap().names.len);
            for (self.unwrap().names, 0..) |name, i| {
                try names.setOwnedItem(i, try py.PyString.create(name));
            }
            return names;
        }
    }) = .{},

    fields: py.property(struct {
        pub fn get(self: *const Self) !py.PyTuple {
            const fields = try py.PyTuple.new(self.unwrap().fields.len);
            for (self.unwrap().fields, 0..) |f, i| {
                try fields.setOwnedItem(i, try pyenc.Array.wrapOwned(f.retain()));
            }
            return fields;
        }
    }) = .{},

    pub fn __str__(self: *const Self) !py.PyString {
        _ = self;
        return try py.PyString.createFmt("StructArray()", .{});
    }

    pub fn field(self: *const Self, args: struct { name: py.PyString }) !*const pyenc.Array {
        if (self.unwrap().findField(try args.name.asSlice())) |f| {
            return pyenc.Array.wrapOwned(f.retain());
        }
        return py.KeyError.raiseFmt("Struct array has no field {s}", .{try args.name.asSlice()});
    }

    /// Class method for constructing a PrimitiveArray from a Python buffer.
    pub fn from_arrays(args: struct { fields: py.PyDict }) !*const Self {
        const nfields = try py.len(args.fields);

        const gpa = pyenc.allocator();

        const names = try gpa.alloc([]const u8, nfields);
        const fields = try gpa.alloc(*enc.Array, nfields);

        var iter = args.fields.itemsIterator();
        var idx: usize = 0;
        while (iter.next()) |item| {
            // PyDict item holds borrowed references to key/value. So no need to decref.
            names[idx] = try gpa.dupe(u8, try item.key([]const u8));
            const f = try item.value(*const pyenc.Array);
            fields[idx] = f.unwrap().retain();
            idx += 1;
        }
        std.debug.assert(idx == nfields);

        return Self.wrapOwned(try enc.StructArray.allocWithOwnedNamesAndFields(
            pyenc.allocator(),
            names,
            fields,
        ));
    }
});
