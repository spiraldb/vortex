//! Python bindings for the pyenc library.
const std = @import("std");
const py = @import("pydust");
const enc = @import("pyenc");
const pyarray = @import("./array.zig");
const pyenc = @import("../pyenc.zig");
const pyio = @import("../pyio.zig");

pub const BinaryArray = py.class(struct {
    const Self = @This();

    pub usingnamespace pyarray.Subclass(Self, enc.BinaryArray);

    array: pyenc.Array,

    views: py.property(struct {
        pub fn get(self: *const Self) !*const pyenc.Buffer {
            return py.init(pyenc.Buffer, .{ .wrapped = self.unwrap().views.retain() });
        }
    }) = .{},

    data: py.property(struct {
        pub fn get(self: *const Self) !py.PyTuple {
            const arrayTuple = try py.PyTuple.new(self.unwrap().data.len);
            for (self.unwrap().data, 0..) |arr, i| {
                try arrayTuple.setOwnedItem(i, try pyenc.Array.wrapOwned(arr.retain()));
            }
            return arrayTuple;
        }
    }) = .{},

    offset: py.property(struct {
        pub fn get(self: *const Self) !usize {
            return self.unwrap().offset;
        }
    }) = .{},

    pub fn __str__(self: *const Self) !py.PyString {
        _ = self;
        return try py.PyString.createFmt("BinaryArray()", .{});
    }

    pub fn from_offsets_and_data(args: struct { offsets: *const pyenc.PrimitiveArray, data: *const pyenc.PrimitiveArray }) !*const Self {
        return Self.wrapOwned(try enc.BinaryArray.fromOffsetsAndData(pyenc.allocator(), args.offsets.unwrap(), args.data.unwrap()));
    }

    pub fn from_views_and_data(args: struct { views: *const pyenc.Buffer, data: py.PyList }) !*const Self {
        const dataArrays = try pyenc.allocator().alloc(*enc.Array, args.data.length());
        for (dataArrays, 0..) |*arr, i| {
            const pyencArray = try args.data.getItem(*const pyenc.Array, @intCast(i));
            arr.* = pyencArray.wrapped;
        }
        return Self.wrapOwned(try enc.BinaryArray.allocWithOwnedArrays(pyenc.allocator(), args.views.wrapped.retain(), dataArrays, 0, null));
    }

    /// Serialize BinaryArray to bytes and write it to the underlying writer, if no writer is provided the serialized bytes are returned
    pub fn to_bytes(self: *const Self, args: struct { writer: ?py.PyObject = null }) !?py.PyBytes {
        if (args.writer) |writer| {
            var pyWriter = try pyio.pythonWriter(writer);
            try self.unwrap().toBytes(pyWriter.writer());
            return null;
        } else {
            var buffer = std.ArrayList(u8).init(pyenc.allocator());
            defer buffer.deinit();

            try self.unwrap().toBytes(buffer.writer());
            // TODO(robert): Construct PyBytes from a buffer protocol class to avoid copy
            return try py.PyBytes.create(buffer.items);
        }
    }

    /// Read BinaryArray from binary representation. The bytes should come from calling BinaryArray.to_bytes()
    pub fn from_bytes(args: struct { reader: py.PyObject }) !*const Self {
        var pyReader = try pyio.pythonReader(args.reader);
        return Self.wrapOwned(try enc.BinaryArray.fromBytes(pyReader.reader(), pyenc.allocator()));
    }
});
