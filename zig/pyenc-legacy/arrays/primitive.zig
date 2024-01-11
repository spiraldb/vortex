//! Python bindings for the pyenc library.
const std = @import("std");
const py = @import("pydust");
const arrow = @import("arrow");
const enc = @import("pyenc");
const pyarray = @import("./array.zig");
const pyarrow = @import("../pyarrow.zig");
const pybuffer = @import("../pybuffer.zig");
const pyenc = @import("../pyenc.zig");
const pyio = @import("../pyio.zig");

/// A pyenc wrapper around a primitive array.
pub const PrimitiveArray = py.class(struct {
    const Self = @This();

    pub usingnamespace pyarray.Subclass(Self, enc.PrimitiveArray);

    array: pyenc.Array,

    ptype: py.property(struct {
        pub fn get(self: *const Self) !*const pyenc.PType {
            return py.init(pyenc.PType, .{ .wrapped = self.unwrap().ptype });
        }
    }) = .{},

    buffer: py.property(struct {
        pub fn get(self: *const Self) !*const pyenc.Buffer {
            return py.init(pyenc.Buffer, .{ .wrapped = self.unwrap().buffer.retain() });
        }
    }) = .{},

    offset: py.property(struct {
        pub fn get(self: *const Self) !usize {
            return self.unwrap().offset;
        }
    }) = .{},

    /// Serialize PrimitiveArray to bytes and write it to the underlying writer, if no writer is provided the serialized bytes are returned
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

    /// Read PrimitiveArray from binary representation. The bytes should come from calling PrimitiveArray.to_bytes()
    pub fn from_bytes(args: struct { reader: py.PyObject }) !*const Self {
        var pyReader = try pyio.pythonReader(args.reader);
        return Self.wrapOwned(try enc.PrimitiveArray.fromBytes(pyReader.reader(), pyenc.allocator()));
    }

    pub fn __str__(self: *const Self) !py.PyString {
        return try py.PyString.createFmt("PrimitiveArray({s})", .{self.unwrap().ptype.name()});
    }

    pub fn getSlice(self: *const Self, args: struct { start: usize, stop: usize }) !*const pyenc.Array {
        const sliced = try self.unwrap().array.getSlice(py.allocator, args.start, args.stop);
        return try pyenc.Array.wrapOwned(sliced);
    }

    pub fn view(self: *const Self, args: struct { ptype: *const pyenc.PType }) !*const Self {
        const cur_ptype = self.unwrap().ptype;
        const new_ptype = args.ptype.wrapped;
        if (cur_ptype.sizeOf() != new_ptype.sizeOf()) {
            return py.ValueError.raiseFmt("Cannot create {} view over {} array", .{ new_ptype, cur_ptype });
        }

        return Self.wrapOwned(try self.unwrap().view(pyenc.allocator(), new_ptype));
    }

    /// Class method for constructing a PrimitiveArray from a Python buffer.
    pub fn from_buffer(args: struct {
        buffer: py.PyObject,
        ptype: *const pyenc.PType,
        offset: usize = 0,
        length: ?usize = null,
    }) !*const Self {
        const buffer = try pybuffer.bufferFromPyBuffer(pyenc.allocator(), args.buffer);
        const ptype = args.ptype.wrapped;

        if (buffer.bytes.len % ptype.sizeOf() != 0) {
            return py.ValueError.raise("Buffer size must be a multiple of the size of the primitive type");
        }
        const len = args.length orelse buffer.bytes.len / ptype.sizeOf();

        const primitive_array = try enc.PrimitiveArray.allocWithOwnedBuffer(
            pyenc.allocator(),
            buffer,
            args.ptype.wrapped,
            args.offset,
            len,
        );

        return Self.wrapOwned(primitive_array);
    }

    /// Class method for constructing a PrimitiveArray from a PyArrow array.
    pub fn from_pyarrow(args: struct { arrow: py.PyObject }) !*const Self {
        const pa_type = try args.arrow.get("type");
        defer pa_type.decref();

        const pa_offset = try args.arrow.get("offset");
        defer pa_offset.decref();

        // Grab the first PyArrow buffer
        const pyarrow_buffers = py.PyList.unchecked(try args.arrow.call0(py.PyObject, "buffers"));
        defer pyarrow_buffers.decref();

        const pyarrow_buffer = try pyarrow_buffers.getItem(py.PyObject, 1); // Borrowed reference

        const buffer = try pybuffer.bufferFromPyBuffer(pyenc.allocator(), pyarrow_buffer);
        const primitive_array = try enc.PrimitiveArray.allocWithOwnedBuffer(
            pyenc.allocator(),
            buffer,
            try pyarrow.ptypeFromPyArrow(pa_type),
            try py.as(usize, pa_offset),
            try py.len(args.arrow),
        );

        return Self.wrapOwned(primitive_array);
    }
});
