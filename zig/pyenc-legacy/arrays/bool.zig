//! Python bindings for the pyenc library.
const std = @import("std");
const py = @import("pydust");
const enc = @import("pyenc");
const pyarray = @import("./array.zig");
const pybuffer = @import("../pybuffer.zig");
const pyenc = @import("../pyenc.zig");
const pyio = @import("../pyio.zig");

/// A pyenc wrapper around a bool array.
pub const BoolArray = py.class(struct {
    const Self = @This();

    pub usingnamespace pyarray.Subclass(Self, enc.BoolArray);

    array: pyenc.Array,

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

    pub fn from_buffer(args: struct { buffer: py.PyObject, offset: usize = 0, length: ?usize = null }) !*const Self {
        const buffer = try pybuffer.bufferFromPyBuffer(pyenc.allocator(), args.buffer);
        const len = args.length orelse buffer.bytes.len * 8;
        return Self.wrapOwned(try enc.BoolArray.allocWithOwnedBuffer(
            pyenc.allocator(),
            buffer,
            args.offset,
            len,
        ));
    }

    /// Class method for constructing a BoolArray from a PyArrow array.
    pub fn from_pyarrow(args: struct { arrow: py.PyObject }) !*const Self {
        const pa_offset = try args.arrow.get("offset");
        defer pa_offset.decref();

        // Grab the first PyArrow buffer
        const pyarrow_buffers = py.PyList.unchecked(try args.arrow.call0(py.PyObject, "buffers"));
        defer pyarrow_buffers.decref();

        const pyarrow_buffer = try pyarrow_buffers.getItem(py.PyObject, 1); // Borrowed reference

        const buffer = try pybuffer.bufferFromPyBuffer(pyenc.allocator(), pyarrow_buffer);
        const bool_array = try enc.BoolArray.allocWithOwnedBuffer(
            pyenc.allocator(),
            buffer,
            try py.as(usize, pa_offset),
            try py.len(args.arrow),
        );

        return Self.wrapOwned(bool_array);
    }

    /// Serialize BoolArray to bytes and write it to the underlying writer, if no writer is provided the serialized bytes are returned
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

    /// Read BoolArray from binary representation. The bytes should come from calling BoolArray.to_bytes()
    pub fn from_bytes(args: struct { reader: py.PyObject }) !*const Self {
        var pyReader = try pyio.pythonReader(args.reader);
        return Self.wrapOwned(try enc.BoolArray.fromBytes(pyReader.reader(), pyenc.allocator()));
    }

    pub fn __str__(self: *const Self) !py.PyString {
        _ = self;
        return try py.PyString.create("BoolArray()");
    }
});
