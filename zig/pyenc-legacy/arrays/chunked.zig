//! Python bindings for the pyenc library.
const py = @import("pydust");
const enc = @import("pyenc");
const pyarray = @import("./array.zig");
const pyenc = @import("../pyenc.zig");

/// A pyenc wrapper around a primitive array.
pub const ChunkedArray = py.class(struct {
    const Self = @This();

    pub usingnamespace pyarray.Subclass(Self, enc.ChunkedArray);

    array: pyenc.Array,

    chunks: py.property(struct {
        pub fn get(self: *const Self) !py.PyTuple {
            const chunks = try py.PyTuple.new(self.unwrap().chunks.len);
            for (self.unwrap().chunks, 0..) |f, i| {
                try chunks.setOwnedItem(i, try pyenc.Array.wrapOwned(f.retain()));
            }
            return chunks;
        }
    }) = .{},

    pub fn __str__(self: *const Self) !py.PyString {
        _ = self;
        // todo(wmanning): stringify the chunks
        return try py.PyString.createFmt("ChunkedArray()", .{});
    }

    pub fn to_pylist(self: *const Self) !py.PyObject {
        // We override this because the default implementation goes through PyArrow and doesn't work for PyObjectDType.
        const result = try py.PyList.new(self.unwrap().array.len);
        var curIdx: usize = 0;
        for (self.unwrap().chunks) |chunk| {
            const chunkList = py.PyList.unchecked(try (try pyenc.Array.wrapOwned(chunk)).to_pylist());
            defer chunkList.decref();
            for (0..chunkList.length()) |i| {
                try result.setItem(curIdx, try chunkList.getItem(py.PyObject, @intCast(i)));
                curIdx += 1;
            }
        }

        return result.obj;
    }

    /// Class method for constructing a ChunkedArray from a list of Python buffer.
    pub fn from_arrays(args: struct { chunks: py.PyList }) !*const Self {
        const nchunks = try py.len(args.chunks);

        const gpa = pyenc.allocator();
        const chunks = try gpa.alloc(*enc.Array, nchunks);

        for (0..nchunks) |idx| {
            // PyList item holds borrowed references. So no need to decref.
            const item = try args.chunks.getItem(*const pyenc.Array, @intCast(idx));
            chunks[idx] = item.unwrap().retain();
        }

        return Self.wrapOwned(try enc.ChunkedArray.allocWithOwnedChunks(
            pyenc.allocator(),
            chunks,
        ));
    }

    pub fn from_pyarrow(args: struct { arrow: py.PyObject }) !*const Self {
        const pa_type = try args.arrow.get("type");
        defer pa_type.decref();

        // Grab the chunks
        const pyarrow_chunks = py.PyList.unchecked(try args.arrow.call0(py.PyObject, "chunks"));
        defer pyarrow_chunks.decref();

        const gpa = pyenc.allocator();
        const length = pyarrow_chunks.length();

        var chunks = try gpa.alloc(*enc.Array, length);

        for (0..length) |i| {
            const tempchunk = try pyarrow_chunks.getItem(py.PyObject, @intCast(i));
            chunks[i] = @constCast(&(try pyenc.PrimitiveArray.from_pyarrow(.{ .arrow = tempchunk })).unwrap().array);
        }

        const chunkedArray = try enc.ChunkedArray.allocWithOwnedChunks(gpa, chunks);

        return Self.wrapOwned(chunkedArray);
    }
});
