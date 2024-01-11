//! Python bindings for the pyenc library.
const std = @import("std");
const py = @import("pydust");
const arrow = @import("arrow");
const enc = @import("pyenc");
const pyarrow = @import("../pyarrow.zig");
const pybuffer = @import("../pybuffer.zig");
const pyenc = @import("../pyenc.zig");

pub const Array = py.class(struct {
    const Self = @This();

    wrapped: *enc.Array,

    encoding: py.property(struct {
        pub fn get(self: *const Self) !py.PyString {
            return py.PyString.create(self.wrapped.id);
        }
    }) = .{},

    dtype: py.property(struct {
        pub fn get(self: *const Self) !*const pyenc.DType {
            return pyenc.DType.wrapOwned(try self.wrapped.dtype.clone(py.allocator));
        }
    }) = .{},

    nbytes: py.property(struct {
        pub fn get(self: *const Self) !usize {
            return try self.wrapped.getNBytes();
        }
    }) = .{},

    stats: py.property(struct {
        pub fn get(self: *const Self) !*const Stats {
            return py.init(Stats, .{ .array = self.unwrap().retain() });
        }
    }) = .{},

    pub fn __del__(self: *Self) void {
        self.wrapped.release();
    }

    pub fn __iter__(self: *const Self) !*const ScalarIterator {
        return try py.init(ScalarIterator, .{
            .array = self.wrapped.retain(),
            .index = 0,
        });
    }

    pub fn __len__(self: *const Self) !usize {
        return self.wrapped.len;
    }

    pub fn __getitem__(self: *const Self, item: py.PyObject) !py.PyObject {
        if (py.PyLong.checkedCast(item)) |index| {
            var idx = try index.as(isize);

            // Normalize negative indices
            if (idx < 0) idx = @as(isize, @intCast(self.wrapped.len)) + idx;

            const scalar = try self.wrapped.getScalar(py.allocator, @intCast(idx));
            return py.object(try pyenc.Scalar.wrapOwned(scalar));
        }

        if (py.PySlice.checkedCast(item)) |slice| {
            // TODO(ngates): validate slice bounds. Maybe using PySlice.adjustIndices?
            const start = try slice.getStart(?u64) orelse 0;
            const stop = try slice.getStop(?u64) orelse self.wrapped.len;
            const arraySlice = try self.wrapped.getSlice(py.allocator, start, stop);
            return py.object(try Self.wrapOwned(arraySlice));
        }

        const ArrayCls = try py.self(Self);
        defer ArrayCls.decref();

        if (try py.isinstance(item, ArrayCls)) {
            const itemArray: *const Self = try py.as(*const Self, item);

            const dtype = itemArray.wrapped.dtype;
            if (dtype == .bool) {
                return py.object(try Self.wrapOwned(try self.wrapped.getMasked(py.allocator, itemArray.wrapped)));
            } else if (dtype == .uint or dtype == .int) {
                // Note(ngates): we delegate this to pyenc.ops.take
                const elems = try enc.ops.take(try pyenc.ctx(), self.wrapped, itemArray.wrapped);
                return py.object(try Self.wrapOwned(elems));
            }
        }

        const typeName = try py.str(py.type_(item));
        defer typeName.decref();
        return py.NotImplementedError.raiseFmt("Array.__getitem__ not implemented for argument type {s}", .{try typeName.asSlice()});
    }

    /// Returns an iterator of plain pyenc arrays.
    pub fn iter_plain(self: *const Self) !*const PlainIterator {
        return try py.init(PlainIterator, .{
            .iter = try self.wrapped.iterPlain(py.allocator),
        });
    }

    /// Convert the pyenc Array into a Python list.
    pub fn to_pylist(self: *const Self) !py.PyObject {
        // TODO(ngates): do this conversion using pyenc scalars, rather than using PyArrow.
        const parr = try self.to_pyarrow();
        defer parr.decref();

        return try parr.call(py.PyObject, "to_pylist", .{}, .{});
    }

    /// Support for the PyArrow extension protocol enabling implicit conversions.
    pub fn __array__(self: *const Self, args: struct { dtype: ?py.PyObject = null }) !py.PyObject {
        // FIXME(ngates): raise a performance warning.
        // warnings.warn(PerformanceWarning(f"Implicit conversion from {self.__class__.__name__} to Numpy array"))
        const narr = try self.to_numpy();
        if (args.dtype) |np_type| {
            defer narr.decref();
            return narr.call(py.PyObject, "cast", .{np_type}, .{});
        }
        return narr;
    }

    /// Convert the pyenc Array into a Numpy array.
    pub fn to_numpy(self: *const Self) !py.PyObject {
        // TODO(ngates): use Numpy C API instead of conversion via PyArrow.
        const parr_chunked = try self.to_pyarrow();
        defer parr_chunked.decref();

        const parr = try parr_chunked.call0(py.PyObject, "combine_chunks");
        defer parr.decref();

        return try parr.call(py.PyObject, "to_numpy", .{}, .{ .zero_copy_only = false });
    }

    /// Support for the PyArrow extension protocol enabling implicit conversions.
    pub fn __arrow_array__(self: *const Self, args: struct { type: ?py.PyObject = null }) !py.PyObject {
        // FIXME(ngates): raise a performance warning.
        // warnings.warn(PerformanceWarning(f"Implicit conversion from {self.__class__.__name__} to PyArrow array"))
        const parr = try self.to_pyarrow();
        if (args.type) |pa_type| {
            defer parr.decref();
            return parr.call(py.PyObject, "view", .{pa_type}, .{});
        }
        return parr;
    }

    /// Collects iter_pyarrow chunks into a pa.ChunkedArray.
    pub fn to_pyarrow(self: *const Self) !py.PyObject {
        if (try self.wrapped.iterArrow(py.allocator)) |arrow_iter| {
            defer arrow_iter.deinit();

            const dtype = self.wrapped.dtype;

            const pa_type = try pyenc.DType.encToPyArrow(dtype);
            defer pa_type.decref();

            const chunks = try py.PyList.new(0);
            defer chunks.decref();
            while (try arrow_iter.next(pyenc.allocator())) |chunk| {
                const pychunk = try pyarrow.arrayToPyArrow(chunk, pa_type);
                defer pychunk.decref();

                try chunks.append(pychunk);
            }

            const pa = try py.import("pyarrow");
            defer pa.decref();

            return try pa.call(py.PyObject, "chunked_array", .{chunks}, .{});
        } else {
            const typeName = try py.str(py.type_(self));
            defer typeName.decref();
            return py.NotImplementedError.raiseFmt("Encoding {s} does not support to_pyarrow", .{try typeName.asSlice()});
        }
    }

    /// Iterate over chunks of a PyArrow array.
    pub fn iter_pyarrow(self: *const Self) !*const PyArrowIterator {
        if (try self.wrapped.iterArrow(py.allocator)) |arrow_iter| {
            const dtype = self.wrapped.dtype;

            return try py.init(PyArrowIterator, .{
                // Transfer ownership of the arrow_iter into the PyArrowIterator
                .arrow_iter = arrow_iter,
                .pa_type = try pyenc.DType.encToPyArrow(dtype),
            });
        } else {
            const typeName = try py.str(py.type_(self));
            defer typeName.decref();
            return py.NotImplementedError.raiseFmt("Encoding {s} does not support to_pyarrow", .{try typeName.asSlice()});
        }
    }

    pub usingnamespace py.zig(struct {
        pub fn unwrap(self: *const Self) *const enc.Array {
            return self.wrapped;
        }

        pub fn wrap(array: *const enc.Array) !*const Self {
            return Self.wrapOwned(array.retain());
        }

        pub fn wrapOwned(array: *enc.Array) !*const Self {
            return if (array.kind) |k| switch (k) {
                inline .binary => &(try pyenc.BinaryArray.wrapOwned(enc.BinaryArray.from(array))).array,
                inline .bool => &(try pyenc.BoolArray.wrapOwned(enc.BoolArray.from(array))).array,
                inline .chunked => &(try pyenc.ChunkedArray.wrapOwned(enc.ChunkedArray.from(array))).array,
                inline .constant => &(try pyenc.ConstantArray.wrapOwned(enc.ConstantArray.from(array))).array,
                inline .dictionary => &(try pyenc.DictionaryArray.wrapOwned(enc.DictionaryArray.from(array))).array,
                inline .patched => &(try pyenc.PatchedArray.wrapOwned(enc.PatchedArray.from(array))).array,
                inline .primitive => &(try pyenc.PrimitiveArray.wrapOwned(enc.PrimitiveArray.from(array))).array,
                inline .roaring_bool => &(try pyenc.RoaringBoolArray.wrapOwned(enc.RoaringBoolArray.from(array))).array,
                inline .roaring_uint => &(try pyenc.RoaringUIntArray.wrapOwned(enc.RoaringUIntArray.from(array))).array,
                inline .struct_ => &(try pyenc.StructArray.wrapOwned(enc.StructArray.from(array))).array,
            } else blk: {
                // TODO(ngates): check if the array is a PyArray.
                // If so, we cast its ptr into the underlying Python object. Otherwise, we return a generic pyenc.Array.
                const pyExt = pyenc.PyExtensionArray.from(array);
                break :blk py.as(*const Self, pyExt.delegate);
            };
        }
    });
});

pub const Stats = py.class(struct {
    const Self = @This();

    array: *enc.Array,

    pub fn __del__(self: *Self) void {
        self.array.release();
    }

    pub fn __getattr__(self: *const Self, item: py.PyString) !py.PyObject {
        const stat = try self.getStatistic(try item.asSlice());
        defer stat.deinit();
        return pyenc.Scalar.enc_to_py(stat);
    }

    pub fn scalar(self: *const Self, args: struct { item: py.PyString }) !*const pyenc.Scalar {
        const stat = try self.getStatistic(try args.item.asSlice());
        return try pyenc.Scalar.wrapOwned(stat);
    }

    fn getStatistic(self: *const Self, item: []const u8) !enc.Scalar {
        const stat = parseStat(item) orelse return py.ValueError.raiseFmt("No statistic named '{s}'", .{item});
        return try self.array.computeStatistic(pyenc.allocator(), stat);
    }

    fn parseStat(item: []const u8) ?enc.Stats.Stat {
        inline for (comptime std.enums.values(enc.Stats.Stat)) |stat_tag| {
            if (std.mem.eql(u8, item, @tagName(stat_tag))) {
                return stat_tag;
            }
        }
        return null;
    }
});

// TODO(ngates): make all our iterators nested classes
pub const ScalarIterator = py.class(struct {
    const Self = @This();

    array: *enc.Array,
    index: usize = 0,

    pub fn __del__(self: *Self) void {
        self.array.release();
    }

    pub fn __next__(self: *Self) !?*const pyenc.Scalar {
        if (self.index < self.array.len) {
            const scalar = try self.array.getScalar(py.allocator, self.index);
            self.index += 1;
            return try pyenc.Scalar.wrapOwned(scalar);
        }
        return null;
    }
});

pub const PlainIterator = py.class(struct {
    const Self = @This();

    iter: enc.Array.Iterator,

    pub fn __del__(self: *Self) void {
        self.iter.deinit();
    }

    pub fn __iter__(self: *const Self) *const Self {
        py.incref(self);
        return self;
    }

    pub fn __next__(self: *Self) !?*const pyenc.Array {
        if (try self.iter.next(pyenc.allocator())) |array| {
            return try pyenc.Array.wrapOwned(array);
        }
        return null;
    }
});

pub const PyArrowIterator = py.class(struct {
    const Self = @This();

    arrow_iter: enc.arrow.Iterator,
    pa_type: py.PyObject,

    pub fn __del__(self: *Self) void {
        self.arrow_iter.deinit();
        self.pa_type.decref();
    }

    pub fn __iter__(self: *const Self) *const Self {
        py.incref(self);
        return self;
    }

    pub fn __next__(self: *Self) !?py.PyObject {
        if (try self.arrow_iter.next(pyenc.allocator())) |arrow_array| {
            return try pyarrow.arrayToPyArrow(arrow_array, self.pa_type);
        }
        return null;
    }
});

/// Comptime function for exporting default methods of a pyenc.Array subclass.
pub fn Subclass(comptime Self: type, comptime EncSelf: type) type {
    return struct {
        pub usingnamespace py.zig(struct {
            pub fn unwrap(self: *const Self) *const EncSelf {
                return EncSelf.from(self.array.wrapped);
            }

            pub fn wrap(array: *const EncSelf) !*const Self {
                return Self.wrapOwned(array.retain());
            }

            pub fn wrapOwned(array: *EncSelf) !*const Self {
                return py.init(Self, .{ .array = .{ .wrapped = &array.array } });
            }
        });
    };
}
