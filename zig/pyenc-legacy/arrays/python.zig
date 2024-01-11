//! Python bindings for the pyenc library.
const std = @import("std");
const arrow = @import("arrow");
const py = @import("pydust");
const enc = @import("pyenc");
const pyenc = @import("../pyenc.zig");

/// An pyenc array that delegates its implementation to a Python object.
pub const PyArray = py.class(struct {
    const Self = @This();

    array: pyenc.Array,

    // The PyArray ExtensionArray is in fact stored inline on this Python object.
    // This allows us to use Python's reference counting and exactly tie the lifetime
    // of the Python-side implementation to the Zig-side implementation.
    pyObjArray: PyExtensionArray,
    gpa: std.mem.Allocator,

    nbytes: py.property(struct {
        pub fn get(self: *const Self) !usize {
            return self.notImplemented("nbytes");
        }
    }) = .{},

    pub fn __init__(self: *Self, args: struct { id: py.PyString, length: usize, dtype: *const pyenc.DType }) !void {
        self.* = .{
            .array = .{ .wrapped = &self.pyObjArray.array },
            .pyObjArray = .{
                .delegate = py.object(self),
                .array = try enc.Array.init(
                    try args.id.asSlice(),
                    &encoding,
                    pyenc.allocator(),
                    try args.dtype.unwrap().clone(pyenc.allocator()),
                    args.length,
                ),
            },
            .gpa = pyenc.allocator(),
        };
    }

    pub fn __del__(self: *Self) void {
        // We do not call super.__del__ since we would recursively call &encoding.release
        // Instead, we deallocate anything we allocated in __init__
        self.pyObjArray.array.deinit();
    }

    pub fn iter_plain(self: *const Self) !void {
        return self.notImplemented("iter_plain");
    }

    pub fn getSlice(self: *const Self) !void {
        return self.notImplemented("getSlice");
    }

    pub fn getScalar(self: *const Self) !void {
        return self.notImplemented("getScalar");
    }

    pub fn getMasked(self: *const Self) !void {
        return self.notImplemented("getMasked");
    }

    pub fn getElements(self: *const Self, args: struct { array: *const pyenc.PrimitiveArray }) !void {
        _ = args;
        return self.notImplemented("getElements");
    }

    fn notImplemented(self: *const Self, name: []const u8) py.PyError {
        const typeName = try py.str(py.type_(self));
        defer typeName.decref();
        return py.NotImplementedError.raiseFmt("PyArray {s} does not override {s}", .{ try typeName.asSlice(), name });
    }
});

const encoding = enc.VTable.Lift(PyExtensionArray);

pub const PyExtensionArray = struct {
    const Self = @This();
    delegate: py.PyObject,
    array: enc.Array,

    pub inline fn from(array: anytype) enc.Array.Downcast(@TypeOf(array), Self) {
        return @fieldParentPtr(Self, "array", array);
    }

    pub fn retain(self: *const Self) *enc.Array {
        self.delegate.incref();
        return @constCast(&self.array);
    }

    pub fn release(self: *Self) void {
        self.delegate.decref();
    }

    pub fn getNBytes(self: *const Self) !usize {
        const gil = py.gil();
        defer gil.release();

        const nbytes = try self.delegate.get("nbytes");
        defer nbytes.decref();
        return try py.as(usize, nbytes);
    }

    pub fn getScalar(self: *const Self, allocator: std.mem.Allocator, index: usize) !enc.Scalar {
        const gil = py.gil();
        defer gil.release();

        const scalar = try self.delegate.call(*const pyenc.Scalar, "getScalar", .{index}, .{});
        defer py.decref(scalar);

        return try scalar.unwrapAlloc(allocator);
    }

    pub fn getSlice(self: *const Self, allocator: std.mem.Allocator, start: usize, stop: usize) !*enc.Array {
        _ = allocator;
        const gil = py.gil();
        defer gil.release();

        const sliced = try self.delegate.call(*const pyenc.Array, "getSlice", .{ start, stop }, .{});
        defer py.decref(sliced);

        return sliced.unwrap().retain();
    }

    pub fn getMasked(self: *const Self, allocator: std.mem.Allocator, mask: *const enc.Array) !*enc.Array {
        _ = allocator;
        const gil = py.gil();
        defer gil.release();

        const wrappedMask = try pyenc.Array.wrap(mask);
        defer py.decref(wrappedMask);

        const masked = try self.delegate.call(
            *const pyenc.Array,
            "getMasked",
            .{wrappedMask},
            .{},
        );
        defer py.decref(masked);

        return masked.unwrap().retain();
    }

    pub fn getElements(self: *const Self, allocator: std.mem.Allocator, indices: *const enc.PrimitiveArray) !*enc.Array {
        _ = allocator;
        const gil = py.gil();
        defer gil.release();

        const wrappedIdx = try pyenc.PrimitiveArray.wrap(indices);
        defer py.decref(wrappedIdx);

        const elements = try self.delegate.call(
            *const pyenc.Array,
            "getElements",
            .{wrappedIdx},
            .{},
        );
        defer py.decref(elements);

        return elements.unwrap().retain();
    }

    pub inline fn computeStatistic(self: *const Self, allocator: std.mem.Allocator, stat: enc.Stats.Stat) !enc.Scalar {
        _ = stat;
        _ = allocator;
        _ = self;
        return enc.Error.StatisticNotSupported;
    }

    pub fn iterPlain(self: *const Self, allocator: std.mem.Allocator) !enc.Array.Iterator {
        const gil = py.gil();
        defer gil.release();

        return enc.Array.Iterator.WithState(struct {
            const Iter = @This();

            pyiter: py.PyIter,

            pub fn next(state: *Iter, gpa: std.mem.Allocator) !?*enc.Array {
                _ = gpa;
                const gil2 = py.gil();
                defer gil2.release();

                if (try state.pyiter.next(*const pyenc.Array)) |pyencArray| {
                    // We decref the Python object, and unwrap the internal pyenc.Array
                    defer py.decref(pyencArray);

                    return pyencArray.unwrap().retain();
                }

                return null;
            }

            pub fn deinit(state: *Iter) void {
                state.pyiter.decref();
            }
        }).alloc(allocator, .{
            .pyiter = try self.delegate.call0(py.PyIter, "iter_plain"),
        });
    }

    pub fn exportToArrow(self: *const Self, allocator: std.mem.Allocator) !arrow.Array {
        _ = allocator;
        const gil = py.gil();
        defer gil.release();

        // TODO(ngates): which function should we use to convert? to_pyarrow returns chunked array,
        // maybe we just need export_to_arrow? This only needs to be implemented for "plain" encodings,
        // so we should be able to remove this once all plain encodings are moved into Zig pyenc.
        const parr = try self.delegate.call0(py.PyObject, "__enc_export_pyarrow__");
        defer parr.decref();

        var arrow_array: arrow.Array = undefined;
        try parr.call(void, "_export_to_c", .{@intFromPtr(&arrow_array)}, .{});

        return arrow_array;
    }
};
