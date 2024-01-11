//! Python bindings for the pyenc library.
const std = @import("std");
const enc = @import("pyenc");
const py = @import("pydust");
const pyenc = @import("./pyenc.zig");
const ptype = @import("ptype.zig");
const pyio = @import("pyio.zig");

pub const DType = py.class(struct {
    const Self = @This();

    wrapped: enc.DType,

    pub fn __del__(self: *const Self) void {
        self.wrapped.deinit();
    }

    pub fn __repr__(self: *const Self) !py.PyString {
        // Defer to the Zig representation of DType.
        return try py.PyString.createFmt("{}", .{self.wrapped});
    }

    pub fn __eq__(self: *const Self, other: *const Self) !bool {
        return self.wrapped.equal(other.wrapped);
    }

    pub fn __hash__(self: *const Self) usize {
        _ = self;
        var hasher = std.hash.Wyhash.init(0);
        //self.wrapped.hash(&hasher);
        return hasher.final();
    }

    pub fn to_ptype(self: *const Self) !?*const ptype.PType {
        return if (self.wrapped.toPType()) |p| ptype.PType.wrap(p) else null;
    }

    /// Serialize DType to bytes and write it to the underlying writer, if no writer is provided the serialized bytes are returned
    pub fn to_bytes(self: *const Self, args: struct { writer: ?py.PyObject = null }) !?py.PyBytes {
        if (args.writer) |writer| {
            var pyWriter = try pyio.pythonWriter(writer);
            try self.wrapped.toBytes(pyWriter.writer());
            return null;
        } else {
            var buffer = std.ArrayList(u8).init(pyenc.allocator());
            defer buffer.deinit();

            try self.wrapped.toBytes(buffer.writer());
            // TODO(robert): Construct PyBytes from a buffer protocol class to avoid copy
            return try py.PyBytes.create(buffer.items);
        }
    }

    /// Convert the pyenc DType to a PyArrow DType.
    pub fn to_pyarrow(self: *const Self) !py.PyObject {
        // Turns out it's actually faster to just call the PyArrow constructors directly.
        return Self.encToPyArrow(self.wrapped);

        // const pa = try py.import("pyarrow");
        // defer pa.decref();
        // const DataType = try pa.get("DataType");

        // const arrowSchema = pyenc.arrow.dtypeToArrow(self.wrapped, pyenc.allocator()) catch |err| switch (err) {
        //     error.ArrowConversionFailed => return py.ValueError.raiseFmt("Arrow conversion is not supported for DType {}", .{self}),
        //     else => return err,
        // };

        // return try DataType.call(py.PyObject, "_import_from_c", .{@intFromPtr(&arrowSchema)}, .{});
    }

    pub usingnamespace py.zig(struct {
        pub fn wrap(dtype: enc.DType, allocator: std.mem.Allocator) py.PyError!*const Self {
            return wrapOwned(try dtype.clone(allocator));
        }

        pub fn wrapOwned(dtype: enc.DType) py.PyError!*const Self {
            // TODO(ngates): this is where we should downcast into the appropriate DType subclass.
            // We currently don't have DType subclasses, except for PyDType. But we should,
            // since it would make accessing properties easier, e.g. IntDType.width
            if (dtype == .extension) {
                return &(try PyDType.wrapOwned(dtype.extension)).dtype;
            }

            return py.init(Self, .{ .wrapped = dtype });
        }

        pub inline fn unwrap(self: *const Self) enc.DType {
            return self.wrapped;
        }

        pub fn unwrapAlloc(self: *const Self, allocator: std.mem.Allocator) !enc.DType {
            return try self.wrapped.clone(allocator);
        }

        pub fn encToPyArrow(dtype: enc.DType) !py.PyObject {
            const pa = try py.import("pyarrow");
            defer pa.decref();

            switch (dtype) {
                .null => return try pa.call0(py.PyObject, "null"),
                .nullable => |n| return try encToPyArrow(n.child.*),
                .bool => return try pa.call0(py.PyObject, "bool_"),
                .int => |i| switch (i) {
                    .Unknown => {},
                    ._8 => return try pa.call0(py.PyObject, "int8"),
                    ._16 => return try pa.call0(py.PyObject, "int16"),
                    ._32 => return try pa.call0(py.PyObject, "int32"),
                    ._64 => return try pa.call0(py.PyObject, "int64"),
                },
                .uint => |i| switch (i) {
                    .Unknown => {},
                    ._8 => return try pa.call0(py.PyObject, "uint8"),
                    ._16 => return try pa.call0(py.PyObject, "uint16"),
                    ._32 => return try pa.call0(py.PyObject, "uint32"),
                    ._64 => return try pa.call0(py.PyObject, "uint64"),
                },
                .float => |f| switch (f) {
                    .Unknown => {},
                    ._16 => return try pa.call0(py.PyObject, "float16"),
                    ._32 => return try pa.call0(py.PyObject, "float32"),
                    ._64 => return try pa.call0(py.PyObject, "float64"),
                },
                .utf8 => return try pa.call0(py.PyObject, "utf8"),
                .binary => return try pa.call0(py.PyObject, "binary"),
                .struct_ => |s| {
                    const pa_fields = try py.PyList.new(s.fields.len);
                    for (s.names, s.fields, 0..) |name, field, i| {
                        // FIXME(ngates): we need to check if the field dtype is nullable and set this on PyArrow field.
                        try pa_fields.setItem(@intCast(i), try py.PyTuple.create(.{ name, try encToPyArrow(field) }));
                    }
                    return try pa.call(py.PyObject, "struct", .{pa_fields}, .{});
                },
                else => {},
            }
            return py.TypeError.raiseFmt("Cannot convert {} to PyArrow type", .{dtype});
        }
    });
});

pub const PyDType = py.class(struct {
    const Self = @This();

    dtype: DType,
    ext_id: []const u8,
    gpa: std.mem.Allocator,

    pub fn __init__(self: *Self, args: struct { id: []const u8 }) !void {
        const gpa = pyenc.allocator();
        const ext_id = try gpa.dupe(u8, args.id);

        const ext: enc.DType.Extension = .{
            .id = ext_id,
            .ptr = py.object(self).py,
            .vtable = &Implementation.vtable,
        };

        self.* = .{
            .dtype = .{ .wrapped = .{ .extension = ext } },
            .ext_id = ext_id,
            .gpa = gpa,
        };
    }

    pub fn __del__(self: *const Self) void {
        self.gpa.free(self.ext_id);
    }

    const Implementation = struct {
        const vtable: enc.DType.Extension.VTable = .{
            .clone = &Implementation.clone,
            .deinit = &Implementation.deinit,
            .equal = &Implementation.equal,
        };

        fn clone(ptr: *anyopaque) enc.DType.Extension {
            const self: *const Self = py.as(*const Self, py.PyObject{ .py = @ptrCast(@alignCast(ptr)) }) catch std.debug.panic("TODO(ngates): pydust missing feature", .{});
            py.incref(self);
            return .{ .id = self.ext_id, .ptr = ptr, .vtable = &vtable };
        }

        fn deinit(ptr: *anyopaque) void {
            const self: py.PyObject = .{ .py = @ptrCast(@alignCast(ptr)) };
            self.decref();
        }

        fn equal(ptr: *anyopaque, other_ptr: *anyopaque) bool {
            const self: py.PyObject = .{ .py = @ptrCast(@alignCast(ptr)) };
            _ = self;
            const other: py.PyObject = .{ .py = @ptrCast(@alignCast(other_ptr)) };
            _ = other;

            // TODO(ngates): the best comparison is the serialized metadata
            return true;
        }
    };

    pub usingnamespace py.zig(struct {
        pub fn wrapOwned(ext_dtype: enc.DType.Extension) !*const Self {
            return try py.as(*const Self, py.PyObject{ .py = @ptrCast(@alignCast(ext_dtype.ptr)) });
        }
    });
});
