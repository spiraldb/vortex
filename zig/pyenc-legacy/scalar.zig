const std = @import("std");
const py = @import("pydust");
const enc = @import("pyenc");
const pyenc = @import("./pyenc.zig");
const pyarrow = @import("./pyarrow.zig");
const pyio = @import("pyio.zig");

pub const Scalar = py.class(struct {
    const Self = @This();

    wrapped: enc.Scalar,

    dtype: py.property(struct {
        pub fn get(self: *const Self) !*const pyenc.DType {
            return try pyenc.DType.wrapOwned(try self.wrapped.getDType(pyenc.allocator()));
        }
    }) = .{},

    nbytes: py.property(struct {
        pub fn get(self: *const Self) usize {
            return self.wrapped.nbytes();
        }
    }) = .{},

    pub fn __del__(self: *const Self) void {
        self.wrapped.deinit();
    }

    pub fn __eq__(self: *const Self, other: *const Self) !bool {
        return self.wrapped.equal(other.wrapped);
    }

    /// Serialize Scalar to bytes and write it to the underlying writer, if no writer is provided the serialized bytes are returned
    pub fn to_bytes(self: *const Self, args: struct { writer: ?py.PyObject = null }) !?py.PyBytes {
        if (args.writer) |writer| {
            var pyWriter = try pyio.pythonWriter(writer);
            try self.wrapped.toBytes(pyWriter.writer());
            return null;
        } else {
            var buffer = std.ArrayList(u8).init(py.allocator);
            defer buffer.deinit();
            try self.wrapped.toBytes(buffer.writer());
            // TODO(robert): Construct PyBytes from a buffer protocol class to avoid copy
            return try py.PyBytes.create(buffer.items);
        }
    }

    pub usingnamespace py.zig(struct {
        pub fn unwrap(self: *const Self) enc.Scalar {
            return self.wrapped;
        }

        pub fn unwrapAlloc(self: *const Self, allocator: std.mem.Allocator) !enc.Scalar {
            return try self.wrapped.clone(allocator);
        }

        pub fn wrap(scalar: enc.Scalar, allocator: std.mem.Allocator) !*const Self {
            return try Self.wrapOwned(try scalar.clone(allocator));
        }

        pub fn wrapOwned(scalar: enc.Scalar) !*const Self {
            if (scalar == .extension) {
                return &(try pyenc.PyScalar.wrapOwned(scalar.extension)).scalar;
            }
            return try py.init(Self, .{ .wrapped = scalar });
        }

        pub fn enc_to_py(scalar: enc.Scalar) anyerror!py.PyObject {
            return switch (scalar) {
                .null => py.None(),
                .bool => |b| py.object(try py.PyBool.create(b.value)),
                .int => |w| switch (w) {
                    inline else => |i| py.object(try py.PyLong.create(i)),
                },
                .uint => |w| switch (w) {
                    inline else => |u| py.object(try py.PyLong.create(u)),
                },
                .float => |w| switch (w) {
                    inline else => |f| py.object(try py.PyFloat.create(f)),
                },
                .list => |l| blk: {
                    const pylist = try py.PyList.new(l.values.len);
                    for (l.values, 0..) |v, i| {
                        try pylist.setOwnedItem(i, try enc_to_py(v));
                    }
                    break :blk pylist.obj;
                },
                .utf8 => |s| py.object(try py.PyString.create(s.bytes)),
                .binary => |b| py.object(try py.PyBytes.create(b.bytes)),
                else => null,
            } orelse py.ValueError.raiseFmt("Cannot convert scalar of type {} into a Python object", .{scalar});
        }

        pub fn py_to_enc(obj: py.PyObject, dtype: ?enc.DType) !enc.Scalar {
            if (dtype) |dt| {
                if (dt == .extension) {
                    // For extension DTypes, we try and unwrap the scalar from the extension type.
                }
            }

            if (py.is_none(obj)) {
                return .null;
            }

            if (py.PyBool.checkedCast(obj)) |pybool| {
                if (dtype) |dt| if (dt != .bool) {
                    return py.TypeError.raiseFmt("Cannot convert Python bool into a scalar of type {}", .{dt});
                };
                return enc.BoolScalar.init(pybool.asbool());
            }

            if (py.PyLong.checkedCast(obj)) |pylong| {
                const dt = dtype orelse enc.DType{ .int = ._64 };

                if (dt == .uint and try pylong.as(i64) < 0) {
                    return py.ValueError.raiseFmt("Cannot convert negative Python int into a scalar of type {}", .{dt});
                }

                const width = switch (dt) {
                    .int, .uint => |int_width| if (int_width == .Unknown) enc.DType.IntWidth._64 else int_width,
                    else => return py.TypeError.raiseFmt("Cannot convert Python int into a scalar of type {}", .{dt}),
                };

                switch (dt == .int) {
                    inline else => |signed| {
                        switch (width) {
                            inline else => |w| {
                                const Int = @Type(.{ .Int = .{ .signedness = if (signed) .signed else .unsigned, .bits = comptime w.asInt() } });
                                const value = if (signed) try pylong.as(i64) else try pylong.as(u64);
                                if (value < std.math.minInt(Int) or value > std.math.maxInt(Int)) {
                                    return py.OverflowError.raiseFmt("Python int {} does not fit into a scalar of type {}", .{ value, dt });
                                }
                                return enc.Scalar.init(try pylong.as(Int));
                            },
                        }
                    },
                }
            }

            if (py.PyFloat.checkedCast(obj)) |pyfloat| {
                const dt = dtype orelse enc.DType{ .float = ._64 };

                const width = switch (dt) {
                    .float => |float_width| if (float_width == .Unknown) enc.DType.FloatWidth._64 else float_width,
                    else => return py.TypeError.raiseFmt("Cannot convert Python float into a scalar of type {}", .{dt}),
                };

                switch (width) {
                    inline else => |w| {
                        const Float = @Type(.{ .Float = .{ .bits = comptime w.asInt() } });
                        return enc.Scalar.init(try pyfloat.as(Float));
                    },
                }
            }

            if (py.PyString.checkedCast(obj)) |pystr| {
                const dt = dtype orelse .utf8;
                if (dt != .utf8) {
                    return py.TypeError.raiseFmt("Cannot convert Python str into a scalar of type {}", .{dt});
                }
                return enc.UTF8Scalar.initOwned(try pyenc.allocator().dupe(u8, try pystr.asSlice()), pyenc.allocator());
            }

            if (py.PyBytes.checkedCast(obj)) |pybytes| {
                const dt = dtype orelse .binary;
                if (dt != .binary) {
                    return py.TypeError.raiseFmt("Cannot convert Python bytes into a scalar of type {}", .{dt});
                }
                return enc.BinaryScalar.initOwned(try pyenc.allocator().dupe(u8, try pybytes.asSlice()), pyenc.allocator());
            }

            // Support conversion from PyArrow by extracting the raw as_py() value.
            const pa = try py.import("pyarrow");
            defer pa.decref();

            const pa_Scalar = try pa.get("Scalar");
            defer pa_Scalar.decref();

            if (try py.isinstance(obj, pa_Scalar)) {
                const pyscalar = try obj.call0(py.PyObject, "as_py");
                defer pyscalar.decref();

                const pa_type = try obj.get("type");
                defer pa_type.decref();

                const pa_enc_dtype = try pyarrow.dtypeFromPyArrow(pa_type);
                if (dtype) |dt| {
                    if (!pa_enc_dtype.equal(dt)) {
                        return py.ValueError.raiseFmt(
                            "Cannot convert PyArrow scalar of type {} into {}",
                            .{ pa_enc_dtype, dt },
                        );
                    }
                }

                return py_to_enc(pyscalar, pa_enc_dtype);
            }

            const typeName = try py.type_(obj).name();
            defer typeName.decref();

            return py.ValueError.raiseFmt("Cannot convert object of type {s} into a scalar", .{try typeName.asSlice()});
        }
    });

    pub fn __repr__(self: *const Self) !py.PyString {
        return try py.PyString.createFmt("{}", .{self.wrapped});
    }

    pub fn as_py(self: *const Self) !py.PyObject {
        return Self.enc_to_py(self.wrapped);
    }

    /// Cast this scalar into another dtype.
    pub fn cast(self: *const Self, args: struct { dtype: *const pyenc.DType }) !*const Self {
        const toCast = try self.unwrap().clone(pyenc.allocator());
        const casted = toCast.cast(pyenc.allocator(), args.dtype.unwrap()) catch |err| switch (err) {
            enc.Error.InvalidCast => {
                const currentDType = try self.unwrap().getDType(pyenc.allocator());
                defer currentDType.deinit();
                return py.TypeError.raiseFmt("Cannot cast scalar of type {} to {}", .{ currentDType, args.dtype.unwrap() });
            },
            else => |e| return e,
        };
        return Self.wrapOwned(casted);
    }

    /// Convert the pyenc scalar to a PyArrow Scalar.
    pub fn to_pyarrow(self: *const Self) !py.PyObject {
        const pa = try py.import("pyarrow");
        defer pa.decref();
        const encDType = try self.wrapped.getDType(pyenc.allocator());
        defer encDType.deinit();
        const pyarrowDType = try pyenc.DType.encToPyArrow(encDType);
        defer pyarrowDType.decref();

        return try pa.call(py.PyObject, "scalar", .{ try self.as_py(), pyarrowDType }, .{});
    }
});

pub const PyScalar = py.class(struct {
    const Self = @This();

    scalar: Scalar,
    ext_id: []const u8,
    gpa: std.mem.Allocator,
    dtype: enc.DType,

    pub fn __init__(self: *Self, args: struct { id: []const u8, dtype: *const pyenc.DType }) !void {
        const gpa = pyenc.allocator();
        const ext_id = try pyenc.allocator().dupe(u8, args.id);

        const ext: enc.ExtensionScalar = .{
            .id = ext_id,
            .ptr = py.object(self).py,
            .vtable = &Implementation.vtable,
        };

        self.* = .{
            .scalar = .{ .wrapped = .{ .extension = ext } },
            .ext_id = ext_id,
            .gpa = gpa,
            .dtype = try args.dtype.unwrap().clone(gpa),
        };
    }

    pub fn __del__(self: *Self) void {
        self.dtype.deinit();
        self.gpa.free(self.ext_id);
    }

    const Implementation = struct {
        const vtable: enc.ExtensionScalar.VTable = .{
            .clone = &Implementation.clone,
            .deinit = &Implementation.deinit,
            .equal = &Implementation.equal,
            .getDType = &Implementation.getDType,
        };

        fn clone(ptr: *anyopaque, gpa: std.mem.Allocator) !enc.ExtensionScalar {
            _ = gpa;
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
            const other: py.PyObject = .{ .py = @ptrCast(@alignCast(other_ptr)) };
            return py.eq(self, other) catch std.debug.panic("Failed to compare", .{});
        }

        fn getDType(ptr: *anyopaque, gpa: std.mem.Allocator) !enc.DType {
            const self: *const Self = py.as(*const Self, py.PyObject{ .py = @ptrCast(@alignCast(ptr)) }) catch std.debug.panic("TODO(ngates): pydust missing feature", .{});
            return self.dtype.clone(gpa);
        }
    };

    pub fn as_py(self: *const Self) !py.PyObject {
        _ = self;
        return py.ValueError.raise("PyScalar as_py() not implemented");
    }

    pub usingnamespace py.zig(struct {
        pub fn wrapOwned(ext_scalar: enc.ExtensionScalar) !*const Self {
            const object: py.PyObject = .{ .py = @ptrCast(@alignCast(ext_scalar.ptr)) };
            return py.as(*const Self, object);
        }
    });
});
