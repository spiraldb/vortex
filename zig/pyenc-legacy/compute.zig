const std = @import("std");
const py = @import("pydust");
const enc = @import("pyenc");
const ec = enc.compute;
const pyarrow = @import("./pyarrow.zig");
const pyenc = @import("./pyenc.zig");
const pybuffer = @import("./pybuffer.zig");
const pretty = @import("pretty");

pub const compute = py.module(struct {
    pub fn __exec__(mod: py.PyModule) !void {
        inline for (@typeInfo(enc.ops).Struct.decls) |d| {
            const decl = @field(enc.ops, d.name);
            if (@typeInfo(@TypeOf(decl)) != .Type) continue;
            if (!@hasDecl(decl, "function")) continue;

            var function = try decl.function(pyenc.allocator());
            defer function.deinit();

            const pyfunction = py.object(try py.init(ComputeFunction, .{
                .function_name = try pyenc.allocator().dupe(u8, function.name),
                .gpa = pyenc.allocator(),
            }));

            const name = try pyenc.allocator().dupeZ(u8, function.name);
            defer pyenc.allocator().free(name);
            try mod.addObjectRef(name, pyfunction);
        }
    }
});

pub const ComputeFunction = py.class(struct {
    const Self = @This();

    function_name: []const u8,
    gpa: std.mem.Allocator,

    pub fn __del__(self: *Self) void {
        self.gpa.free(self.function_name);
    }

    pub fn __call__(self: *const Self, args: struct { params: py.Args }) !py.PyObject {
        const gpa = pyenc.allocator();

        const ScalarCls = try py.self(pyenc.Scalar);
        defer ScalarCls.decref();
        const ArrayCls = try py.self(pyenc.Array);
        defer ArrayCls.decref();

        const params: []ec.Param = try gpa.alloc(ec.Param, args.params.len);
        defer gpa.free(params);

        for (args.params, params) |pyparam, *param| {
            if (try py.isinstance(pyparam, ScalarCls)) {
                const scalar = try py.as(*const pyenc.Scalar, pyparam);
                param.* = .{ .scalar = scalar.wrapped };
            } else if (try py.isinstance(pyparam, ArrayCls)) {
                const array = try py.as(*const pyenc.Array, pyparam);
                param.* = .{ .array = array.wrapped };
            } else {
                // Otherwise, attempt to convert it to a scalar
                // TODO(ngates): we need to deinit this.
                param.* = .{ .scalar = try pyenc.Scalar.py_to_enc(pyparam, null) };
            }
        }

        var ctx = try pyenc.ctx();
        // TODO(ngates): it may not actually be _this_ kernel that's missing if the compute function makes recursive calls....
        const result = ctx.registry.call(self.function_name, ctx, params, &struct {}) catch |err| switch (err) {
            error.NoSuchFunction => return py.KeyError.raiseFmt("No function named {s} found in context registry", .{self.function_name}),
            error.InvalidArguments => return py.ValueError.raiseFmt("Invalid arguments to function {s}: {any}", .{ self.function_name, params }),
            error.NoKernel => return py.TypeError.raiseFmt(
                "No kernel found for {s} with parameters {any}",
                .{ self.function_name, pretty.List(ec.Param, "()").init(params) },
            ),
            else => return err,
        };

        return switch (result) {
            .scalar => |s| py.object(try pyenc.Scalar.wrapOwned(s)),
            .array => |a| py.object(try pyenc.Array.wrapOwned(a)),
        };
    }

    pub fn register(self: *const Self, args: struct { param_types: py.Args }) !*const FunctionRegistration {
        const ScalarCls = try py.self(pyenc.Scalar);
        defer ScalarCls.decref();
        const ArrayCls = try py.self(pyenc.Array);
        defer ArrayCls.decref();

        var param_specs = std.ArrayList(ec.ParamSpec).init(pyenc.allocator());
        outer: for (args.param_types) |pyparam| {
            if (try issubclass(pyparam, ScalarCls)) {
                try param_specs.append(.scalar);
            } else if (try issubclass(pyparam, ArrayCls)) {
                // TODO(robert): pyenc arrays to have class property that denotes the kind
                inline for (comptime std.enums.values(enc.ArrayKind)) |kind| {
                    const Cls = switch (kind) {
                        .binary => pyenc.BinaryArray,
                        .bool => pyenc.BoolArray,
                        .constant => pyenc.ConstantArray,
                        .chunked => pyenc.ChunkedArray,
                        .dictionary => pyenc.DictionaryArray,
                        .primitive => pyenc.PrimitiveArray,
                        .patched => pyenc.PatchedArray,
                        .roaring_bool => pyenc.RoaringBoolArray,
                        .roaring_uint => pyenc.RoaringUIntArray,
                        .struct_ => pyenc.StructArray,
                    };

                    const ClsPyType = try py.self(Cls);
                    defer ClsPyType.decref();

                    if (try issubclass(pyparam, ClsPyType)) {
                        try param_specs.append(.{ .array = kind });
                        continue :outer;
                    }
                }
                try param_specs.append(.{ .array = null });
            } else {
                return py.TypeError.raise("Invalid compute parameter type, must be pyenc.Scalar or subclass of pyenc.Array");
            }
        }

        return py.init(FunctionRegistration, .{
            .param_specs = param_specs,
            .function_name = try pyenc.allocator().dupe(u8, self.function_name),
            .gpa = pyenc.allocator(),
            .kernel = undefined,
        });
    }

    pub fn unregister(self: *const Self, args: struct { func: py.PyObject }) !void {
        const ctx = try pyenc.ctx();
        const function = ctx.registry.findFunction(self.function_name) orelse return py.KeyError.raiseFmt(
            "No function with name {s}",
            .{self.function_name},
        );
        for (function.kernels.items) |kernel| {
            if (kernel.ptr == @as(*anyopaque, @ptrCast(args.func.py))) {
                if (function.unregisterKernel(kernel)) return;
                return py.ValueError.raise("Kernel is not registered");
            }
        }
    }
});

/// TODO(ngates): move this to Pydust
pub fn issubclass(object: anytype, cls: anytype) !bool {
    const pyobj = py.object(object);
    const pycls = py.object(cls);

    const result = py.ffi.PyObject_IsSubclass(pyobj.py, pycls.py);
    if (result < 0) return py.PyError.PyRaised;
    return result == 1;
}

pub const FunctionRegistration = py.class(struct {
    const Self = @This();

    param_specs: std.ArrayList(ec.ParamSpec),

    function_name: []const u8,
    gpa: std.mem.Allocator,

    kernel: ec.Kernel,

    pub fn __del__(self: *const Self) void {
        self.param_specs.deinit();
        self.gpa.free(self.function_name);
    }

    pub fn __call__(self: *Self, args: struct { func: py.PyObject }) !py.PyObject {
        if (py.ffi.PyCallable_Check(args.func.py) != 1) {
            return py.TypeError.raise("Registered function must be callable");
        }

        const inspect = try py.import("inspect");
        defer inspect.decref();

        const sig = try inspect.call(py.PyObject, "signature", .{args.func}, .{});
        defer sig.decref();

        const ctx = try pyenc.ctx();

        if (ctx.registry.findFunction(self.function_name)) |function| {
            if (function.param_kinds.len != self.param_specs.items.len) {
                // TODO(ngates): check the actual param specs.
                return py.TypeError.raiseFmt("Function {s} expected {} parameters, found {}", .{
                    self.function_name,
                    function.param_kinds.len,
                    self.param_specs.items.len,
                });
            }

            const gpa = pyenc.allocator();
            args.func.incref();
            const kernel: ec.Kernel = .{
                .param_specs = try gpa.dupe(ec.ParamSpec, self.param_specs.items),
                .allocator = gpa,
                .ptr = args.func.py,
                .vtable = &PyKernel.vtable,
            };
            try function.registerOwnedKernel(kernel);
        } else {
            return py.KeyError.raiseFmt("No function named {s} found in context registry", .{self.function_name});
        }

        args.func.incref();
        return args.func;
    }
});

/// An implementation of an pyenc Kernel that delegates to a Python function.
const PyKernel = struct {
    const vtable: ec.Kernel.VTable = .{
        .deinit = &deinit,
        .call = &call,
    };

    fn deinit(ptr: *anyopaque) void {
        const func = py.PyObject{ .py = @ptrCast(@alignCast(ptr)) };
        func.decref();
    }

    fn call(ptr: *anyopaque, ctx: enc.Ctx, params: []const ec.Param, options: *const anyopaque) !ec.Result {
        _ = options;
        _ = ctx;
        const func = py.PyObject{ .py = @ptrCast(@alignCast(ptr)) };

        const args = try py.PyTuple.new(params.len);
        defer args.decref();

        for (params, 0..) |param, i| {
            const arg = switch (param) {
                .array => |a| py.object(try pyenc.Array.wrapOwned(a.retain())),
                .scalar => |s| py.object(try pyenc.Scalar.wrapOwned(try s.clone(pyenc.allocator()))),
            };
            try args.setOwnedItem(i, arg);
        }

        const result = try py.call(py.PyObject, func, args, @as(?py.PyDict, null));
        defer result.decref();

        const ScalarCls = try py.self(pyenc.Scalar);
        defer ScalarCls.decref();
        const ArrayCls = try py.self(pyenc.Array);
        defer ArrayCls.decref();

        if (try py.isinstance(result, ScalarCls)) {
            const scalar = try py.as(*const pyenc.Scalar, result);
            return .{ .scalar = try scalar.wrapped.clone(pyenc.allocator()) };
        } else if (try py.isinstance(result, ArrayCls)) {
            const array = try py.as(*const pyenc.Array, result);
            return .{ .array = array.wrapped.retain() };
        } else {
            const funcStr = try py.str(func);
            defer funcStr.decref();

            const pytypeStr = try py.str(py.type_(result));
            defer pytypeStr.decref();

            return py.TypeError.raiseFmt(
                "Invalid result from compute function {s}. Must be pyenc.Scalar or subclass of pyenc.Array. Found {s}",
                .{ try funcStr.asSlice(), try pytypeStr.asSlice() },
            );
        }
    }
};
