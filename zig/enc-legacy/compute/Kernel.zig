//! A compute kernel represents a single implementation of a compute function.
//!
//! It is specific to a given set of parameter specs which provide a fast way of
//! checking compatability for a function invocation.
const std = @import("std");
const enc = @import("../enc.zig");
const ec = @import("./compute.zig");
const pretty = @import("pretty");

const Self = @This();

param_specs: []const ec.ParamSpec,
allocator: ?std.mem.Allocator,
ptr: *anyopaque,
vtable: *const VTable,

pub const KernelFn = fn (ptr: *anyopaque, enc.Ctx, []const ec.Param, options: *const anyopaque) anyerror!ec.Result;

pub const VTable = struct {
    call: *const KernelFn,
    deinit: ?*const fn (ptr: *anyopaque) void,
};

pub fn deinit(self: *Self) void {
    if (self.allocator) |ally| ally.free(self.param_specs);
    if (self.vtable.deinit) |func| func(self.ptr);
}

pub fn format(self: Self, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
    _ = fmt;
    _ = options;
    try writer.print("Kernel{}", .{pretty.List(ec.ParamSpec, "()").init(self.param_specs)});
}

/// Invoke the kernel with the given parameters.
/// The caller is responsible for ensuring that the parameters are compatible with the kernel.
pub inline fn call(self: *const Self, ctx: enc.Ctx, params: []const ec.Param, options: *const anyopaque) anyerror!ec.Result {
    return self.vtable.call(self.ptr, ctx, params, options);
}

/// Test whether the kernel matches the given parameters.
pub fn matchesParams(self: *const Self, params: []const ec.Param) bool {
    if (self.param_specs.len != params.len) {
        return false;
    }
    for (self.param_specs, params) |spec, param| {
        if (!spec.matchesParam(param)) {
            return false;
        }
    }
    return true;
}

/// Test whether the kernel matches the given parameter kinds.
pub fn matchesParamKinds(self: *const Self, params: []const ?ec.ParamKind) bool {
    if (self.param_specs.len != params.len) return false;
    for (self.param_specs, params) |spec, param| {
        if (param) |kind| {
            if (spec != kind) return false;
        }
    }
    return true;
}

/// Return the number of parameters of this kernel that are pinned to a specific array type (including scalars).
pub fn pinnedParameterCount(self: *const Self) usize {
    var pinned: usize = 0;
    for (self.param_specs) |spec| {
        if (spec == .array and spec.array == null) continue;
        pinned += 1;
    }
    return pinned;
}

pub fn initWithFunction(comptime func: anytype) Self {
    const Fn = @typeInfo(@TypeOf(func)).Fn;
    if (Fn.params.len <= 2) {
        @compileError("Kernel function must have at least Ctx, and options parameters");
    }
    const arity = Fn.params.len - 2;

    checkParamType(func, 0, enc.Ctx);

    const StaticKernel = struct {
        const param_specs: [arity]ec.ParamSpec = blk: {
            var param_specs_: [arity]ec.ParamSpec = undefined;
            for (Fn.params[1 .. 1 + arity], 0..) |fn_param, i| {
                const ParamType = fn_param.type.?;
                if (ParamType == enc.Scalar) {
                    param_specs_[i] = .scalar;
                } else {
                    param_specs_[i] = .{ .array = arrayKindFromType(ParamType) };
                }
            }
            break :blk param_specs_;
        };

        const vtable: VTable = .{
            .call = &@This().call,
            .deinit = null,
        };

        fn call(ptr: *const anyopaque, ctx: enc.Ctx, params: []const ec.Param, options_ptr: *const anyopaque) anyerror!ec.Result {
            _ = ptr;
            if (params.len != arity) return error.InvalidArguments;

            var args: std.meta.ArgsTuple(@TypeOf(func)) = undefined;
            args[0] = ctx;
            inline for (param_specs, 0..) |param_spec, i| {
                // Downcast the parameter to the array encoding if one is specified.
                if (param_spec == .array) {
                    if (param_spec.array) |array_kind| {
                        args[1 + i] = enc.ArrayEncoding(array_kind).from(params[i].array);
                    } else {
                        args[1 + i] = params[i].array;
                    }
                } else {
                    args[i + 1] = params[i].scalar;
                }
            }
            args[args.len - 1] = @ptrCast(@alignCast(options_ptr));

            const result = try @call(.auto, func, args);
            const R = @TypeOf(result);
            if (R == ec.Result) return result;
            if (R == *enc.Array) return .{ .array = result };
            if (R == enc.Scalar) return .{ .scalar = result };
            if (@typeInfo(R) == .Pointer) {
                if (@hasField(@typeInfo(R).Pointer.child, "array")) {
                    return .{ .array = &result.array };
                }
            } else {
                if (@hasField(R, "array")) {
                    return .{ .array = &result.array };
                }
            }

            @compileError("Unsupported kernel return type " ++ @typeName(R));
        }
    };

    return ec.Kernel{
        .param_specs = &StaticKernel.param_specs,
        .allocator = null, // To avoid freeing the static param_specs,
        .ptr = undefined,
        .vtable = &StaticKernel.vtable,
    };
}

fn checkParamType(comptime func: anytype, comptime paramIdx: usize, comptime expected: type) void {
    const Fn = @typeInfo(@TypeOf(func)).Fn;

    if (paramIdx >= Fn.params.len) {
        @compileError(std.fmt.comptimePrint("Expected at least {} parameters, found {} on {}", .{ paramIdx + 1, Fn.params.len, func }));
    }

    const paramType = Fn.params[paramIdx].type.?;
    if (paramType != expected) {
        @compileError(std.fmt.comptimePrint("Expected {s} for parameter {} of {}, found {s}", .{
            @typeName(expected),
            paramIdx,
            @TypeOf(func),
            @typeName(paramType),
        }));
    }
}

fn arrayKindFromType(comptime ArrayType: type) ?enc.ArrayKind {
    if (ArrayType == *const enc.Array) {
        // No specialization, just the parent array class;
        return null;
    }

    const ArrayEncoding = @typeInfo(ArrayType).Pointer.child;
    return enc.ArrayEncodingKind(ArrayEncoding);
}
