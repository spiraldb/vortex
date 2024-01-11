const std = @import("std");
const enc = @import("../enc.zig");
const ec = @import("./compute.zig");

pub fn UnaryFunction(comptime args: struct {
    name: [:0]const u8,
    doc: [:0]const u8,
    Options: type,
    Impls: []const type,
}) type {
    return Function(.{
        .name = args.name,
        .doc = args.doc,
        .paramKinds = &.{.array},
        .Options = args.Options,
        .Impls = args.Impls,
    });
}

pub fn BinaryFunction(comptime args: struct {
    name: [:0]const u8,
    doc: [:0]const u8,
    Options: type,
    Impls: []const type,
}) type {
    return Function(.{
        .name = args.name,
        .doc = args.doc,
        .paramKinds = &.{ null, null },
        .Options = args.Options,
        .Impls = args.Impls,
    });
}

fn Function(comptime args: struct {
    name: [:0]const u8,
    doc: [:0]const u8,
    paramKinds: []const ?ec.ParamKind,
    Options: type,
    Impls: []const type,
}) type {
    return struct {
        const static_kernels = extractKernels(args.paramKinds, args.Impls);

        pub fn function(gpa: std.mem.Allocator) !ec.Function {
            var func = try ec.Function.init(
                gpa,
                args.name,
                args.doc,
                args.paramKinds,
            );
            for (static_kernels) |kernel| {
                try func.registerOwnedKernel(kernel);
            }
            return func;
        }
    };
}

fn extractKernels(paramKinds: []const ?ec.ParamKind, comptime Impls: []const type) [countKernels(Impls)]ec.Kernel {
    var kernels: [countKernels(Impls)]ec.Kernel = undefined;
    var kernelIdx = 0;

    for (Impls) |Impl| {
        for (@typeInfo(Impl).Struct.decls) |d| {
            const decl = @field(Impl, d.name);
            const kernel = ec.Kernel.initWithFunction(decl);
            if (!kernel.matchesParamKinds(paramKinds)) {
                @compileError(std.fmt.comptimePrint(
                    "Kernel {} does not match expected parameter kinds {any} for function",
                    .{ @TypeOf(decl), paramKinds },
                ));
            }
            kernels[kernelIdx] = kernel;
            kernelIdx += 1;
        }
    }

    return sortedKernels(countKernels(Impls), paramKinds.len, kernels);
}

/// Sort kernels such that those which are more specific appear first.
fn sortedKernels(comptime nkernels: usize, arity: u8, kernels: [nkernels]ec.Kernel) [nkernels]ec.Kernel {
    var sorted: [nkernels]ec.Kernel = undefined;
    var idx = 0;

    for (0..arity + 1) |a| {
        for (kernels) |kernel| {
            std.debug.assert(kernel.param_specs.len == arity);

            var unspecified_arrays = 0;
            for (kernel.param_specs) |param_spec| {
                if (param_spec == .array and param_spec.array == null) {
                    unspecified_arrays += 1;
                }
            }

            if (unspecified_arrays == a) {
                sorted[idx] = kernel;
                idx += 1;
            }
        }
    }
    std.debug.assert(idx == nkernels);

    return sorted;
}

fn countKernels(comptime Impls: []const type) usize {
    var count: usize = 0;
    for (Impls) |Impl| {
        const info = @typeInfo(Impl).Struct;
        for (info.decls) |d| {
            const decl = @field(Impl, d.name);
            if (@typeInfo(@TypeOf(decl)) == .Fn) {
                count += 1;
            }
        }
    }
    return count;
}
