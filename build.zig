const std = @import("std");
const CreateOptions = std.Build.Module.CreateOptions;
const TestOptions = std.Build.TestOptions;
const StaticLibraryOptions = std.Build.StaticLibraryOptions;

pub fn build(b: *std.Build) void {
    b.reference_trace = 16;

    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Setup test step
    b.addOptions().addOption([]const u8, "filter-tests", "");
    const filter_test = b.option([]const u8, "filter-tests", "Filter tests by name");
    const test_step = b.step("test", "Run library tests");

    // tracy options
    const tracy = b.option(bool, "tracy", "Enable Tracy integration") orelse true;
    const tracy_callstack = b.option(bool, "tracy-callstack", "Include callstack information with Tracy data. Does nothing if -Dtracy is not provided") orelse tracy;
    const tracy_allocation = b.option(bool, "tracy-allocation", "Include allocation information with Tracy data. Does nothing if -Dtracy is not provided") orelse tracy;
    const callstack_depth = b.option(u16, "tracy-callstack-depth", "Depth of tracy callstack information. 10 by default") orelse 10;
    const tracyOpts = b.addOptions();
    tracyOpts.addOption(bool, "enable_tracy", tracy);
    tracyOpts.addOption(bool, "enable_tracy_callstack", tracy_callstack);
    tracyOpts.addOption(bool, "enable_tracy_allocation", tracy_allocation);
    tracyOpts.addOption(u16, "callstack_depth", callstack_depth);

    // trazy
    const trazy = b.addModule("trazy", CreateOptions{
        .root_source_file = .{ .path = "zig/trazy/trazy.zig" },
        .target = target,
        .optimize = optimize,
    });
    trazy.addImport("tracy_options", tracyOpts.createModule());

    // zimd
    const zimd = b.addModule("zimd", CreateOptions{
        .root_source_file = .{ .path = "zig/zimd/zimd.zig" },
        .target = target,
        .optimize = optimize,
    });

    // test zimd
    const zimd_test = b.addTest(TestOptions{
        .root_source_file = .{ .path = "zig/zimd/zimd.zig" },
        .target = target,
        .optimize = optimize,
        .filter = filter_test,
    });
    zimd_test.root_module.addImport("zimd", zimd);
    const zimd_test_run = b.addRunArtifact(zimd_test);
    test_step.dependOn(&zimd_test_run.step);

    // C ABI types
    const c_abi_options = CreateOptions{
        .root_source_file = .{ .path = "zig/c-abi/types.zig" },
        .target = target,
        .optimize = optimize,
        .c_std = std.Build.CStd.C11,
        .link_libc = true,
    };
    const c_abi_types = b.addModule("abi", c_abi_options);
    c_abi_types.addIncludePath(.{ .path = "zig/c-abi/include" });

    // codecs
    const codecz = b.addModule("codecz", CreateOptions{
        .root_source_file = .{ .path = "zig/codecz/codecz.zig" },
        .target = target,
        .optimize = optimize,
        .c_std = std.Build.CStd.C11,
        .link_libc = true,
    });
    codecz.addImport("abi", c_abi_types);
    // codecz.addImport("trazy", trazy);
    codecz.addImport("zimd", zimd);

    // test codecs
    const codecz_test = b.addTest(TestOptions{
        .root_source_file = .{ .path = "zig/codecz/codecz.zig" },
        .target = target,
        .optimize = optimize,
        .filter = filter_test,
        .link_libc = true,
    });
    codecz_test.root_module.addImport("codecz", codecz);
    codecz_test.root_module.addImport("abi", c_abi_types);
    codecz_test.root_module.addImport("zimd", zimd);
    // codecz_test.root_module.addImport("trazy", trazy);
    // dependencyTracy(codecz_test);
    codecz_test.addIncludePath(.{ .path = "zig/c-abi/include" });
    const codecz_test_run = b.addRunArtifact(codecz_test);
    test_step.dependOn(&codecz_test_run.step);

    // wrap it all up as a static library to call from Rust
    const lib_step = b.addStaticLibrary(StaticLibraryOptions{
        .name = "codecz",
        .root_source_file = .{ .path = "zig/c-abi/wrapper.zig" },
        .link_libc = true,
        .use_llvm = true,
        .target = target,
        .optimize = optimize,
        .single_threaded = false,
        .unwind_tables = true,
    });
    lib_step.root_module.addImport("codecz", codecz);
    lib_step.root_module.addImport("abi", c_abi_types);
    lib_step.root_module.addImport("zimd", zimd);
    //lib_step.root_module.addImport("trazy", trazy);
    //dependencyTracy(lib_step);
    lib_step.addIncludePath(.{ .path = "zig/c-abi/include" });
    lib_step.root_module.c_std = std.Build.CStd.C11;
    lib_step.bundle_compiler_rt = true;
    lib_step.formatted_panics = true;
    b.installArtifact(lib_step);

    // also test invoking the static library from Zig
    const lib_test = b.addTest(TestOptions{
        .root_source_file = .{ .path = "zig/c-abi/test_wrapper.zig" },
        .target = target,
        .optimize = optimize,
        .filter = filter_test,
    });
    lib_test.addIncludePath(.{ .path = "zig/c-abi/include" });
    lib_test.linkLibrary(lib_step);

    const lib_test_run = b.addRunArtifact(lib_test);
    test_step.dependOn(&lib_test_run.step);

    // Option for emitting test binary based on the given root source.
    // This is used for debugging as in .vscode/launch.json.
    const test_debug_root = b.option([]const u8, "test-debug-root", "The root path of a file emitted as a binary for use with the debugger");
    if (test_debug_root) |root| {
        // FIXME(ngates): which test task?
        codecz_test.root_module.root_source_file = .{ .path = root };
        const test_bin_install = b.addInstallBinFile(codecz_test.getEmittedBin(), "test.bin");
        b.getInstallStep().dependOn(&test_bin_install.step);
    }
}

fn dependencyTracy(lib: *std.Build.Step.Compile) void {
    lib.linkLibCpp();
    lib.addIncludePath(.{ .path = "zig/deps/tracy/public/tracy" });
    lib.addCSourceFile(.{
        .file = .{ .path = "zig/deps/tracy/public/TracyClient.cpp" },
        .flags = &.{ "-fno-sanitize=undefined", "-DTRACY_ENABLE=1" },
    });
}
