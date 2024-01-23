const std = @import("std");

pub fn build(b: *std.Build) void {
    b.reference_trace = 16;

    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Setup test step
    b.addOptions().addOption([]const u8, "filter-tests", "");
    const filter_test = b.option([]const u8, "filter-tests", "Filter tests by name");
    const test_step = b.step("test", "Run library tests");

    // zimd
    const zimd = b.addModule("zimd", .{
        .source_file = .{ .path = "zig/zimd/zimd.zig" },
    });
    const zimd_test = b.addTest(.{
        .root_source_file = .{ .path = "zig/zimd/zimd.zig" },
        .target = target,
        .optimize = optimize,
        .filter = filter_test,
    });
    const zimd_test_run = b.addRunArtifact(zimd_test);
    test_step.dependOn(&zimd_test_run.step);

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
    const trazy = b.addModule("trazy", .{
        .source_file = .{ .path = "zig/trazy/trazy.zig" },
        .dependencies = &.{
            .{ .name = "tracy_options", .module = tracyOpts.createModule() },
        },
    });

    // C ABI types
    const c_abi_types = b.addModule("abi-types", .{
        .source_file = .{ .path = "zig/c-abi/types.zig" },
    });

    // codecs
    const codecs = b.addModule("codecs", .{
        .source_file = .{ .path = "zig/codecs/codecs.zig" },
        .dependencies = &.{
            .{ .name = "zimd", .module = zimd },
            .{ .name = "trazy", .module = trazy },
            .{ .name = "abi-types", .module = c_abi_types },
        },
    });

    const codecs_test = b.addTest(.{
        .root_source_file = .{ .path = "zig/codecs/codecs.zig" },
        .target = target,
        .optimize = optimize,
        .filter = filter_test,
    });
    codecs_test.addModule("codecs", codecs);
    codecs_test.addModule("trazy", trazy);
    codecs_test.addModule("zimd", zimd);

    const codecs_test_run = b.addRunArtifact(codecs_test);
    test_step.dependOn(&codecs_test_run.step);

    // wrap it all up as a static library to call from Rust
    const lib_step = b.addStaticLibrary(std.Build.StaticLibraryOptions{
        .name = "zenc",
        .root_source_file = .{ .path = "zig/c-abi/wrapper.zig" },
        .link_libc = true,
        .use_llvm = true,
        .target = target,
        .optimize = optimize,
    });
    lib_step.addModule("codecs", codecs);
    lib_step.addModule("zimd", zimd);
    lib_step.addIncludePath(.{ .path = "zig/c-abi" });
    lib_step.c_std = std.Build.CStd.C11;
    lib_step.bundle_compiler_rt = true;
    b.installArtifact(lib_step);

    // test the static library from Zig
    const lib_test = b.addTest(.{
        .root_source_file = .{ .path = "zig/c-abi/test-wrapper.zig" },
        .target = target,
        .optimize = optimize,
        .filter = filter_test,
    });
    lib_test.addIncludePath(.{ .path = "zig/c-abi" });
    lib_test.linkLibrary(lib_step);

    const lib_test_run = b.addRunArtifact(lib_test);
    test_step.dependOn(&lib_test_run.step);

    // Option for emitting test binary based on the given root source.
    // This is used for debugging as in .vscode/launch.json.
    const test_debug_root = b.option([]const u8, "test-debug-root", "The root path of a file emitted as a binary for use with the debugger");
    if (test_debug_root) |root| {
        // FIXME(ngates): which test task?
        codecs_test.root_src = .{ .path = root };
        const test_bin_install = b.addInstallBinFile(codecs_test.getEmittedBin(), "test.bin");
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
