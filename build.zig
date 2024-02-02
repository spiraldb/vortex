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

    // zimd
    const zimd = b.addModule("zimd", CreateOptions{
        .root_source_file = .{ .path = "zig/zimd/zimd.zig" },
        .target = target,
        .optimize = optimize,
        .omit_frame_pointer = false,
        .error_tracing = true,
        .unwind_tables = true,
        .strip = false,
    });

    // test zimd
    const zimd_test = b.addTest(TestOptions{
        .root_source_file = .{ .path = "zig/zimd/zimd.zig" },
        .target = target,
        .optimize = optimize,
        .filter = filter_test,
        .omit_frame_pointer = false,
        .error_tracing = true,
        .unwind_tables = true,
        .strip = false,
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
        .omit_frame_pointer = false,
        .error_tracing = true,
        .unwind_tables = true,
        .strip = false,
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
        .omit_frame_pointer = false,
        .error_tracing = true,
        .unwind_tables = true,
        .strip = false,
    });
    codecz.addImport("abi", c_abi_types);
    codecz.addImport("zimd", zimd);

    // test codecs
    const codecz_test = b.addTest(TestOptions{
        .root_source_file = .{ .path = "zig/codecz/codecz.zig" },
        .target = target,
        .optimize = optimize,
        .filter = filter_test,
        .link_libc = true,
        .omit_frame_pointer = false,
        .error_tracing = true,
        .unwind_tables = true,
        .strip = false,
    });
    codecz_test.root_module.addImport("codecz", codecz);
    codecz_test.root_module.addImport("abi", c_abi_types);
    codecz_test.root_module.addImport("zimd", zimd);
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
        .omit_frame_pointer = false,
        .error_tracing = true,
        .unwind_tables = true,
        .strip = false,
    });
    lib_step.root_module.addImport("codecz", codecz);
    lib_step.root_module.addImport("abi", c_abi_types);
    lib_step.root_module.addImport("zimd", zimd);
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
        .omit_frame_pointer = false,
        .error_tracing = true,
        .unwind_tables = true,
        .strip = false,
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
