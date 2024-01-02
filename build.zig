const std = @import("std");
const py = @import("./pydust.build.zig");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Setup test step
    b.addOptions().addOption([]const u8, "filter-tests", "");
    const filter_test = b.option([]const u8, "filter-tests", "Filter tests by name");
    const test_step = b.step("test", "Run library tests");

    // arrow
    const arrow = b.addModule("arrow", .{
        .source_file = .{ .path = "zig/arrow/arrow.zig" },
    });
    const arrow_test = b.addTest(.{
        .root_source_file = .{ .path = "zig/arrow/test.zig" },
        .target = target,
        .optimize = optimize,
        .filter = filter_test,
    });
    arrow_test.addIncludePath(.{ .path = "zig/arrow" });
    const arrow_test_run = b.addRunArtifact(arrow_test);
    test_step.dependOn(&arrow_test_run.step);

    // roaring
    const roaring = b.addModule("roaring", .{
        .source_file = .{ .path = "zig/deps/roaring-zig/src/roaring.zig" },
        .dependencies = &.{},
    });

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

    // codecs
    const codecs = b.addModule("codecs", .{
        .source_file = .{ .path = "zig/codecs/codecs.zig" },
        .dependencies = &.{
            .{ .name = "zimd", .module = zimd },
            .{ .name = "roaring", .module = roaring },
        },
    });
    const codecs_test = b.addTest(.{
        .root_source_file = .{ .path = "zig/codecs/codecs.zig" },
        .target = target,
        .optimize = optimize,
        .filter = filter_test,
    });
    dependencyRoaring(codecs_test);
    dependencyStreamvbyte(codecs_test);
    codecs_test.addModule("codecs", codecs);
    codecs_test.addModule("roaring", roaring);
    codecs_test.addModule("zimd", zimd);
    const codecs_test_run = b.addRunArtifact(codecs_test);
    test_step.dependOn(&codecs_test_run.step);

    // pretty
    const pretty = b.addModule("pretty", .{
        .source_file = .{ .path = "zig/pretty/pretty.zig" },
        .dependencies = &.{},
    });

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

    // enc
    const enc = b.addModule("enc", .{
        .source_file = .{ .path = "zig/enc/enc.zig" },
        .dependencies = &.{
            .{ .name = "arrow", .module = arrow },
            .{ .name = "codecs", .module = codecs },
            .{ .name = "pretty", .module = pretty },
            .{ .name = "roaring", .module = roaring },
            .{ .name = "trazy", .module = trazy },
        },
    });
    const enc_test = b.addTest(.{
        .root_source_file = .{ .path = "zig/enc/enc.zig" },
        .target = target,
        .optimize = optimize,
        .filter = filter_test,
    });
    enc_test.addModule("arrow", arrow);
    enc_test.addModule("codecs", codecs);
    enc_test.addModule("pretty", pretty);
    enc_test.addModule("roaring", roaring);
    enc_test.addModule("trazy", trazy);
    dependencyTracy(enc_test);
    dependencyRoaring(enc_test);

    const enc_test_run = b.addRunArtifact(enc_test);
    const enc_test_step = b.step("test.enc", "Run enc tests");
    enc_test_step.dependOn(&enc_test_run.step);
    test_step.dependOn(enc_test_step);

    // pydust
    const pydust = py.addPydust(b, .{
        .test_step = test_step,
    });

    // Python bindings for enc
    const pyenc = pydust.addPythonModule(.{
        .name = "enc._enc",
        .root_source_file = .{ .path = "zig/pyenc/pyenc.zig" },
        .main_pkg_path = .{ .path = "zig/pyenc/" },
        .limited_api = true,
        .target = target,
        .optimize = optimize,
    });
    pyenc.library_step.addModule("codecs", codecs);
    pyenc.test_step.addModule("codecs", codecs);
    pyenc.library_step.addModule("pretty", pretty);
    pyenc.test_step.addModule("pretty", pretty);
    pyenc.library_step.addModule("enc", enc);
    pyenc.test_step.addModule("enc", enc);
    pyenc.library_step.addModule("arrow", arrow);
    pyenc.test_step.addModule("arrow", arrow);
    pyenc.library_step.addModule("roaring", roaring);
    pyenc.test_step.addModule("roaring", roaring);
    pyenc.library_step.addModule("zimd", zimd);
    pyenc.test_step.addModule("zimd", zimd);
    pyenc.library_step.addModule("trazy", trazy);
    pyenc.test_step.addModule("trazy", trazy);
    dependencyTracy(pyenc.library_step);
    dependencyTracy(pyenc.test_step);
    dependencyRoaring(pyenc.library_step);
    dependencyRoaring(pyenc.test_step);

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

/// Configure the streamvbyte dependency
fn dependencyStreamvbyte(lib: *std.Build.Step.Compile) void {
    lib.linkLibC();
    lib.addIncludePath(.{ .path = "zig/deps/streamvbyte/include" });
    lib.addCSourceFiles(.{
        .files = &.{
            "zig/deps/streamvbyte/src/streamvbyte_encode.c",
            "zig/deps/streamvbyte/src/streamvbyte_decode.c",
        },
        .flags = &.{ "-fPIC", "-std=c11", "-O3", "-Wall", "-Wextra", "-pedantic", "-Wshadow" },
    });
}

fn dependencyTracy(lib: *std.Build.Step.Compile) void {
    lib.linkLibCpp();
    lib.addIncludePath(.{ .path = "zig/deps/tracy/public/tracy" });
    lib.addCSourceFile(.{
        .file = .{ .path = "zig/deps/tracy/public/TracyClient.cpp" },
        .flags = &.{ "-fno-sanitize=undefined", "-DTRACY_ENABLE=1" },
    });
}

// Configure the roaring-zig dependency
fn dependencyRoaring(lib: *std.Build.Step.Compile) void {
    lib.linkLibC();
    lib.addIncludePath(.{ .path = "zig/deps/roaring-zig/croaring" });
    lib.addCSourceFile(.{
        .file = .{ .path = "zig/deps/roaring-zig/croaring/roaring.c" },
        .flags = &.{"-fno-sanitize=undefined"},
    });
}
