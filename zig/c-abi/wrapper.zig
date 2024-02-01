const std = @import("std");
const builtin = @import("builtin");
const codecz = @import("codecz");
const encodings = codecz.encodings;
const simd_math = codecz.simd_math;
const CodecError = codecz.CodecError;
const c = @cImport({
    @cInclude("wrapper.h");
});
const abi = @import("abi");

// aliases
const Alignment: u29 = c.SPIRAL_ALIGNMENT;
const AlpExponents = abi.AlpExponents;
const ByteBuffer = abi.ByteBuffer;
const ResultStatus = abi.ResultStatus;
const WrittenBuffer = abi.WrittenBuffer;
const OneBufferResult = abi.OneBufferResult;
const TwoBufferResult = abi.TwoBufferResult;
const AlpExponentsResult = abi.AlpExponentsResult;

const UnsignedIntegerTypes = [_]type{ u8, u16, u32, u64 };
const SignedIntegerTypes = [_]type{ i8, i16, i32, i64 };
const IntegerTypes = UnsignedIntegerTypes ++ SignedIntegerTypes;
const SizeTypes = [_]type{u32};
const FloatTypes = [_]type{ f32, f64 };
const NumberTypes = IntegerTypes ++ FloatTypes;

comptime {
    if (!builtin.link_libc) {
        @compileError("Must be built with libc in order for codecz-sys (rust) to call codecz (zig) via the C ABI");
    }
    if (@bitSizeOf(usize) != @bitSizeOf(c.expected_zig_usize_t)) {
        @compileError(std.fmt.comptimePrint(
            "Mismatch between zig's usize ({} bits) and the C ABI's expected size type ({} bits)",
            .{ @bitSizeOf(usize), @bitSizeOf(c.expected_zig_usize_t) },
        ));
    }
}

//
// SIMD Math
//
comptime {
    abi.checkStructABI(simd_math.RunLengthStats, c.RunLengthStats_t);
    for (NumberTypes) |T| {
        const wrapper = MathWrapper(T);
        @export(wrapper.max, std.builtin.ExportOptions{ .name = "codecz_math_max_" ++ @typeName(T), .linkage = .Strong });
        @export(wrapper.min, std.builtin.ExportOptions{ .name = "codecz_math_min_" ++ @typeName(T), .linkage = .Strong });
        @export(wrapper.isSorted, std.builtin.ExportOptions{ .name = "codecz_math_isSorted_" ++ @typeName(T), .linkage = .Strong });
        @export(wrapper.isConstant, std.builtin.ExportOptions{ .name = "codecz_math_isConstant_" ++ @typeName(T), .linkage = .Strong });
        @export(wrapper.runLengthStats, std.builtin.ExportOptions{ .name = "codecz_math_runLengthStats_" ++ @typeName(T), .linkage = .Strong });
        wrapper.checkFnSignatures();
    }
}

fn MathWrapper(comptime T: type) type {
    return struct {
        const Self = @This();

        pub fn max(elems: [*c]const T, len: u64) callconv(.C) T {
            return simd_math.max(T, elems[0..len]);
        }

        pub fn min(elems: [*c]const T, len: u64) callconv(.C) T {
            return simd_math.min(T, elems[0..len]);
        }

        pub fn isSorted(elems: [*c]const T, len: u64) callconv(.C) bool {
            return simd_math.isSorted(T, elems[0..len]);
        }

        pub fn isConstant(elems: [*c]const T, len: u64) callconv(.C) bool {
            return simd_math.isConstant(T, elems[0..len]);
        }

        pub fn runLengthStats(elems: [*c]const T, len: u64, out: [*c]c.RunLengthStats_t) callconv(.C) void {
            out.* = simd_math.runLengthStats(T, elems[0..len]);
        }

        pub fn checkFnSignatures() void {
            if (T == u8) {
                abi.checkFnSignature(Self.max, c.codecz_math_max_u8);
                abi.checkFnSignature(Self.min, c.codecz_math_min_u8);
                abi.checkFnSignature(Self.isSorted, c.codecz_math_isSorted_u8);
                abi.checkFnSignature(Self.isConstant, c.codecz_math_isConstant_u8);
                abi.checkFnSignature(Self.runLengthStats, c.codecz_math_runLengthStats_u8);
            } else if (T == u16) {
                abi.checkFnSignature(Self.max, c.codecz_math_max_u16);
                abi.checkFnSignature(Self.min, c.codecz_math_min_u16);
                abi.checkFnSignature(Self.isSorted, c.codecz_math_isSorted_u16);
                abi.checkFnSignature(Self.isConstant, c.codecz_math_isConstant_u16);
                abi.checkFnSignature(Self.runLengthStats, c.codecz_math_runLengthStats_u16);
            } else if (T == u32) {
                abi.checkFnSignature(Self.max, c.codecz_math_max_u32);
                abi.checkFnSignature(Self.min, c.codecz_math_min_u32);
                abi.checkFnSignature(Self.isSorted, c.codecz_math_isSorted_u32);
                abi.checkFnSignature(Self.isConstant, c.codecz_math_isConstant_u32);
                abi.checkFnSignature(Self.runLengthStats, c.codecz_math_runLengthStats_u32);
            } else if (T == u64) {
                abi.checkFnSignature(Self.max, c.codecz_math_max_u64);
                abi.checkFnSignature(Self.min, c.codecz_math_min_u64);
                abi.checkFnSignature(Self.isSorted, c.codecz_math_isSorted_u64);
                abi.checkFnSignature(Self.isConstant, c.codecz_math_isConstant_u64);
                abi.checkFnSignature(Self.runLengthStats, c.codecz_math_runLengthStats_u64);
            } else if (T == i8) {
                abi.checkFnSignature(Self.max, c.codecz_math_max_i8);
                abi.checkFnSignature(Self.min, c.codecz_math_min_i8);
                abi.checkFnSignature(Self.isSorted, c.codecz_math_isSorted_i8);
                abi.checkFnSignature(Self.isConstant, c.codecz_math_isConstant_i8);
                abi.checkFnSignature(Self.runLengthStats, c.codecz_math_runLengthStats_i8);
            } else if (T == i16) {
                abi.checkFnSignature(Self.max, c.codecz_math_max_i16);
                abi.checkFnSignature(Self.min, c.codecz_math_min_i16);
                abi.checkFnSignature(Self.isSorted, c.codecz_math_isSorted_i16);
                abi.checkFnSignature(Self.isConstant, c.codecz_math_isConstant_i16);
                abi.checkFnSignature(Self.runLengthStats, c.codecz_math_runLengthStats_i16);
            } else if (T == i32) {
                abi.checkFnSignature(Self.max, c.codecz_math_max_i32);
                abi.checkFnSignature(Self.min, c.codecz_math_min_i32);
                abi.checkFnSignature(Self.isSorted, c.codecz_math_isSorted_i32);
                abi.checkFnSignature(Self.isConstant, c.codecz_math_isConstant_i32);
                abi.checkFnSignature(Self.runLengthStats, c.codecz_math_runLengthStats_i32);
            } else if (T == i64) {
                abi.checkFnSignature(Self.max, c.codecz_math_max_i64);
                abi.checkFnSignature(Self.min, c.codecz_math_min_i64);
                abi.checkFnSignature(Self.isSorted, c.codecz_math_isSorted_i64);
                abi.checkFnSignature(Self.isConstant, c.codecz_math_isConstant_i64);
                abi.checkFnSignature(Self.runLengthStats, c.codecz_math_runLengthStats_i64);
            } else if (T == f32) {
                abi.checkFnSignature(Self.max, c.codecz_math_max_f32);
                abi.checkFnSignature(Self.min, c.codecz_math_min_f32);
                abi.checkFnSignature(Self.isSorted, c.codecz_math_isSorted_f32);
                abi.checkFnSignature(Self.isConstant, c.codecz_math_isConstant_f32);
                abi.checkFnSignature(Self.runLengthStats, c.codecz_math_runLengthStats_f32);
            } else if (T == f64) {
                abi.checkFnSignature(Self.max, c.codecz_math_max_f64);
                abi.checkFnSignature(Self.min, c.codecz_math_min_f64);
                abi.checkFnSignature(Self.isSorted, c.codecz_math_isSorted_f64);
                abi.checkFnSignature(Self.isConstant, c.codecz_math_isConstant_f64);
                abi.checkFnSignature(Self.runLengthStats, c.codecz_math_runLengthStats_f64);
            } else {
                @compileError(std.fmt.comptimePrint("SIMD Math: Unsupported type {s}", .{@typeName(T)}));
            }
        }
    };
}

//
// Run End Encoding
//
comptime {
    const REE_TYPES = NumberTypes ++ .{f16};
    for (REE_TYPES) |V| {
        for (SizeTypes) |E| {
            const wrapper = RunEndWrapper(V, E);
            @export(wrapper.encode, std.builtin.ExportOptions{
                .name = "codecz_ree_encode_" ++ @typeName(V) ++ "_" ++ @typeName(E),
                .linkage = .Strong,
            });
            @export(wrapper.decode, std.builtin.ExportOptions{
                .name = "codecz_ree_decode_" ++ @typeName(V) ++ "_" ++ @typeName(E),
                .linkage = .Strong,
            });
            wrapper.checkFnSignatures();
        }
    }
}

fn RunEndWrapper(comptime V: type, comptime E: type) type {
    return struct {
        const Self = @This();
        // we want bitwise equality rather than float equality (where e.g., NaN equality is not reflexive) for FP types
        const V2 = switch (@typeInfo(V)) {
            .Int => V,
            .Float => std.meta.Int(.unsigned, @bitSizeOf(V)),
            else => @compileError("REE: Unsupported type " ++ @typeName(V)),
        };
        comptime {
            if (@bitSizeOf(V) != @bitSizeOf(V2)) {
                @compileError(std.fmt.comptimePrint(
                    "REE: programmer error trying to encode {s} with {s} (bit sizes don't match)",
                    .{ @typeName(V), @typeName(E) },
                ));
            }
        }
        const codec = encodings.RunEnd(V2, E, Alignment);

        pub fn encode(elems: [*c]V, len: usize, out: [*c]c.TwoBufferResult_t) callconv(.C) void {
            // this verifies alignment and returns an error result if the buffer is not properly aligned
            const zigOut = TwoBufferResult.from(out.*) catch |err| return TwoBufferResult.errOut(err, V, E, out);
            const valuesBuf = zigOut.first.buffer.check() catch |err| return TwoBufferResult.errOut(err, V, E, out);
            const runEndsBuf = zigOut.second.buffer.check() catch |err| return TwoBufferResult.errOut(err, V, E, out);

            const values: []align(Alignment) V2 = @alignCast(std.mem.bytesAsSlice(V2, valuesBuf.bytes()));
            const runEnds: []align(Alignment) E = @alignCast(std.mem.bytesAsSlice(E, runEndsBuf.bytes()));
            const elemsSlice = std.mem.bytesAsSlice(V2, std.mem.sliceAsBytes(elems[0..len]));

            if (codec.encode(elemsSlice, values, runEnds)) |numRuns| {
                const first = WrittenBuffer.initFromSlice(V2, valuesBuf, values[0..numRuns]);
                const second = WrittenBuffer.initFromSlice(E, runEndsBuf, runEnds[0..numRuns]);
                const result = TwoBufferResult.ok(first, second);
                out.* = result.into();
            } else |err| {
                TwoBufferResult.errOut(err, V, E, out);
            }
        }

        pub fn decode(values_: [*c]V, runEnds_: [*c]E, numRuns: usize, out: [*c]c.OneBufferResult_t) callconv(.C) void {
            const values: []const V2 = std.mem.bytesAsSlice(V2, std.mem.sliceAsBytes(values_[0..numRuns]));
            const runEnds: []const E = runEnds_[0..numRuns];

            const zigOut = OneBufferResult.from(out.*) catch |err| return OneBufferResult.errOut(err, V, out);
            const outBuf = zigOut.buf.buffer.check() catch |err| return OneBufferResult.errOut(err, V, out);
            const decoded: []align(Alignment) V2 = @alignCast(std.mem.bytesAsSlice(V2, outBuf.bytes()));

            if (codec.decode(values, runEnds, decoded)) {
                const result = OneBufferResult.ok(WrittenBuffer.initFromSlice(V2, outBuf, decoded));
                out.* = result.into();
            } else |err| {
                OneBufferResult.errOut(err, V2, out);
            }
        }

        pub fn checkFnSignatures() void {
            if (V == u8 and E == u32) {
                abi.checkFnSignature(Self.encode, c.codecz_ree_encode_u8_u32);
                abi.checkFnSignature(Self.decode, c.codecz_ree_decode_u8_u32);
            } else if (V == u16 and E == u32) {
                abi.checkFnSignature(Self.encode, c.codecz_ree_encode_u16_u32);
                abi.checkFnSignature(Self.decode, c.codecz_ree_decode_u16_u32);
            } else if (V == u32 and E == u32) {
                abi.checkFnSignature(Self.encode, c.codecz_ree_encode_u32_u32);
                abi.checkFnSignature(Self.decode, c.codecz_ree_decode_u32_u32);
            } else if (V == u64 and E == u32) {
                abi.checkFnSignature(Self.encode, c.codecz_ree_encode_u64_u32);
                abi.checkFnSignature(Self.decode, c.codecz_ree_decode_u64_u32);
            } else if (V == i8 and E == u32) {
                abi.checkFnSignature(Self.encode, c.codecz_ree_encode_i8_u32);
                abi.checkFnSignature(Self.decode, c.codecz_ree_decode_i8_u32);
            } else if (V == i16 and E == u32) {
                abi.checkFnSignature(Self.encode, c.codecz_ree_encode_i16_u32);
                abi.checkFnSignature(Self.decode, c.codecz_ree_decode_i16_u32);
            } else if (V == i32 and E == u32) {
                abi.checkFnSignature(Self.encode, c.codecz_ree_encode_i32_u32);
                abi.checkFnSignature(Self.decode, c.codecz_ree_decode_i32_u32);
            } else if (V == i64 and E == u32) {
                abi.checkFnSignature(Self.encode, c.codecz_ree_encode_i64_u32);
                abi.checkFnSignature(Self.decode, c.codecz_ree_decode_i64_u32);
            } else if (V == f16 and E == u32) {
                abi.checkFnSignature(Self.encode, c.codecz_ree_encode_f16_u32);
                abi.checkFnSignature(Self.decode, c.codecz_ree_decode_f16_u32);
            } else if (V == f32 and E == u32) {
                abi.checkFnSignature(Self.encode, c.codecz_ree_encode_f32_u32);
                abi.checkFnSignature(Self.decode, c.codecz_ree_decode_f32_u32);
            } else if (V == f64 and E == u32) {
                abi.checkFnSignature(Self.encode, c.codecz_ree_encode_f64_u32);
                abi.checkFnSignature(Self.decode, c.codecz_ree_decode_f64_u32);
            } else {
                @compileError(std.fmt.comptimePrint("REE: Unsupported type pair {s} and {s}", .{ @typeName(V), @typeName(E) }));
            }
        }
    };
}

//
// Adaptive Lossless Floating Point Encoding
//
comptime {
    abi.checkStructABI(AlpExponents, c.AlpExponents_t);
    for (FloatTypes) |F| {
        const wrapper = ALPWrapper(F);
        @export(wrapper.sampleFindExponents, std.builtin.ExportOptions{
            .name = "codecz_alp_sampleFindExponents_" ++ @typeName(F),
            .linkage = .Strong,
        });
        @export(wrapper.encode, std.builtin.ExportOptions{
            .name = "codecz_alp_encode_" ++ @typeName(F),
            .linkage = .Strong,
        });
        @export(wrapper.decode, std.builtin.ExportOptions{
            .name = "codecz_alp_decode_" ++ @typeName(F),
            .linkage = .Strong,
        });
        wrapper.checkFnSignatures();
    }
}

fn ALPWrapper(comptime F: type) type {
    return struct {
        const Self = @This();
        const codec = encodings.AdaptiveLosslessFloatingPoint(F);

        pub fn sampleFindExponents(elems: [*c]F, len: usize, out: [*c]c.AlpExponentsResult_t) callconv(.C) void {
            if (codec.sampleFindExponents(elems[0..len])) |exp| {
                const result = AlpExponentsResult.ok(exp);
                out.* = result.into();
            } else |err| {
                const result = AlpExponentsResult.err(err);
                out.* = result.into();
            }
        }

        pub fn encode(
            elems: [*c]F,
            elems_len: usize,
            exp_: [*c]c.AlpExponents_t,
            out: [*c]c.TwoBufferResult_t,
        ) callconv(.C) void {
            const exp: AlpExponents = AlpExponents.from(exp_.*);
            const zigOut = TwoBufferResult.from(out.*) catch |err| return TwoBufferResult.errOut(err, codec.EncInt, u1, out);
            const encBuf = zigOut.first.buffer.check() catch |err| return TwoBufferResult.errOut(err, codec.EncInt, u1, out);
            const excPosBuf = zigOut.second.buffer.check() catch |err| return TwoBufferResult.errOut(err, codec.EncInt, u1, out);

            const values: []align(Alignment) codec.EncInt = @alignCast(std.mem.bytesAsSlice(codec.EncInt, encBuf.bytes()));
            var excPositions = excPosBuf.bits();

            if (codec.encodeRaw(elems[0..elems_len], exp, values, &excPositions)) |numExceptions| {
                const first = WrittenBuffer.initFromSlice(codec.EncInt, encBuf, values[0..elems_len]);
                const second = WrittenBuffer.initFromBitSlice(excPosBuf, excPositions, numExceptions);
                const result = TwoBufferResult.ok(first, second);
                out.* = result.into();
            } else |err| {
                TwoBufferResult.errOut(err, codec.EncInt, u1, out);
            }
        }

        pub fn decode(input: [*c]codec.EncInt, len: usize, exp_: [*c]c.AlpExponents_t, out: [*c]c.OneBufferResult_t) callconv(.C) void {
            const exp: AlpExponents = AlpExponents.from(exp_.*);
            const zigOut = OneBufferResult.from(out.*) catch |err| return OneBufferResult.errOut(err, F, out);
            const outSlice: []align(Alignment) F = @alignCast(std.mem.bytesAsSlice(F, zigOut.buf.buffer.bytes()));
            if (codec.decodeRaw(input[0..len], exp, outSlice)) {
                const buf = WrittenBuffer.initFromSlice(F, zigOut.buf.buffer, outSlice[0..len]);
                const result = OneBufferResult.ok(buf);
                out.* = result.into();
            } else |err| {
                OneBufferResult.errOut(err, F, out);
            }
        }

        pub fn checkFnSignatures() void {
            if (F == f32) {
                abi.checkFnSignature(Self.sampleFindExponents, c.codecz_alp_sampleFindExponents_f32);
                abi.checkFnSignature(Self.encode, c.codecz_alp_encode_f32);
                abi.checkFnSignature(Self.decode, c.codecz_alp_decode_f32);
            } else if (F == f64) {
                abi.checkFnSignature(Self.sampleFindExponents, c.codecz_alp_sampleFindExponents_f64);
                abi.checkFnSignature(Self.encode, c.codecz_alp_encode_f64);
                abi.checkFnSignature(Self.decode, c.codecz_alp_decode_f64);
            } else {
                @compileError(std.fmt.comptimePrint("ALP: unsupported type {s}", .{@typeName(F)}));
            }
        }
    };
}

//
// ZigZag Encoding
//
comptime {
    for (SignedIntegerTypes) |V| {
        const wrapper = ZigZagWrapper(V);
        @export(wrapper.encode, std.builtin.ExportOptions{
            .name = "codecz_zz_encode_" ++ @typeName(V),
            .linkage = .Strong,
        });
        @export(wrapper.decode, std.builtin.ExportOptions{
            .name = "codecz_zz_decode_" ++ @typeName(V),
            .linkage = .Strong,
        });
        wrapper.checkFnSignatures();
    }
}

fn ZigZagWrapper(comptime V: type) type {
    return struct {
        const Self = @This();
        const codec = encodings.ZigZag(V);
        const U: type = codec.Unsigned;

        pub fn encode(elems: [*c]V, len: usize, out: [*c]c.OneBufferResult_t) callconv(.C) void {
            const zigOut = OneBufferResult.from(out.*) catch |err| return OneBufferResult.errOut(err, V, out);
            const encodedBuf = zigOut.buf.buffer.check() catch |err| return OneBufferResult.errOut(err, V, out);
            const encoded: []align(Alignment) U = @alignCast(std.mem.bytesAsSlice(U, encodedBuf.bytes()));
            if (codec.encode(elems[0..len], encoded)) {
                const result = OneBufferResult.ok(WrittenBuffer.initFromSlice(U, encodedBuf, encoded[0..len]));
                out.* = result.into();
            } else |err| {
                return OneBufferResult.errOut(err, U, out);
            }
        }

        pub fn decode(encoded: [*c]U, len: usize, out: [*c]c.OneBufferResult_t) callconv(.C) void {
            const zigOut = OneBufferResult.from(out.*) catch |err| return OneBufferResult.errOut(err, V, out);
            const outBuf = zigOut.buf.buffer.check() catch |err| return OneBufferResult.errOut(err, V, out);
            const decoded: []align(Alignment) V = @alignCast(std.mem.bytesAsSlice(V, outBuf.bytes()));
            if (codec.decode(encoded[0..len], decoded)) {
                const result = OneBufferResult.ok(WrittenBuffer.initFromSlice(V, outBuf, decoded[0..len]));
                out.* = result.into();
            } else |err| {
                OneBufferResult.errOut(err, V, out);
            }
        }

        pub fn checkFnSignatures() void {
            if (V == i8) {
                abi.checkFnSignature(Self.encode, c.codecz_zz_encode_i8);
                abi.checkFnSignature(Self.decode, c.codecz_zz_decode_i8);
            } else if (V == i16) {
                abi.checkFnSignature(Self.encode, c.codecz_zz_encode_i16);
                abi.checkFnSignature(Self.decode, c.codecz_zz_decode_i16);
            } else if (V == i32) {
                abi.checkFnSignature(Self.encode, c.codecz_zz_encode_i32);
                abi.checkFnSignature(Self.decode, c.codecz_zz_decode_i32);
            } else if (V == i64) {
                abi.checkFnSignature(Self.encode, c.codecz_zz_encode_i64);
                abi.checkFnSignature(Self.decode, c.codecz_zz_decode_i64);
            } else {
                @compileError(std.fmt.comptimePrint("ZigZag: unsupported type {}", .{@typeName(V)}));
            }
        }
    };
}

//
// custom panic handler
//
const stack_trace_frames = 10;
var stack_address: [stack_trace_frames]usize = [_]usize{0} ** stack_trace_frames;

pub fn panic(msg: []const u8, error_return_trace: ?*std.builtin.StackTrace, ret_addr: ?usize) noreturn {
    const stderr = std.io.getStdErr().writer();
    if (error_return_trace) |trace| {
        stderr.print("\nError return trace:\n", .{}) catch {};
        std.debug.dumpStackTrace(trace.*);
    }

    stderr.print("\nCurrent stack trace:\n", .{}) catch {};
    std.debug.dumpCurrentStackTrace(ret_addr);

    stderr.print("\nManually collected stack trace:\n", .{}) catch {};
    @memset(&stack_address, 0);
    const first_trace_addr = ret_addr orelse @returnAddress();
    var stack_trace = std.builtin.StackTrace{
        .instruction_addresses = &stack_address,
        .index = 0,
    };
    std.debug.captureStackTrace(first_trace_addr, &stack_trace);
    std.debug.dumpStackTrace(stack_trace);

    stderr.print("\nDelegating to std.debug.panicImpl with message: {s}\n", .{msg}) catch {};
    std.debug.panicImpl(error_return_trace, ret_addr, msg);
}
