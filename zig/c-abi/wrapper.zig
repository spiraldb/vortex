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
const OutputAlign: u29 = c.VORTEX_ALIGNMENT;
const AlpExponents = abi.AlpExponents;
const ByteBuffer = abi.ByteBuffer;
const ResultStatus = abi.ResultStatus;
const WrittenBuffer = abi.WrittenBuffer;
const OneBufferResult = abi.OneBufferResult;
const TwoBufferResult = abi.TwoBufferResult;
const AlpExponentsResult = abi.AlpExponentsResult;
const OneBufferNumExceptionsResult = abi.OneBufferNumExceptionsResult;

const UnsignedIntegerTypes = [_]type{ u8, u16, u32, u64 };
const SignedIntegerTypes = [_]type{ i8, i16, i32, i64 };
const IntegerTypes = UnsignedIntegerTypes ++ SignedIntegerTypes;
const SizeTypes = [_]type{u32};
const FloatTypes = [_]type{ f32, f64 };
const NumberTypes = IntegerTypes ++ FloatTypes;
const BitPackingWidths = [_]comptime_int{ 1, 2, 4, 8, 16, 32 };

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
        const codec = encodings.RunEnd(V2, E, OutputAlign);

        pub fn encode(elems: [*c]V, len: usize, out: [*c]c.TwoBufferResult_t) callconv(.C) void {
            // this verifies alignment and returns an error result if the buffer is not properly aligned
            const zigOut = TwoBufferResult.from(out.*) catch |err| return TwoBufferResult.errOut(err, V, E, out);
            const valuesBuf = zigOut.first.buffer.checkAlignment() catch |err| return TwoBufferResult.errOut(err, V, E, out);
            const runEndsBuf = zigOut.second.buffer.checkAlignment() catch |err| return TwoBufferResult.errOut(err, V, E, out);

            const values: []align(OutputAlign) V2 = @alignCast(std.mem.bytesAsSlice(V2, valuesBuf.bytes()));
            const runEnds: []align(OutputAlign) E = @alignCast(std.mem.bytesAsSlice(E, runEndsBuf.bytes()));
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
            const outBuf = zigOut.buf.buffer.checkAlignment() catch |err| return OneBufferResult.errOut(err, V, out);
            const decoded: []align(OutputAlign) V2 = @alignCast(std.mem.bytesAsSlice(V2, outBuf.bytes()));

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
// Fastlanes Shared Functions
//
fn fastLanesMaxPackedBitWidth(comptime T: u8) u8 {
    if (T == 0) {
        @compileError("Cannot pack 0 bits");
    } else if (T <= 8) {
        return T - 1;
    } else {
        // should make at least a 20% reduction
        return @divFloor(T * 3 + 1, 4);
    }
}

test "max num bits to pack" {
    try std.testing.expectEqual(fastLanesMaxPackedBitWidth(8), 7);
    try std.testing.expectEqual(fastLanesMaxPackedBitWidth(9), 7);
    try std.testing.expectEqual(fastLanesMaxPackedBitWidth(10), 8);
    try std.testing.expectEqual(fastLanesMaxPackedBitWidth(16), 12);
    try std.testing.expectEqual(fastLanesMaxPackedBitWidth(32), 24);
    try std.testing.expectEqual(fastLanesMaxPackedBitWidth(64), 51);
}

//
// Fastlanes Fused Frame of Reference (FFoR) Encoding
//
comptime {
    for (IntegerTypes) |V| {
        const wrapper = FforWrapper(V);
        @export(wrapper.encodedSizeInBytes, std.builtin.ExportOptions{
            .name = "codecz_flbp_encodedSizeInBytes_" ++ @typeName(V),
            .linkage = .Strong,
        });
        @export(wrapper.maxPackedBitWidth, std.builtin.ExportOptions{
            .name = "codecz_flbp_maxPackedBitWidth_" ++ @typeName(V),
            .linkage = .Strong,
        });
        @export(wrapper.encode, std.builtin.ExportOptions{
            .name = "codecz_ffor_encode_" ++ @typeName(V),
            .linkage = .Strong,
        });
        @export(wrapper.decode, std.builtin.ExportOptions{
            .name = "codecz_ffor_decode_" ++ @typeName(V),
            .linkage = .Strong,
        });
        @export(wrapper.decodeSingle, std.builtin.ExportOptions{
            .name = "codecz_ffor_decodeSingle_" ++ @typeName(V),
            .linkage = .Strong,
        });
        @export(wrapper.collectExceptions, std.builtin.ExportOptions{
            .name = "codecz_ffor_collectExceptions_" ++ @typeName(V),
            .linkage = .Strong,
        });
        wrapper.checkFnSignatures();
    }
}

fn FforWrapper(comptime V: type) type {
    const InputAlign: u29 = codecz.encodings.fastlanes.InputAlignment;
    if (@typeInfo(V) != .Int) {
        @compileError("FFoR: Unsupported type " ++ @typeName(V));
    }
    comptime {
        switch (@bitSizeOf(V)) {
            inline 8, 16, 32, 64 => {},
            else => @compileError("FFoR: Unsupported type " ++ @typeName(V)),
        }
    }

    return struct {
        const Self = @This();
        const T = @bitSizeOf(V);
        const maxW = fastLanesMaxPackedBitWidth(T);

        pub fn encodedSizeInBytes(len: usize, num_bits: u8) callconv(.C) usize {
            switch (num_bits) {
                inline 1...maxW => |W| {
                    const codec: type = encodings.FFOR(V, W);
                    return codec.encodedSizeInBytes(len);
                },
                else => return 0,
            }
        }

        pub fn maxPackedBitWidth() callconv(.C) u8 {
            return maxW;
        }

        pub fn encode(elems_: [*c]align(InputAlign) V, len: usize, num_bits: u8, min_val: V, out: [*c]c.OneBufferNumExceptionsResult_t) callconv(.C) void {
            if (!std.mem.isAligned(@intFromPtr(elems_), InputAlign)) {
                OneBufferNumExceptionsResult.errOut(CodecError.IncorrectAlignment, V, out);
            }

            switch (num_bits) {
                inline 1...maxW => |W| {
                    const codec: type = encodings.FFOR(V, W);
                    const numBytes = codec.encodedSizeInBytes(len);
                    const zigOut = OneBufferNumExceptionsResult.from(out.*) catch |err| return OneBufferNumExceptionsResult.errOut(err, V, out);
                    const outBuf = zigOut.encoded.buffer.checkAlignment() catch |err| return OneBufferNumExceptionsResult.errOut(err, V, out);

                    const elems: []align(InputAlign) const V = @alignCast(elems_[0..len]);
                    const encoded: []align(OutputAlign) u8 = @alignCast(std.mem.bytesAsSlice(u8, outBuf.bytes()));
                    if (codec.encodeRaw(elems, @intCast(min_val), encoded)) |num_exceptions| {
                        const result = OneBufferNumExceptionsResult.ok(
                            WrittenBuffer.initFromSlice(u8, outBuf, encoded[0..numBytes]),
                            num_exceptions,
                        );
                        out.* = result.into();
                    } else |err| {
                        OneBufferNumExceptionsResult.errOut(err, V, out);
                    }
                },
                else => OneBufferNumExceptionsResult.errOut(CodecError.InvalidEncodingParameter, V, out),
            }
        }

        pub fn decode(
            encoded_: [*c]c.ByteBuffer_t,
            elems_len: usize,
            num_bits: u8,
            min_val: V,
            out: [*c]c.OneBufferResult_t,
        ) callconv(.C) void {
            switch (num_bits) {
                inline 1...maxW => |W| {
                    const codec: type = encodings.FFOR(V, W);
                    const encoded = ByteBuffer.from(encoded_.*) catch |err| return OneBufferResult.errOut(err, V, out);
                    const zigOut = OneBufferResult.from(out.*) catch |err| return OneBufferResult.errOut(err, V, out);
                    const outBuf = zigOut.buf.buffer.checkAlignment() catch |err| return OneBufferResult.errOut(err, V, out);

                    const decoded: []align(OutputAlign) V = @alignCast(std.mem.bytesAsSlice(V, outBuf.bytes()));
                    if (codec.decodeRaw(encoded.bytes(), elems_len, @intCast(min_val), decoded)) {
                        const result = OneBufferResult.ok(WrittenBuffer.initFromSlice(V, outBuf, decoded[0..elems_len]));
                        out.* = result.into();
                    } else |err| {
                        OneBufferResult.errOut(err, V, out);
                    }
                },
                else => OneBufferResult.errOut(CodecError.InvalidEncodingParameter, V, out),
            }
        }

        pub fn decodeSingle(
            encoded_: [*c]c.ByteBuffer_t,
            elems_len: usize,
            num_bits: u8,
            min_val: V,
            index: usize,
            out: [*c]V,
        ) callconv(.C) ResultStatus {
            switch (num_bits) {
                inline 1...maxW => |W| {
                    const codec: type = encodings.FFOR(V, W);
                    const encoded = ByteBuffer.from(encoded_.*) catch |err| return ResultStatus.fromCodecError(err);
                    if (codec.decodeSingle(encoded.bytes(), elems_len, @intCast(min_val), index)) |value| {
                        out.* = value;
                        return ResultStatus.Ok;
                    } else |err| {
                        return ResultStatus.fromCodecError(err);
                    }
                },
                else => return ResultStatus.InvalidEncodingParameter,
            }
        }

        pub fn collectExceptions(elems: [*c]V, len: usize, num_bits: u8, min_val: V, num_exceptions: usize, out: [*c]c.TwoBufferResult_t) callconv(.C) void {
            switch (num_bits) {
                inline 1...maxW => |W| {
                    const codec: type = encodings.FFOR(V, W);
                    const zigOut = TwoBufferResult.from(out.*) catch |err| return TwoBufferResult.errOut(err, V, u1, out);
                    const exceptionsBuf = zigOut.first.buffer.checkAlignment() catch |err| return TwoBufferResult.errOut(err, V, u1, out);
                    var excPosBuf = zigOut.second.buffer.checkAlignment() catch |err| return TwoBufferResult.errOut(err, V, u1, out);
                    excPosBuf.fillZeroes();

                    const exceptions: []align(OutputAlign) V = @alignCast(std.mem.bytesAsSlice(V, exceptionsBuf.bytes()));
                    var excPositions = excPosBuf.bits(len) catch |err| return TwoBufferResult.errOut(err, V, u1, out);
                    if (codec.collectExceptions(elems[0..len], @intCast(min_val), num_exceptions, exceptions, &excPositions)) {
                        const first = WrittenBuffer.initFromSlice(V, exceptionsBuf, exceptions[0..num_exceptions]); // autofix
                        const second = WrittenBuffer.initFromBitSlice(excPosBuf, excPositions, num_exceptions);
                        const result = TwoBufferResult.ok(first, second);
                        out.* = result.into();
                    } else |err| {
                        TwoBufferResult.errOut(err, V, u1, out);
                    }
                },
                else => TwoBufferResult.errOut(CodecError.InvalidEncodingParameter, V, u1, out),
            }
        }

        pub fn checkFnSignatures() void {
            if (V == u8) {
                abi.checkFnSignature(Self.encodedSizeInBytes, c.codecz_flbp_encodedSizeInBytes_u8);
                abi.checkFnSignature(Self.encode, c.codecz_ffor_encode_u8);
                abi.checkFnSignature(Self.decode, c.codecz_ffor_decode_u8);
                abi.checkFnSignature(Self.collectExceptions, c.codecz_ffor_collectExceptions_u8);
                abi.checkFnSignature(Self.decodeSingle, c.codecz_ffor_decodeSingle_u8);
            } else if (V == u16) {
                abi.checkFnSignature(Self.encodedSizeInBytes, c.codecz_flbp_encodedSizeInBytes_u16);
                abi.checkFnSignature(Self.encode, c.codecz_ffor_encode_u16);
                abi.checkFnSignature(Self.decode, c.codecz_ffor_decode_u16);
                abi.checkFnSignature(Self.collectExceptions, c.codecz_ffor_collectExceptions_u16);
                abi.checkFnSignature(Self.decodeSingle, c.codecz_ffor_decodeSingle_u16);
            } else if (V == u32) {
                abi.checkFnSignature(Self.encodedSizeInBytes, c.codecz_flbp_encodedSizeInBytes_u32);
                abi.checkFnSignature(Self.encode, c.codecz_ffor_encode_u32);
                abi.checkFnSignature(Self.decode, c.codecz_ffor_decode_u32);
                abi.checkFnSignature(Self.collectExceptions, c.codecz_ffor_collectExceptions_u32);
                abi.checkFnSignature(Self.decodeSingle, c.codecz_ffor_decodeSingle_u32);
            } else if (V == u64) {
                abi.checkFnSignature(Self.encodedSizeInBytes, c.codecz_flbp_encodedSizeInBytes_u64);
                abi.checkFnSignature(Self.encode, c.codecz_ffor_encode_u64);
                abi.checkFnSignature(Self.decode, c.codecz_ffor_decode_u64);
                abi.checkFnSignature(Self.collectExceptions, c.codecz_ffor_collectExceptions_u64);
                abi.checkFnSignature(Self.decodeSingle, c.codecz_ffor_decodeSingle_u64);
            } else if (V == i8) {
                abi.checkFnSignature(Self.encodedSizeInBytes, c.codecz_flbp_encodedSizeInBytes_i8);
                abi.checkFnSignature(Self.encode, c.codecz_ffor_encode_i8);
                abi.checkFnSignature(Self.decode, c.codecz_ffor_decode_i8);
                abi.checkFnSignature(Self.collectExceptions, c.codecz_ffor_collectExceptions_i8);
                abi.checkFnSignature(Self.decodeSingle, c.codecz_ffor_decodeSingle_i8);
            } else if (V == i16) {
                abi.checkFnSignature(Self.encodedSizeInBytes, c.codecz_flbp_encodedSizeInBytes_i16);
                abi.checkFnSignature(Self.encode, c.codecz_ffor_encode_i16);
                abi.checkFnSignature(Self.decode, c.codecz_ffor_decode_i16);
                abi.checkFnSignature(Self.collectExceptions, c.codecz_ffor_collectExceptions_i16);
                abi.checkFnSignature(Self.decodeSingle, c.codecz_ffor_decodeSingle_i16);
            } else if (V == i32) {
                abi.checkFnSignature(Self.encodedSizeInBytes, c.codecz_flbp_encodedSizeInBytes_i32);
                abi.checkFnSignature(Self.encode, c.codecz_ffor_encode_i32);
                abi.checkFnSignature(Self.decode, c.codecz_ffor_decode_i32);
                abi.checkFnSignature(Self.collectExceptions, c.codecz_ffor_collectExceptions_i32);
                abi.checkFnSignature(Self.decodeSingle, c.codecz_ffor_decodeSingle_i32);
            } else if (V == i64) {
                abi.checkFnSignature(Self.encodedSizeInBytes, c.codecz_flbp_encodedSizeInBytes_i64);
                abi.checkFnSignature(Self.encode, c.codecz_ffor_encode_i64);
                abi.checkFnSignature(Self.decode, c.codecz_ffor_decode_i64);
                abi.checkFnSignature(Self.collectExceptions, c.codecz_ffor_collectExceptions_i64);
                abi.checkFnSignature(Self.decodeSingle, c.codecz_ffor_decodeSingle_i64);
            } else {
                @compileError(std.fmt.comptimePrint("FFoR: Unsupported type {s}", .{@typeName(V)}));
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
        @export(wrapper.encodeSingle, std.builtin.ExportOptions{
            .name = "codecz_alp_encodeSingle_" ++ @typeName(F),
            .linkage = .Strong,
        });
        @export(wrapper.decodeSingle, std.builtin.ExportOptions{
            .name = "codecz_alp_decodeSingle_" ++ @typeName(F),
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
            const encBuf = zigOut.first.buffer.checkAlignment() catch |err| return TwoBufferResult.errOut(err, codec.EncInt, u1, out);
            var excPosBuf = zigOut.second.buffer.checkAlignment() catch |err| return TwoBufferResult.errOut(err, codec.EncInt, u1, out);
            excPosBuf.fillZeroes();

            const values: []align(OutputAlign) codec.EncInt = @alignCast(std.mem.bytesAsSlice(codec.EncInt, encBuf.bytes()));
            var excPositions = excPosBuf.bits(elems_len) catch |err| return TwoBufferResult.errOut(err, codec.EncInt, u1, out);

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
            const outSlice: []align(OutputAlign) F = @alignCast(std.mem.bytesAsSlice(F, zigOut.buf.buffer.bytes()));
            if (codec.decodeRaw(input[0..len], exp, outSlice)) {
                const buf = WrittenBuffer.initFromSlice(F, zigOut.buf.buffer, outSlice[0..len]);
                const result = OneBufferResult.ok(buf);
                out.* = result.into();
            } else |err| {
                OneBufferResult.errOut(err, F, out);
            }
        }

        pub fn encodeSingle(value: F, exp_: [*c]c.AlpExponents_t, out: [*c]codec.EncInt) callconv(.C) ResultStatus {
            const exp: AlpExponents = AlpExponents.from(exp_.*);
            out.* = codec.encodeSingle(value, exp);
            return ResultStatus.Ok;
        }

        pub fn decodeSingle(encoded: codec.EncInt, exp_: [*c]c.AlpExponents_t, out: [*c]F) callconv(.C) ResultStatus {
            const exp: AlpExponents = AlpExponents.from(exp_.*);
            out.* = codec.decodeSingle(encoded, exp);
            return ResultStatus.Ok;
        }

        pub fn checkFnSignatures() void {
            if (F == f32) {
                abi.checkFnSignature(Self.sampleFindExponents, c.codecz_alp_sampleFindExponents_f32);
                abi.checkFnSignature(Self.encode, c.codecz_alp_encode_f32);
                abi.checkFnSignature(Self.decode, c.codecz_alp_decode_f32);
                abi.checkFnSignature(Self.encodeSingle, c.codecz_alp_encodeSingle_f32);
                abi.checkFnSignature(Self.decodeSingle, c.codecz_alp_decodeSingle_f32);
            } else if (F == f64) {
                abi.checkFnSignature(Self.sampleFindExponents, c.codecz_alp_sampleFindExponents_f64);
                abi.checkFnSignature(Self.encode, c.codecz_alp_encode_f64);
                abi.checkFnSignature(Self.decode, c.codecz_alp_decode_f64);
                abi.checkFnSignature(Self.encodeSingle, c.codecz_alp_encodeSingle_f64);
                abi.checkFnSignature(Self.decodeSingle, c.codecz_alp_decodeSingle_f64);
            } else {
                @compileError(std.fmt.comptimePrint("ALP: unsupported type {s}", .{@typeName(F)}));
            }
        }
    };
}
