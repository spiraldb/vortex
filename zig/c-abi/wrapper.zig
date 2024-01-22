const std = @import("std");
const builtin = @import("builtin");
const zimd = @import("zimd");
const codecs = @import("codecs");
const c = @cImport({
    @cInclude("wrapper.h");
});
const abiTypes = @import("types.zig");

// aliases
const Alignment: u29 = c.SPIRAL_ALIGNMENT;
const AlpExponents = codecs.AlpExponents;
const CodecError = codecs.CodecError;
const ByteBuffer = abiTypes.ByteBuffer;
const ResultStatus = abiTypes.ResultStatus;
const WrittenBuffer = abiTypes.WrittenBuffer;
const OneBufferResult = abiTypes.OneBufferResult;
const TwoBufferResult = abiTypes.TwoBufferResult;
const AlpExponentsResult = abiTypes.AlpExponentsResult;

const IntegerTypes = [_]type{ u8, u16, u32, u64, i8, i16, i32, i64 };
const SizeTypes = [_]type{ u32, u64 };
const FloatTypes = [_]type{ f32, f64 };
const NumberTypes = IntegerTypes ++ FloatTypes;

comptime {
    if (!builtin.link_libc) {
        @compileError("Must be built with libc in order for zenc-sys (rust) to call zenc (zig) via the C ABI");
    }
    if (@bitSizeOf(usize) != @bitSizeOf(c.expected_zig_usize_t)) {
        @compileError(std.fmt.comptimePrint(
            "Mismatch between usize ({} bits) and the C ABI's uintptr_t ({} bits)",
            .{ @bitSizeOf(usize), @bitSizeOf(c.expected_zig_usize_t) },
        ));
    }
}

//
// Zimd Math
//
comptime {
    abiTypes.checkABI(zimd.math.RunLengthStats, c.RunLengthStats_t);
    for (NumberTypes) |T| {
        const wrapper = MathWrapper(T);
        @export(wrapper.max, std.builtin.ExportOptions{ .name = "zimd_max_" ++ @typeName(T), .linkage = .Strong });
        @export(wrapper.min, std.builtin.ExportOptions{ .name = "zimd_min_" ++ @typeName(T), .linkage = .Strong });
        @export(wrapper.isSorted, std.builtin.ExportOptions{ .name = "zimd_isSorted_" ++ @typeName(T), .linkage = .Strong });
        @export(wrapper.isConstant, std.builtin.ExportOptions{ .name = "zimd_isConstant_" ++ @typeName(T), .linkage = .Strong });
        @export(wrapper.runLengthStats, std.builtin.ExportOptions{ .name = "zimd_runLengthStats_" ++ @typeName(T), .linkage = .Strong });
    }
}

fn MathWrapper(comptime T: type) type {
    return struct {
        pub fn max(elems: [*c]const T, len: usize) callconv(.C) T {
            return zimd.math.max(T, elems[0..len]);
        }

        pub fn min(elems: [*c]const T, len: usize) callconv(.C) T {
            return zimd.math.min(T, elems[0..len]);
        }

        pub fn isSorted(elems: [*c]const T, len: usize) callconv(.C) bool {
            return zimd.math.isSorted(T, elems[0..len]);
        }

        pub fn isConstant(elems: [*c]const T, len: usize) callconv(.C) bool {
            return zimd.math.isConstant(T, elems[0..len]);
        }

        pub fn runLengthStats(elems: [*c]const T, len: usize) callconv(.C) zimd.math.RunLengthStats {
            return zimd.math.runLengthStats(T, elems[0..len]);
        }
    };
}

//
// Run End Encoding
//
comptime {
    const REE_TYPES = IntegerTypes;
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
        }
    }
}

fn RunEndWrapper(comptime V: type, comptime E: type) type {
    return struct {
        const codec = codecs.RunEnd(V, E, Alignment);

        pub fn encode(elems: [*c]V, elems_len: usize, values_buf: ByteBuffer, runends_buf: ByteBuffer) callconv(.C) TwoBufferResult {
            const values: []align(Alignment) V = @alignCast(std.mem.bytesAsSlice(V, values_buf.bytes()));
            const runends: []align(Alignment) E = @alignCast(std.mem.bytesAsSlice(E, runends_buf.bytes()));

            if (codec.encode(elems[0..elems_len], values, runends)) |enc| {
                return TwoBufferResult{
                    .status = ResultStatus.Ok,
                    .firstBuffer = WrittenBuffer{
                        .buffer = values_buf,
                        .bitSizePerElement = @bitSizeOf(V),
                        .numElements = enc.numRuns,
                        .inputBytesUsed = std.mem.sliceAsBytes(values[0..enc.numRuns]).len,
                    },
                    .secondBuffer = WrittenBuffer{
                        .buffer = runends_buf,
                        .bitSizePerElement = @bitSizeOf(E),
                        .numElements = enc.numRuns,
                        .inputBytesUsed = std.mem.sliceAsBytes(runends[0..enc.numRuns]).len,
                    },
                };
            } else |err| {
                return TwoBufferResult{
                    .status = ResultStatus.fromCodecError(err),
                    .firstBuffer = WrittenBuffer{
                        .buffer = values_buf,
                        .bitSizePerElement = @bitSizeOf(V),
                        .inputBytesUsed = 0,
                        .numElements = 0,
                    },
                    .secondBuffer = WrittenBuffer{
                        .buffer = runends_buf,
                        .bitSizePerElement = @bitSizeOf(E),
                        .inputBytesUsed = 0,
                        .numElements = 0,
                    },
                };
            }
        }

        pub fn decode(values: ByteBuffer, runends: ByteBuffer, numRuns: usize, out: ByteBuffer) callconv(.C) OneBufferResult {
            const encoded = codec.Encoded{
                .values = @alignCast(std.mem.bytesAsSlice(V, values.bytes())),
                .runends = @alignCast(std.mem.bytesAsSlice(E, runends.bytes())),
                .numRuns = numRuns,
            };

            const outSlice: []align(Alignment) V = @alignCast(std.mem.bytesAsSlice(V, out.bytes()));
            if (codec.decode(encoded, outSlice)) {
                return OneBufferResult{
                    .status = ResultStatus.Ok,
                    .buffer = WrittenBuffer{
                        .buffer = out,
                        .bitSizePerElement = @bitSizeOf(V),
                        .inputBytesUsed = std.mem.sliceAsBytes(outSlice).len,
                        .numElements = outSlice.len,
                    },
                };
            } else |err| {
                return OneBufferResult{
                    .status = ResultStatus.fromCodecError(err),
                    .buffer = WrittenBuffer{
                        .buffer = out,
                        .bitSizePerElement = @bitSizeOf(V),
                        .inputBytesUsed = 0,
                        .numElements = 0,
                    },
                };
            }
        }
    };
}

//
// Adaptive Lossless Floating Point Encoding
//
comptime {
    abiTypes.checkABI(codecs.AlpExponents, c.AlpExponents_t);
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
    }
}

fn ALPWrapper(comptime F: type) type {
    return struct {
        const codec = codecs.AdaptiveLosslessFloatingPoint(F);

        pub fn sampleFindExponents(elems: [*c]F, elems_len: usize) callconv(.C) AlpExponentsResult {
            if (codec.sampleFindExponents(elems[0..elems_len])) |exponents| {
                return AlpExponentsResult{
                    .status = ResultStatus.Ok,
                    .exponents = exponents,
                };
            } else |err| {
                return AlpExponentsResult{
                    .status = ResultStatus.fromCodecError(err),
                    .exponents = codecs.AlpExponents{ .e = std.math.maxInt(u8), .f = std.math.maxInt(u8) },
                };
            }
        }

        pub fn encode(
            elems: [*c]F,
            elems_len: usize,
            exp: AlpExponents,
            enc_buf: ByteBuffer,
            exc_idx_buf: ByteBuffer,
        ) callconv(.C) TwoBufferResult {
            const values: []align(Alignment) codec.EncInt = @alignCast(std.mem.bytesAsSlice(codec.EncInt, enc_buf.bytes()));
            var exc_idx = std.PackedIntSlice(u1){
                .bytes = exc_idx_buf.bytes(),
                .bit_offset = 0,
                .len = exc_idx_buf.len * 8,
            };

            if (codec.encodeRaw(elems[0..elems_len], exp, values, &exc_idx)) |numExceptions| {
                return TwoBufferResult{
                    .status = ResultStatus.Ok,
                    .firstBuffer = WrittenBuffer{
                        .buffer = enc_buf,
                        .bitSizePerElement = @bitSizeOf(codec.EncInt),
                        .inputBytesUsed = codec.valuesBufferSizeInBytes(elems_len),
                        .numElements = elems_len,
                    },
                    .secondBuffer = WrittenBuffer{
                        .buffer = exc_idx_buf,
                        .bitSizePerElement = @bitSizeOf(u1),
                        .inputBytesUsed = std.math.divCeil(usize, elems_len, 8) catch unreachable,
                        .numElements = numExceptions,
                    },
                };
            } else |err| {
                return TwoBufferResult{
                    .status = ResultStatus.fromCodecError(err),
                    .firstBuffer = WrittenBuffer{
                        .buffer = enc_buf,
                        .bitSizePerElement = @bitSizeOf(codec.EncInt),
                        .inputBytesUsed = 0,
                        .numElements = 0,
                    },
                    .secondBuffer = WrittenBuffer{
                        .buffer = exc_idx_buf,
                        .bitSizePerElement = @bitSizeOf(u1),
                        .inputBytesUsed = 0,
                        .numElements = 0,
                    },
                };
            }
        }

        pub fn decode(input: [*c]codec.EncInt, input_len: usize, exp: AlpExponents, out: ByteBuffer) callconv(.C) OneBufferResult {
            const outSlice: []align(Alignment) F = @alignCast(std.mem.bytesAsSlice(F, out.bytes()));
            if (codec.decodeRaw(input[0..input_len], exp, outSlice)) {
                return OneBufferResult{
                    .status = ResultStatus.Ok,
                    .buffer = WrittenBuffer{
                        .buffer = out,
                        .bitSizePerElement = @bitSizeOf(F),
                        .inputBytesUsed = std.mem.sliceAsBytes(outSlice[0..input_len]).len,
                        .numElements = input_len,
                    },
                };
            } else |err| {
                return OneBufferResult{
                    .status = ResultStatus.fromCodecError(err),
                    .buffer = WrittenBuffer{
                        .buffer = out,
                        .bitSizePerElement = @bitSizeOf(F),
                        .inputBytesUsed = 0,
                        .numElements = 0,
                    },
                };
            }
        }
    };
}
