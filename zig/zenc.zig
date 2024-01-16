const std = @import("std");
const builtin = @import("builtin");
const zimd = @import("zimd");
const codecz = @import("codecs");
const CodecError = codecz.CodecError;

const c = @cImport({
    @cInclude("zenc.h");
});

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

const Alignment: u29 = c.SPIRAL_ALIGNMENT;
const INTEGERS = [_]type{ u8, u16, u32, u64, i8, i16, i32, i64 };
const SIZES = [_]type{ u32, u64 };
const FLOATS = [_]type{ f32, f64 };

//
// Zimd Math
//

comptime {
    const MATH_TYPES = INTEGERS ++ FLOATS;
    for (MATH_TYPES) |T| {
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
// Codecz
//

const EncodeDecodeCtx = enum {
    encode,
    decode,
};

const ResultStatus = enum(c.ResultStatus_t) {
    Ok,
    // errors
    InvalidInput,
    IncorrectAlignment,
    EncodingFailed,
    OutputBufferTooSmall,
    OutOfMemory,
    UnknownCodecError,

    pub fn fromCodecError(err: CodecError) ResultStatus {
        if (err == CodecError.InvalidInput) {
            return ResultStatus.InvalidInput;
        } else if (err == CodecError.IncorrectAlignment) {
            return ResultStatus.IncorrectAlignment;
        } else if (err == CodecError.EncodingFailed) {
            return ResultStatus.EncodingFailed;
        } else if (err == CodecError.OutputBufferTooSmall) {
            return ResultStatus.OutputBufferTooSmall;
        } else if (err == CodecError.OutOfMemory) {
            return ResultStatus.OutOfMemory;
        } else {
            return ResultStatus.UnknownCodecError;
        }
    }
};

test "result status" {
    try std.testing.expectEqual(c.Ok, @intFromEnum(ResultStatus.Ok));
    try std.testing.expectEqual(c.InvalidInput, @intFromEnum(ResultStatus.InvalidInput));
    try std.testing.expectEqual(c.IncorrectAlignment, @intFromEnum(ResultStatus.IncorrectAlignment));
    try std.testing.expectEqual(c.EncodingFailed, @intFromEnum(ResultStatus.EncodingFailed));
    try std.testing.expectEqual(c.OutputBufferTooSmall, @intFromEnum(ResultStatus.OutputBufferTooSmall));
    try std.testing.expectEqual(c.OutOfMemory, @intFromEnum(ResultStatus.OutOfMemory));
    try std.testing.expectEqual(c.UnknownCodecError, @intFromEnum(ResultStatus.UnknownCodecError));
}

const ByteBuffer = extern struct {
    ptr: [*c]align(Alignment) u8,
    len: usize,

    pub fn cast(cbb: *c.ByteBuffer_t) CodecError!*ByteBuffer {
        if (!std.mem.isAligned(cbb.ptr, Alignment)) {
            return CodecError.IncorrectAlignment;
        }
        return @ptrCast(cbb);
    }
};

const WrittenBuffer = extern struct {
    buffer: ByteBuffer,
    bitSizePerElement: u8,
    numElements: u64,
    inputBytesUsed: u64,

    pub fn cast(cwb: *c.WrittenBuffer_t) CodecError!*WrittenBuffer {
        _ = try ByteBuffer.cast(&cwb.buffer);
        return @ptrCast(cwb);
    }
};

const OneBufferResult = extern struct {
    status: ResultStatus,
    buffer: WrittenBuffer,

    pub fn cast(cobr: *c.OneBufferResult_t) CodecError!*OneBufferResult {
        _ = try WrittenBuffer.cast(&cobr.buffer);
        return @ptrCast(cobr);
    }
};

const TwoBufferResult = extern struct {
    status: ResultStatus,
    firstBuffer: WrittenBuffer,
    secondBuffer: WrittenBuffer,

    pub fn cast(ctbr: *c.TwoBufferResult_t) CodecError!*TwoBufferResult {
        _ = try WrittenBuffer.cast(&ctbr.firstBuffer);
        _ = try WrittenBuffer.cast(&ctbr.secondBuffer);
        return @ptrCast(ctbr);
    }
};

comptime {
    checkABI(ByteBuffer, c.ByteBuffer_t);
    checkABI(WrittenBuffer, c.WrittenBuffer_t);
    checkABI(OneBufferResult, c.OneBufferResult_t);
    checkABI(TwoBufferResult, c.TwoBufferResult_t);
}

comptime {
    const REE_TYPES = INTEGERS;
    for (REE_TYPES) |V| {
        for (SIZES) |E| {
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
        const codec = codecz.RunEnd(V, E, Alignment);

        pub fn encode(elems: [*c]V, elems_len: usize, values_buf: ByteBuffer, runends_buf: ByteBuffer) callconv(.C) TwoBufferResult {
            const values: []align(Alignment) V = @alignCast(std.mem.bytesAsSlice(V, values_buf.ptr[0..values_buf.len]));
            const runends: []align(Alignment) E = @alignCast(std.mem.bytesAsSlice(E, runends_buf.ptr[0..runends_buf.len]));

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
                .values = @alignCast(std.mem.bytesAsSlice(V, values.ptr[0..values.len])),
                .runends = @alignCast(std.mem.bytesAsSlice(E, runends.ptr[0..runends.len])),
                .numRuns = numRuns,
            };

            const outSlice: []align(Alignment) V = @alignCast(std.mem.bytesAsSlice(V, out.ptr[0..out.len]));
            codec.decode(encoded, outSlice) catch |err| {
                return OneBufferResult{
                    .status = ResultStatus.fromCodecError(err),
                    .buffer = WrittenBuffer{
                        .buffer = out,
                        .bitSizePerElement = @bitSizeOf(V),
                        .inputBytesUsed = 0,
                        .numElements = 0,
                    },
                };
            };
            return OneBufferResult{
                .status = ResultStatus.Ok,
                .buffer = WrittenBuffer{
                    .buffer = out,
                    .bitSizePerElement = @bitSizeOf(V),
                    .inputBytesUsed = std.mem.sliceAsBytes(outSlice).len,
                    .numElements = outSlice.len,
                },
            };
        }
    };
}

fn checkABI(comptime zigType: type, comptime cType: type) void {
    if (@sizeOf(zigType) != @sizeOf(cType)) {
        @compileError(std.fmt.comptimePrint(
            "Mismatch between zig type {s} ({} bytes) and C type {s} ({} bytes)",
            .{ @typeName(zigType), @sizeOf(zigType), @typeName(cType), @sizeOf(cType) },
        ));
    }
    for (@typeInfo(zigType).Struct.fields, @typeInfo(cType).Struct.fields) |zf, cf| {
        if (!std.mem.eql(u8, zf.name, cf.name)) {
            @compileError(std.fmt.comptimePrint(
                "Mismatch between zig field {s} and C field {s}",
                .{ zf.name, cf.name },
            ));
        }
        if (@typeInfo(zf.type) == .Pointer and @typeInfo(cf.type) == .Pointer) {
            if (@typeInfo(zf.type).Pointer.child != @typeInfo(cf.type).Pointer.child) {
                @compileError(std.fmt.comptimePrint(
                    "Mismatch between zig field {s} (type {s}) and C field of the same name (type {s})",
                    .{ zf.name, @typeName(zf.type), @typeName(cf.type) },
                ));
            }
            if (@typeInfo(zf.type).Pointer.alignment != Alignment) {
                @compileError(std.fmt.comptimePrint(
                    "Zig field {s} on type {s} is a pointer with alignment {}, should have alignment {}",
                    .{ zf.name, @typeName(zigType), zf.alignment, Alignment },
                ));
            }
        } else if (zf.type != cf.type) {
            if (@typeInfo(zf.type) == .Struct and @typeInfo(cf.type) == .Struct) {
                checkABI(zf.type, cf.type);
            } else if (@typeInfo(zf.type) == .Enum) {
                if (@typeInfo(zf.type).Enum.tag_type != cf.type) {
                    @compileError(std.fmt.comptimePrint(
                        "Mismatch between zig extern enum {s} (type {s}) and C field of the same name (type {s})",
                        .{ zf.name, @typeName(zf.type), @typeName(cf.type) },
                    ));
                }
            } else {
                @compileError(std.fmt.comptimePrint(
                    "Mismatch between zig field {s} (type {s}) and C field of the same name (type {s})",
                    .{ zf.name, @typeName(zf.type), @typeName(cf.type) },
                ));
            }
        }
    }
}

// pub fn main() !void {
//     @setEvalBranchQuota(100_000);

//     var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
//     defer arena.deinit();
//     const allocator = arena.allocator();

//     var buffer = std.ArrayList(u8).init(allocator);
//     try buffer.writer().print(
//         \\ //////////////////////////////////////////////////////////
//         \\ // This file was auto-generated by header.zig           //
//         \\ //              Do not manually modify.                 //
//         \\ //////////////////////////////////////////////////////////
//     , .{});
// }
