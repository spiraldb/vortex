const std = @import("std");
const builtin = @import("builtin");
const codecz = @import("codecz");
const AlpExponents = codecz.codecs.AlpExponents;
const CodecError = codecz.CodecError;

const c = @cImport({
    @cInclude("wrapper.h");
});

pub const Alignment: u29 = c.SPIRAL_ALIGNMENT;

//
// Codecz
//
pub const ResultStatus = enum(c.ResultStatus_t) {
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

pub const ByteBuffer = extern struct {
    ptr: [*c]align(Alignment) u8,
    len: usize,

    pub fn cast(cbb: *c.ByteBuffer_t) CodecError!*ByteBuffer {
        if (!std.mem.isAligned(cbb.ptr, Alignment)) {
            return CodecError.IncorrectAlignment;
        }
        return @ptrCast(cbb);
    }

    pub fn bytes(self: *const ByteBuffer) []u8 {
        return self.ptr[0..self.len];
    }
};

pub const WrittenBuffer = extern struct {
    buffer: ByteBuffer,
    bitSizePerElement: u8,
    numElements: u64,
    inputBytesUsed: u64,

    pub fn cast(cwb: *c.WrittenBuffer_t) CodecError!*WrittenBuffer {
        _ = try ByteBuffer.cast(&cwb.buffer);
        return @ptrCast(cwb);
    }
};

pub const OneBufferResult = extern struct {
    status: ResultStatus,
    buffer: WrittenBuffer,

    pub fn cast(cobr: *c.OneBufferResult_t) CodecError!*OneBufferResult {
        _ = try WrittenBuffer.cast(&cobr.buffer);
        return @ptrCast(cobr);
    }
};

pub const TwoBufferResult = extern struct {
    status: ResultStatus,
    firstBuffer: WrittenBuffer,
    secondBuffer: WrittenBuffer,

    pub fn cast(ctbr: *c.TwoBufferResult_t) CodecError!*TwoBufferResult {
        _ = try WrittenBuffer.cast(&ctbr.firstBuffer);
        _ = try WrittenBuffer.cast(&ctbr.secondBuffer);
        return @ptrCast(ctbr);
    }
};

pub const AlpExponentsResult = extern struct {
    status: ResultStatus,
    exponents: AlpExponents,
};

comptime {
    checkABI(ByteBuffer, c.ByteBuffer_t);
    checkABI(WrittenBuffer, c.WrittenBuffer_t);
    checkABI(OneBufferResult, c.OneBufferResult_t);
    checkABI(TwoBufferResult, c.TwoBufferResult_t);
    checkABI(AlpExponentsResult, c.AlpExponentsResult_t);
}

pub fn checkABI(comptime zigType: type, comptime cType: type) void {
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
