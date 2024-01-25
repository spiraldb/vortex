const std = @import("std");
const builtin = @import("builtin");

const c = @cImport({
    @cInclude("wrapper.h");
});

pub const Alignment: u29 = c.SPIRAL_ALIGNMENT;

pub const CodecError = error{
    InvalidInput,
    IncorrectAlignment,
    EncodingFailed,
    OutputBufferTooSmall,
} || std.mem.Allocator.Error;

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
    len: u64,

    pub fn from(cbb: c.ByteBuffer_t) CodecError!ByteBuffer {
        if (!std.mem.isAligned(@intFromPtr(cbb.ptr), Alignment)) {
            return CodecError.IncorrectAlignment;
        }
        return @bitCast(cbb);
    }

    pub fn into(self: ByteBuffer) c.ByteBuffer_t {
        return @bitCast(self);
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

    pub fn from(cwb: c.WrittenBuffer_t) CodecError!WrittenBuffer {
        _ = try ByteBuffer.from(cwb.buffer);
        return @bitCast(cwb);
    }

    pub fn into(self: WrittenBuffer) c.WrittenBuffer_t {
        return @bitCast(self);
    }
};

pub const OneBufferResult = extern struct {
    status: ResultStatus,
    buffer: WrittenBuffer,

    pub fn from(cobr: c.OneBufferResult_t) CodecError!OneBufferResult {
        _ = try WrittenBuffer.from(cobr.buffer);
        return @bitCast(cobr);
    }

    pub fn into(self: OneBufferResult) c.OneBufferResult_t {
        return @bitCast(self);
    }
};

pub const TwoBufferResult = extern struct {
    status: ResultStatus,
    firstBuffer: WrittenBuffer,
    secondBuffer: WrittenBuffer,

    pub fn from(ctbr: c.TwoBufferResult_t) CodecError!TwoBufferResult {
        _ = try WrittenBuffer.from(ctbr.firstBuffer);
        _ = try WrittenBuffer.from(ctbr.secondBuffer);
        return @bitCast(ctbr);
    }

    pub fn into(self: TwoBufferResult) c.TwoBufferResult_t {
        return @bitCast(self);
    }
};

pub const AlpExponents = c.AlpExponents_t;

pub const AlpExponentsResult = extern struct {
    status: ResultStatus,
    exponents: AlpExponents,

    pub fn from(exp: c.AlpExponentsResult_t) AlpExponentsResult {
        return @bitCast(exp);
    }

    pub fn into(self: AlpExponentsResult) c.AlpExponentsResult_t {
        return @bitCast(self);
    }
};

pub const RunLengthStats = c.RunLengthStats_t;

comptime {
    checkStructABI(ByteBuffer, c.ByteBuffer_t);
    checkStructABI(WrittenBuffer, c.WrittenBuffer_t);
    checkStructABI(OneBufferResult, c.OneBufferResult_t);
    checkStructABI(TwoBufferResult, c.TwoBufferResult_t);
    checkStructABI(AlpExponents, c.AlpExponents_t);
    checkStructABI(AlpExponentsResult, c.AlpExponentsResult_t);
    checkStructABI(RunLengthStats, c.RunLengthStats_t);
}

pub fn checkStructABI(comptime zigType: type, comptime cType: type) void {
    if (@bitSizeOf(zigType) != @bitSizeOf(cType)) {
        @compileError(std.fmt.comptimePrint(
            "Mismatched size between zig type {s} ({} bits) and C type {s} ({} bits)",
            .{ @typeName(zigType), @bitSizeOf(zigType), @typeName(cType), @bitSizeOf(cType) },
        ));
    }

    const zigTypeInfo = @typeInfo(zigType);
    const cTypeInfo = @typeInfo(cType);
    if (zigTypeInfo == .Struct and cTypeInfo == .Struct) {
        for (zigTypeInfo.Struct.fields, cTypeInfo.Struct.fields) |zf, cf| {
            if (!std.mem.eql(u8, zf.name, cf.name)) {
                @compileError(std.fmt.comptimePrint(
                    "Mismatch between zig field {s} and C field {s}",
                    .{ zf.name, cf.name },
                ));
            }
            if (zf.alignment != cf.alignment) {
                @compileError(std.fmt.comptimePrint(
                    "Mismatch between zig field {s} (alignment {}) and C field of the same name (alignment {})",
                    .{ zf.name, zf.alignment, cf.alignment },
                ));
            }
            if (zf.is_comptime != cf.is_comptime) {
                @compileError(std.fmt.comptimePrint(
                    "Mismatch between zig field {s} (is_comptime {}) and C field of the same name (is_comptime {})",
                    .{ zf.name, zf.is_comptime, cf.is_comptime },
                ));
            }
            @setEvalBranchQuota(1_000_000);
            checkABI(zf.name, zf.type, cf.type);
        }
    } else {
        @compileError(std.fmt.comptimePrint(
            "Called checkStructABI on zig type {s} and C type {s} (at least one is not a Struct)",
            .{ @typeName(zigType), @typeName(cType) },
        ));
    }
}

fn checkABI(comptime name: []const u8, comptime zigType: type, comptime cType: type) void {
    if (zigType == cType) {
        return;
    }

    const zigTypeInfo = @typeInfo(zigType);
    const cTypeInfo = @typeInfo(cType);
    if (zigTypeInfo == .Struct and cTypeInfo == .Struct) {
        checkStructABI(zigType, cType);
        return;
    }

    if (@bitSizeOf(zigType) != @bitSizeOf(cType)) {
        @compileError(std.fmt.comptimePrint(
            "Mismatched size between {s} with zig type {s} ({} bits) and C type {s} ({} bits)",
            .{ name, @typeName(zigType), @bitSizeOf(zigType), @typeName(cType), @bitSizeOf(cType) },
        ));
    }
    if (zigTypeInfo == .Enum) {
        if (zigTypeInfo.Enum.tag_type != cType) {
            @compileError(std.fmt.comptimePrint(
                "Mismatch between zig extern enum {s} (type {s}) and C field of the same name (type {s})",
                .{ name, @typeName(zigType), @typeName(cType) },
            ));
        }
    } else if (zigTypeInfo == .Pointer) {
        if (zigTypeInfo.Pointer.child != cTypeInfo.Pointer.child) {
            @compileError(std.fmt.comptimePrint(
                "Mismatch between {s} with zig type {s} and C type {s}",
                .{ name, @typeName(zigType), @typeName(cType) },
            ));
        }
        if (!std.mem.eql(u8, name, "param") and !std.mem.eql(u8, name, "return_type")) {
            if (zigTypeInfo.Pointer.alignment != Alignment) {
                @compileError(std.fmt.comptimePrint(
                    "Zig {s} with type {s} is a pointer with alignment {}, should have alignment {}",
                    .{ name, @typeName(zigType), zigTypeInfo.Pointer.alignment, Alignment },
                ));
            }
        }
    } else if (zigTypeInfo == .Int) {
        if (zigTypeInfo.Int.signedness != cTypeInfo.Int.signedness or zigTypeInfo.Int.bits != cTypeInfo.Int.bits) {
            @compileError(std.fmt.comptimePrint(
                "Mismatch between zig {s} (type {s}) and C field of the same name (type {s})",
                .{ name, @typeName(zigType), @typeName(cType) },
            ));
        }
    } else {
        @compileError(std.fmt.comptimePrint(
            "Mismatch between zig {s} (type {s}) and C field of the same name (type {s})",
            .{ name, @typeName(zigType), @typeName(cType) },
        ));
    }
}

pub fn checkFnSignature(zfunc: anytype, cfunc: anytype) void {
    const zfn = @typeInfo(@TypeOf(zfunc)).Fn;
    const cfn = @typeInfo(@TypeOf(cfunc)).Fn;
    if (zfn.alignment != cfn.alignment) {
        @compileError(std.fmt.comptimePrint(
            "Mismatch between zig function {s} (alignment {}) and C function of the same name (alignment {})",
            .{ @typeName(zfunc), zfn.alignment, cfn.alignment },
        ));
    }
    if (zfn.calling_convention != cfn.calling_convention) {
        @compileError(std.fmt.comptimePrint(
            "Mismatch between zig function {s} (calling convention {}) and C function of the same name (calling convention {})",
            .{ @typeName(zfunc), zfn.calling_convention, cfn.calling_convention },
        ));
    }
    if (zfn.is_generic != cfn.is_generic) {
        @compileError(std.fmt.comptimePrint(
            "Mismatch between zig function {s} (is_generic {}) and C function of the same name (is_generic {})",
            .{ @typeName(zfunc), zfn.is_generic, cfn.is_generic },
        ));
    }
    if (zfn.is_var_args != cfn.is_var_args) {
        @compileError(std.fmt.comptimePrint(
            "Mismatch between zig function {s} (is_var_args {}) and C function of the same name (is_var_args {})",
            .{ @typeName(zfunc), zfn.is_var_args, cfn.is_var_args },
        ));
    }

    if (zfn.return_type != cfn.return_type) {
        @setEvalBranchQuota(1_000_000);
        checkABI("return_type", zfn.return_type.?, cfn.return_type.?);
    }
    for (zfn.params, cfn.params) |zparam, cparam| {
        if (zparam.type != cparam.type) {
            @setEvalBranchQuota(1_000_000);
            checkABI("param", zparam.type.?, cparam.type.?);
        }
    }
}
