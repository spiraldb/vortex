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

    pub fn from(crs: c.ResultStatus_t) ResultStatus {
        return @enumFromInt(crs);
    }
};

pub const RunLengthStats = c.RunLengthStats_t;

pub const ByteBuffer = extern struct {
    const Self = @This();
    ptr: [*c]align(Alignment) u8,
    len: u64,

    pub fn initFromSlice(slice: anytype) ByteBuffer {
        const sliceBytes = std.mem.sliceAsBytes(slice);
        if (@typeInfo(@TypeOf(sliceBytes)).Pointer.alignment != Alignment) {
            @compileError(std.fmt.comptimePrint("ByteBuffer.initFromSlice called with slice that is not aligned to {}", .{Alignment}));
        }
        return initFromBytes(sliceBytes);
    }

    pub fn initFromBytes(sliceBytes: []align(Alignment) u8) ByteBuffer {
        return ByteBuffer{
            .ptr = @ptrCast(sliceBytes.ptr),
            .len = @intCast(sliceBytes.len),
        };
    }

    pub fn from(cbb: c.ByteBuffer_t) CodecError!ByteBuffer {
        const bb: Self = @bitCast(cbb);
        return bb.check();
    }

    pub fn check(self: Self) CodecError!Self {
        if (!std.mem.isAligned(@intFromPtr(self.ptr), Alignment)) {
            return CodecError.IncorrectAlignment;
        }
        return self;
    }

    pub fn fillZeroes(self: *Self) void {
        @memset(self.bytes(), 0);
    }

    pub fn into(self: ByteBuffer) c.ByteBuffer_t {
        return @bitCast(self);
    }

    pub fn bytes(self: *const ByteBuffer) []align(Alignment) u8 {
        return self.ptr[0..self.len];
    }

    pub fn bits(self: *const ByteBuffer, len: usize) CodecError!std.PackedIntSlice(u1) {
        if (self.len * 8 < len) {
            return CodecError.OutputBufferTooSmall;
        }
        return std.PackedIntSlice(u1){
            .bytes = self.bytes(),
            .bit_offset = 0,
            .len = len,
        };
    }
};

pub const WrittenBuffer = extern struct {
    buffer: ByteBuffer,
    bitSizePerElement: u8,
    numElements: u64,
    inputBytesUsed: u64,

    pub fn initEmpty(comptime T: type, buffer: ByteBuffer) WrittenBuffer {
        return WrittenBuffer{
            .buffer = buffer,
            .bitSizePerElement = @bitSizeOf(T),
            .numElements = 0,
            .inputBytesUsed = 0,
        };
    }

    pub fn initFromSlice(comptime T: type, buffer: ByteBuffer, slice: []align(Alignment) const T) WrittenBuffer {
        const bytes = std.mem.sliceAsBytes(slice);
        return WrittenBuffer{
            .buffer = buffer,
            .bitSizePerElement = @bitSizeOf(T),
            .numElements = @intCast(slice.len),
            .inputBytesUsed = @intCast(bytes.len),
        };
    }

    pub fn initFromBitSlice(buffer: ByteBuffer, slice: std.PackedIntSlice(u1), cardinality: usize) WrittenBuffer {
        return WrittenBuffer{
            .buffer = buffer,
            .bitSizePerElement = @bitSizeOf(u1),
            .numElements = @intCast(cardinality),
            .inputBytesUsed = slice.bytes.len,
        };
    }

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
    buf: WrittenBuffer,

    pub fn from(cobr: c.OneBufferResult_t) CodecError!OneBufferResult {
        _ = try WrittenBuffer.from(cobr.buf);
        return @bitCast(cobr);
    }

    pub fn into(self: OneBufferResult) c.OneBufferResult_t {
        return @bitCast(self);
    }

    pub fn ok(buffer: WrittenBuffer) OneBufferResult {
        return OneBufferResult{
            .status = ResultStatus.Ok,
            .buf = buffer,
        };
    }

    pub fn empty(buffer: ByteBuffer) OneBufferResult {
        const writtenBuffer = WrittenBuffer.initEmpty(u8, buffer);
        return OneBufferResult{
            .status = ResultStatus.UnknownCodecError,
            .buf = writtenBuffer,
        };
    }

    pub fn err(err_: CodecError, buffer: WrittenBuffer) OneBufferResult {
        return OneBufferResult{
            .status = ResultStatus.fromCodecError(err_),
            .buf = buffer,
        };
    }

    pub fn errOut(err_: CodecError, comptime T: type, out: *c.OneBufferResult_t) void {
        // explicitly don't check buffer validity
        const buffer = WrittenBuffer.initEmpty(T, @bitCast(out.buf.buffer));
        const result = OneBufferResult.err(err_, buffer);
        out.* = result.into();
    }
};

pub const TwoBufferResult = extern struct {
    status: ResultStatus,
    first: WrittenBuffer,
    second: WrittenBuffer,

    pub fn from(ctbr: c.TwoBufferResult_t) CodecError!TwoBufferResult {
        _ = try WrittenBuffer.from(ctbr.first);
        _ = try WrittenBuffer.from(ctbr.second);
        return @bitCast(ctbr);
    }

    pub fn into(self: TwoBufferResult) c.TwoBufferResult_t {
        return @bitCast(self);
    }

    pub fn ok(first: WrittenBuffer, second: WrittenBuffer) TwoBufferResult {
        return TwoBufferResult{
            .status = ResultStatus.Ok,
            .first = first,
            .second = second,
        };
    }

    pub fn empty(first: ByteBuffer, second: ByteBuffer) TwoBufferResult {
        const firstBuffer = WrittenBuffer.initEmpty(u8, first);
        const secondBuffer = WrittenBuffer.initEmpty(u8, second);
        return TwoBufferResult{
            .status = ResultStatus.UnknownCodecError,
            .first = firstBuffer,
            .second = secondBuffer,
        };
    }

    pub fn err(err_: CodecError, first: WrittenBuffer, second: WrittenBuffer) TwoBufferResult {
        return TwoBufferResult{
            .status = ResultStatus.fromCodecError(err_),
            .first = first,
            .second = second,
        };
    }

    pub fn errOut(err_: CodecError, comptime T1: type, comptime T2: type, out: *c.TwoBufferResult_t) void {
        // explicitly don't check buffer validity
        const first = WrittenBuffer.initEmpty(T1, @bitCast(out.first.buffer));
        const second = WrittenBuffer.initEmpty(T2, @bitCast(out.second.buffer));
        const result = TwoBufferResult.err(err_, first, second);
        out.* = result.into();
    }
};

pub const AlpExponents = extern struct {
    e: u8 = 0,
    f: u8 = 0,

    pub fn from(exp: c.AlpExponents_t) AlpExponents {
        return @bitCast(exp);
    }

    pub fn into(self: AlpExponents) c.AlpExponents_t {
        return @bitCast(self);
    }
};

pub const AlpExponentsResult = extern struct {
    status: ResultStatus,
    exponents: AlpExponents,

    pub fn from(exp: c.AlpExponentsResult_t) AlpExponentsResult {
        return @bitCast(exp);
    }

    pub fn into(self: AlpExponentsResult) c.AlpExponentsResult_t {
        return @bitCast(self);
    }

    pub fn ok(exp: AlpExponents) AlpExponentsResult {
        return AlpExponentsResult{
            .status = ResultStatus.Ok,
            .exponents = exp,
        };
    }

    pub fn empty() AlpExponentsResult {
        return AlpExponentsResult{
            .status = ResultStatus.UnknownCodecError,
            .exponents = AlpExponents{ .e = std.math.maxInt(u8), .f = std.math.maxInt(u8) },
        };
    }

    pub fn err(err_: CodecError) AlpExponentsResult {
        return AlpExponentsResult{
            .status = ResultStatus.fromCodecError(err_),
            .exponents = AlpExponents{ .e = std.math.maxInt(u8), .f = std.math.maxInt(u8) },
        };
    }
};

pub const PackedIntsResult = extern struct {
    const Self = @This();

    status: ResultStatus,
    encoded: WrittenBuffer,
    num_exceptions: u64,

    pub fn from(cresult: c.PackedIntsResult_t) CodecError!Self {
        _ = try WrittenBuffer.from(cresult.encoded);
        return @bitCast(cresult);
    }

    pub fn into(self: Self) c.PackedIntsResult_t {
        return @bitCast(self);
    }

    pub fn ok(buffer: WrittenBuffer, num_exceptions: usize) Self {
        return Self{
            .status = ResultStatus.Ok,
            .encoded = buffer,
            .num_exceptions = @intCast(num_exceptions),
        };
    }

    pub fn empty(buffer: ByteBuffer) Self {
        const writtenBuffer = WrittenBuffer.initEmpty(u8, buffer);
        return Self{
            .status = ResultStatus.UnknownCodecError,
            .encoded = writtenBuffer,
            .num_exceptions = 0,
        };
    }

    pub fn err(err_: CodecError, buffer: WrittenBuffer) Self {
        return Self{
            .status = ResultStatus.fromCodecError(err_),
            .encoded = buffer,
            .num_exceptions = 0,
        };
    }

    pub fn errOut(err_: CodecError, comptime T: type, out: *c.PackedIntsResult_t) void {
        // explicitly don't check buffer validity
        const buffer = WrittenBuffer.initEmpty(T, @bitCast(out.encoded.buffer));
        const result = Self.err(err_, buffer);
        out.* = result.into();
    }
};

pub const FforResult = extern struct {
    const Self = @This();

    status: ResultStatus,
    encoded: WrittenBuffer,
    min_val: i64,
    num_exceptions: u64,

    pub fn from(cresult: c.FforResult_t) CodecError!Self {
        _ = try WrittenBuffer.from(cresult.encoded);
        return @bitCast(cresult);
    }

    pub fn into(self: Self) c.FforResult_t {
        return @bitCast(self);
    }

    pub fn ok(buffer: WrittenBuffer, min_val: i64, num_exceptions: usize) Self {
        return Self{
            .status = ResultStatus.Ok,
            .encoded = buffer,
            .min_val = min_val,
            .num_exceptions = @intCast(num_exceptions),
        };
    }

    pub fn empty(buffer: ByteBuffer) Self {
        const writtenBuffer = WrittenBuffer.initEmpty(u8, buffer);
        return Self{
            .status = ResultStatus.UnknownCodecError,
            .encoded = writtenBuffer,
            .min_val = 0,
            .num_exceptions = 0,
        };
    }

    pub fn err(err_: CodecError, buffer: WrittenBuffer) Self {
        return Self{
            .status = ResultStatus.fromCodecError(err_),
            .encoded = buffer,
            .min_val = 0,
            .num_exceptions = 0,
        };
    }

    pub fn errOut(err_: CodecError, comptime T: type, out: *c.FforResult_t) void {
        // explicitly don't check buffer validity
        const buffer = WrittenBuffer.initEmpty(T, @bitCast(out.encoded.buffer));
        const result = Self.err(err_, buffer);
        out.* = result.into();
    }
};

comptime {
    checkStructABI(RunLengthStats, c.RunLengthStats_t);
    checkStructABI(ByteBuffer, c.ByteBuffer_t);
    checkStructABI(WrittenBuffer, c.WrittenBuffer_t);
    checkStructABI(OneBufferResult, c.OneBufferResult_t);
    checkStructABI(TwoBufferResult, c.TwoBufferResult_t);
    checkStructABI(AlpExponents, c.AlpExponents_t);
    checkStructABI(AlpExponentsResult, c.AlpExponentsResult_t);
    checkStructABI(PackedIntsResult, c.PackedIntsResult_t);
    checkStructABI(FforResult, c.FforResult_t);
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
        if (zigTypeInfo.Pointer.child == f16) {
            const cChildTypeInfo = @typeInfo(cTypeInfo.Pointer.child);
            if (cChildTypeInfo != .Int or cChildTypeInfo.Int.bits != 16 and cChildTypeInfo.Int.signedness != .Signed) {
                @compileError(std.fmt.comptimePrint("Expected *i16 pointer for f16 type for {s} but got {s} and {s}", .{ name, @typeName(zigType), @typeName(cType) }));
            }
        } else if (zigTypeInfo.Pointer.child != cTypeInfo.Pointer.child) {
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
        if (zigTypeInfo.Pointer.size != cTypeInfo.Pointer.size) {
            @compileError(std.fmt.comptimePrint(
                "Mismatched size between zig {s} (type {s}, size {}) and C field of the same name (type {s}, size {})",
                .{ name, @typeName(zigType), zigTypeInfo.Pointer.size, @typeName(cType), cTypeInfo.Pointer.size },
            ));
        }
        if (zigTypeInfo.Pointer.address_space != cTypeInfo.Pointer.address_space) {
            @compileError(std.fmt.comptimePrint(
                "Mismatched address space between zig {s} (type {}, address_space {s}) and C field of the same name (type {s}, address_space {})",
                .{ name, @typeName(zigType), zigTypeInfo.Pointer.address_space, @typeName(cType), cTypeInfo.Pointer.address_space },
            ));
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
