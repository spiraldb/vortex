const std = @import("std");

pub const CodecError = error{
    InvalidInput,
    IncorrectAlignment,
    EncodingFailed,
    OutputBufferTooSmall,
} || std.mem.Allocator.Error;
