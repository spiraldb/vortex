pub const arrow = @import("./arrow.zig");
pub const Buffer = @import("./Buffer.zig");
pub const Ctx = @import("./Ctx.zig");
pub usingnamespace @import("./arrays/array.zig");
pub const BinaryArray = @import("./arrays/BinaryArray.zig");
pub const BoolArray = @import("./arrays/BoolArray.zig");
pub const ChunkedArray = @import("./arrays/ChunkedArray.zig");
pub const ConstantArray = @import("./arrays/ConstantArray.zig");
pub const DictionaryArray = @import("./arrays/DictionaryArray.zig");
pub const PatchedArray = @import("./arrays/PatchedArray.zig");
pub const PrimitiveArray = @import("./arrays/PrimitiveArray.zig");
pub const RoaringBoolArray = @import("./arrays/RoaringBoolArray.zig");
pub const RoaringUIntArray = @import("./arrays/RoaringUIntArray.zig");
pub const StructArray = @import("./arrays/StructArray.zig");
pub const dtypes = @import("./dtypes.zig");
pub const DType = dtypes.DType;
pub const DTypeKind = dtypes.DTypeKind;
pub usingnamespace @import("./ptype.zig");

pub usingnamespace @import("./scalar.zig");

pub const Stats = @import("./Stats.zig");

pub const compute = @import("./compute.zig");
pub const ops = @import("./compute/ops.zig");

pub const Error = error{
    FFIRaised,
    IndexOutOfBounds,
    InvalidCast,
    StatisticNotSupported,
    GetElementsNotSupported,
    AsMutableNotSupported,
    UnsupportedTypeForDictionaryEncoding,
    EmptyChunk,
} || arrow.Error;

test {
    const std = @import("std");
    std.testing.refAllDeclsRecursive(@This());
}
