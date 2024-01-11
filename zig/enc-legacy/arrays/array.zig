//! An Enc Array is the base object representing all arrays in enc.
//!
//! Arrays have a dtype and an encoding. DTypes represent the logical type of the
//! values stored in an enc array. Encodings represent the physical layout of the
//! array.
//!
//! This differs from Apache Arrow where logical and physical are combined in
//! the data type, e.g. LargeString, RunEndEncoded.
//!
//! Arrays are reference counted and immutable whenever the refcnt > 1.
const std = @import("std");
const arrow = @import("arrow");
const enc = @import("../enc.zig");
const rc = @import("../refcnt.zig");
const itertools = @import("../itertools.zig");
const mem = @import("../mem.zig");

pub const ArrayKind = enum {
    binary,
    bool,
    chunked,
    constant,
    dictionary,
    patched,
    primitive,
    roaring_bool,
    roaring_uint,
    struct_,
};

pub fn ArrayEncoding(comptime kind: ArrayKind) type {
    return switch (kind) {
        .binary => enc.BinaryArray,
        .bool => enc.BoolArray,
        .chunked => enc.ChunkedArray,
        .constant => enc.ConstantArray,
        .dictionary => enc.DictionaryArray,
        .patched => enc.PatchedArray,
        .primitive => enc.PrimitiveArray,
        .roaring_bool => enc.RoaringBoolArray,
        .roaring_uint => enc.RoaringUIntArray,
        .struct_ => enc.StructArray,
    };
}

pub fn ArrayEncodingKind(comptime T: type) ?ArrayKind {
    return switch (T) {
        enc.BinaryArray => .binary,
        enc.BoolArray => .bool,
        enc.ChunkedArray => .chunked,
        enc.ConstantArray => .constant,
        enc.DictionaryArray => .dictionary,
        enc.PatchedArray => .patched,
        enc.PrimitiveArray => .primitive,
        enc.RoaringBoolArray => .roaring_bool,
        enc.RoaringUIntArray => .roaring_uint,
        enc.StructArray => .struct_,
        else => null,
    };
}

const knownKinds: []const []const u8 = &.{ "enc.utf8", "enc.bool", "enc.chunked", "enc.constant", "enc.dictionary", "enc.patched", "enc.primitive", "enc.roaring_bool", "enc.roaring_uint", "enc.struct" };

fn idToArrayKind(id: []const u8) ?ArrayKind {
    for (knownKinds, 0..) |k, i| {
        if (std.mem.eql(u8, k, id)) {
            return @enumFromInt(i);
        }
    }
    return null;
}

pub const Array = struct {
    const Self = @This();

    pub const Iterator = itertools.Iterator(*Self);
    pub const Slice = mem.DeinitSlice(*Self, Self.release);

    id: []const u8,
    kind: ?ArrayKind,
    vtable: *const VTable,
    dtype: enc.DType,
    len: usize,
    stats: *enc.Stats,
    allocator: std.mem.Allocator,

    /// Note: callers should use retain instead of init.
    pub fn init(id: []const u8, vtable: *const VTable, allocator: std.mem.Allocator, dtype: enc.DType, len: usize) !Self {
        // We heap-allocate stats so they're mutable (for memoization) even with a const array.
        const stats = try allocator.create(enc.Stats);
        stats.* = .{};

        return .{
            .id = try allocator.dupe(u8, id),
            .kind = idToArrayKind(id),
            .vtable = vtable,
            .dtype = dtype,
            .len = len,
            .stats = stats,
            .allocator = allocator,
        };
    }

    /// Note: callers should use release instead of deinit.
    pub fn deinit(self: *Self) void {
        self.allocator.free(self.id);
        self.dtype.deinit();
        self.stats.deinit();
        self.allocator.destroy(self.stats);
    }

    pub fn retain(self: *const Self) *Self {
        return self.vtable.retain(self);
    }

    pub fn release(self: *Self) void {
        return self.vtable.release(self);
    }

    pub fn asMutable(self: *const Self, allocator: std.mem.Allocator) !*Self {
        return self.vtable.asMutable(self, allocator);
    }

    pub fn getNBytes(self: *const Self) !usize {
        return self.vtable.getNBytes(self);
    }

    pub fn getScalar(self: *const Self, allocator: std.mem.Allocator, index: usize) !enc.Scalar {
        return self.vtable.getScalar(self, allocator, index);
    }

    pub fn getSlice(self: *const Self, allocator: std.mem.Allocator, start: usize, stop: usize) !*Self {
        if (stop < start) {
            return std.debug.panic("invalid slice bounds: start {}, stop {}", .{ start, stop });
        }
        return self.vtable.getSlice(self, allocator, start, stop);
    }

    pub fn getMasked(self: *const Self, allocator: std.mem.Allocator, mask: *const Self) !*Self {
        return self.vtable.getMasked(self, allocator, mask);
    }

    pub fn computeStatistic(self: *const Self, allocator: std.mem.Allocator, stat: enc.Stats.Stat) !enc.Scalar {
        if (self.stats.get(stat)) |value| {
            return value.clone(allocator);
        }

        const newValue = try self.vtable.computeStatistic(self, allocator, stat);

        self.stats.put(stat, newValue);
        return newValue.clone(allocator);
    }

    pub fn isPlain(self: *const Self) bool {
        return self.vtable.isPlain(self);
    }

    pub fn exportToArrow(self: *const Self, allocator: std.mem.Allocator) !arrow.Array {
        return self.vtable.exportToArrow(self, allocator);
    }

    pub fn iterPlain(self: *const Self, allocator: std.mem.Allocator) !Iterator {
        return self.vtable.iterPlain(self, allocator);
    }

    pub fn getElements(self: *const Self, allocator: std.mem.Allocator, indices: *const enc.PrimitiveArray) !*enc.Array {
        // TODO(robert): implement getElements for builtin arrays
        return if (self.vtable.getElements) |elemFn| elemFn(self, allocator, indices) else enc.Error.GetElementsNotSupported;
    }

    pub fn iterArrow(self: *const Self, allocator: std.mem.Allocator) !?enc.arrow.Iterator {
        const IterState = struct {
            plain_iter: Iterator,

            pub fn next(state: *@This(), gpa: std.mem.Allocator) !?arrow.Array {
                if (try state.plain_iter.next(gpa)) |chunk| {
                    defer chunk.release();

                    std.debug.assert(chunk.isPlain());
                    return try chunk.exportToArrow(gpa);
                }
                return null;
            }

            pub fn deinit(state: *@This()) void {
                state.plain_iter.deinit();
            }
        };

        return try enc.arrow.Iterator.WithState(IterState).alloc(allocator, .{
            .plain_iter = try self.iterPlain(allocator),
        });
    }

    pub fn Downcast(comptime ArrayPtr: type, comptime EncodingType: type) type {
        var ptr = @typeInfo(ArrayPtr).Pointer;
        if (ptr.child != enc.Array) {
            @compileError("Cannot downcast " ++ @typeName(ptr.child));
        }
        ptr.child = EncodingType;
        return @Type(.{ .Pointer = ptr });
    }
};

/// An encoding provides functions for interpreting the data and child arrays.
pub const VTable = struct {
    retain: *const fn (*const enc.Array) *enc.Array,
    release: *const fn (*enc.Array) void,
    asMutable: *const fn (*const enc.Array, std.mem.Allocator) anyerror!*enc.Array,
    getNBytes: *const fn (*const enc.Array) anyerror!usize,
    getScalar: *const fn (*const enc.Array, std.mem.Allocator, index: usize) anyerror!enc.Scalar,
    getSlice: *const fn (*const enc.Array, std.mem.Allocator, start: usize, stop: usize) anyerror!*enc.Array,
    getMasked: *const fn (*const enc.Array, std.mem.Allocator, mask: *const enc.Array) anyerror!*enc.Array,
    iterPlain: *const fn (*const enc.Array, std.mem.Allocator) anyerror!enc.Array.Iterator,
    isPlain: *const fn (*const enc.Array) bool,
    computeStatistic: *const fn (*const enc.Array, std.mem.Allocator, enc.Stats.Stat) anyerror!enc.Scalar,
    exportToArrow: *const fn (*const enc.Array, std.mem.Allocator) anyerror!arrow.Array,

    getElements: ?*const fn (*const enc.Array, std.mem.Allocator, *const enc.PrimitiveArray) anyerror!*enc.Array,

    /// Lift encoding functions from an Array subtype into functions that take *const Array as argument.
    /// This allows users to call the functions as member functions directly on the subtype, or on the Array
    /// parent type.
    pub fn Lift(comptime Subtype: anytype) VTable {
        const SubtypeEncoding = struct {
            pub fn retain(array: *const enc.Array) *enc.Array {
                return Subtype.retain(Subtype.from(array));
            }

            pub fn release(array: *enc.Array) void {
                Subtype.release(Subtype.from(array));
            }

            pub fn asMutable(array: *const enc.Array, allocator: std.mem.Allocator) !*enc.Array {
                if (@hasDecl(Subtype, "asMutable")) {
                    return Subtype.asMutable(Subtype.from(array), allocator);
                }
                return enc.Error.AsMutableNotSupported;
            }

            pub fn isPlain(array: *const enc.Array) bool {
                _ = array;
                return @hasDecl(Subtype, "exportToArrow");
            }

            pub fn getNBytes(array: *const enc.Array) !usize {
                return Subtype.getNBytes(Subtype.from(array));
            }

            pub fn getScalar(array: *const enc.Array, allocator: std.mem.Allocator, index: usize) !enc.Scalar {
                return Subtype.getScalar(Subtype.from(array), allocator, index);
            }

            pub fn getSlice(array: *const enc.Array, allocator: std.mem.Allocator, start: usize, stop: usize) !*enc.Array {
                return try Subtype.getSlice(Subtype.from(array), allocator, start, stop);
            }

            pub fn getMasked(array: *const enc.Array, allocator: std.mem.Allocator, mask: *const enc.Array) !*enc.Array {
                return try Subtype.getMasked(Subtype.from(array), allocator, mask);
            }

            pub fn iterPlain(array: *const enc.Array, allocator: std.mem.Allocator) !enc.Array.Iterator {
                return try Subtype.iterPlain(Subtype.from(array), allocator);
            }

            pub fn computeStatistic(array: *const enc.Array, allocator: std.mem.Allocator, stat: enc.Stats.Stat) !enc.Scalar {
                return try Subtype.computeStatistic(Subtype.from(array), allocator, stat);
            }

            pub fn getElements(array: *const enc.Array, allocator: std.mem.Allocator, indices: *const enc.PrimitiveArray) !*enc.Array {
                return try Subtype.getElements(Subtype.from(array), allocator, indices);
            }

            pub fn exportToArrow(array: *const enc.Array, allocator: std.mem.Allocator) !arrow.Array {
                if (@hasDecl(Subtype, "exportToArrow")) {
                    return try Subtype.exportToArrow(Subtype.from(array), allocator);
                } else {
                    return enc.Error.ArrowConversionFailed;
                }
            }
        };

        return VTable{
            .retain = &SubtypeEncoding.retain,
            .release = &SubtypeEncoding.release,
            .asMutable = &SubtypeEncoding.asMutable,
            .isPlain = &SubtypeEncoding.isPlain,
            .getNBytes = &SubtypeEncoding.getNBytes,
            .getScalar = &SubtypeEncoding.getScalar,
            .getSlice = &SubtypeEncoding.getSlice,
            .getMasked = &SubtypeEncoding.getMasked,
            .iterPlain = &SubtypeEncoding.iterPlain,
            .computeStatistic = &SubtypeEncoding.computeStatistic,
            .exportToArrow = &SubtypeEncoding.exportToArrow,
            .getElements = if (@hasDecl(Subtype, "getElements")) &SubtypeEncoding.getElements else null,
        };
    }
};
