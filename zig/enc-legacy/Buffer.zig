//! An Enc buffer encapsulates a slice of raw bytes.
//!
//! Buffers are reference counted and similarly are considered immutable when
//! refcnt > 1.
const std = @import("std");
const rc = @import("./refcnt.zig");
const PType = @import("./ptype.zig").PType;
const Scalar = @import("./scalar.zig").Scalar;

const Self = @This();

// Apache Arrow is only 64 byte, but fastlanes uses 1024 bit buffers.
// We could move to 512 bit buffers for now to be zero-copy with Arrow?
pub const Alignment = 128;

const RefCnt = rc.SingleThreadedRefCnt(Self, "refcnt");
pub usingnamespace RefCnt.Fns();

refcnt: RefCnt = RefCnt.init(&destroy),
gpa: std.mem.Allocator,

bytes: []align(Alignment) const u8,
is_mutable: bool,
ptr: ?*anyopaque,
deinit: *const fn (*Self) void,

fn destroy(self: *Self) void {
    self.deinit(self);
    self.gpa.destroy(self);
}

/// Allocate a new buffer with enough space for length * PType elements.
pub fn allocEmpty(gpa: std.mem.Allocator, length: usize) !*Self {
    const bytes = try gpa.alignedAlloc(u8, Alignment, length);
    return allocWithOwnedSlice(gpa, bytes);
}

/// Copy a slice into a new buffer.
pub fn allocWithCopy(gpa: std.mem.Allocator, slice: []const u8) !*Self {
    const copy = try gpa.alignedAlloc(u8, Alignment, slice.len);
    @memcpy(copy, slice);
    return allocWithOwnedSlice(gpa, copy);
}

/// Initialize a buffer with an existing Zig slice.
pub fn allocWithOwnedSlice(gpa: std.mem.Allocator, bytes: []align(Alignment) const u8) !*Self {
    const Closure = struct {
        fn deinit(buffer: *Self) void {
            buffer.gpa.free(buffer.bytes);
        }
    };

    const self = try gpa.create(Self);
    self.* = .{
        .bytes = bytes,
        .is_mutable = true,
        .ptr = null,
        .deinit = &Closure.deinit,
        .gpa = gpa,
    };
    return self;
}

pub fn isMutable(self: *const Self) bool {
    return self.is_mutable and self.refcnt.isExclusive();
}

pub fn asMutable(self: *const Self) []align(Alignment) u8 {
    if (self.isMutable()) {
        return @constCast(self.bytes);
    }
    std.debug.panic("Buffer is not mutable", .{});
}
