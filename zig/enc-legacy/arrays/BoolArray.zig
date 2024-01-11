const std = @import("std");
const arrow = @import("arrow");
const enc = @import("../enc.zig");
const rc = @import("../refcnt.zig");
const stats = @import("../stats/bool.zig");

const Self = @This();

pub const BitSlice = std.PackedIntSlice(u1);

const RefCnt = rc.SingleThreadedRefCnt(Self, "refcnt");
pub usingnamespace RefCnt.Fns();

buffer: *enc.Buffer,
offset: usize,
refcnt: RefCnt = RefCnt.init(&destroy),
allocator: std.mem.Allocator,
array: enc.Array,

const vtable = enc.VTable.Lift(Encoding);

fn destroy(self: *Self) void {
    self.buffer.release();
    self.array.deinit();
    self.allocator.destroy(self);
}

pub fn from(array: anytype) enc.Array.Downcast(@TypeOf(array), Self) {
    return @fieldParentPtr(Self, "array", array);
}

/// Allocate an uninitialized bool array of the requested length.
pub fn allocEmpty(gpa: std.mem.Allocator, len: usize) !*Self {
    const bytelen = try std.math.divCeil(usize, len, 8);
    return allocWithOwnedBuffer(gpa, try enc.Buffer.allocEmpty(gpa, bytelen), 0, len);
}

/// Allocate with a copy of the given bools
pub fn allocWithBools(gpa: std.mem.Allocator, bools: []const bool) !*Self {
    const self = try allocEmpty(gpa, bools.len);

    var slice = self.asSlice();
    for (bools, 0..) |b, i| {
        slice.set(i, if (b) 1 else 0);
    }

    return self;
}

/// Allocate a boolean array with an owned buffer.
pub fn allocWithOwnedBuffer(gpa: std.mem.Allocator, buffer: *enc.Buffer, offset: usize, len: usize) !*Self {
    // TODO(ngates): check bounds on buffer size?
    const self = try gpa.create(Self);
    self.* = .{
        .buffer = buffer,
        .offset = offset,
        .allocator = gpa,
        .array = try enc.Array.init("enc.bool", &vtable, gpa, enc.dtypes.bool_, len),
    };

    return self;
}

pub fn getOffset(self: *const Self) usize {
    return self.offset;
}

pub fn asSlice(self: *const Self) BitSlice {
    const byte_offset = self.offset / 8;
    const bit_offset: u3 = @truncate(self.offset);
    return .{
        .bytes = @constCast(self.buffer.bytes[byte_offset..]),
        .bit_offset = bit_offset,
        .len = self.array.len,
    };
}

/// Returns a slice representing the full bytes of this array.
/// Use in combination with `leadingByte` and `trailingByte`
pub fn fullBytesSlice(self: *const Self) []const u8 {
    _ = self;
    std.debug.panic("Not Implemented", .{});
}

/// Returns the leading byte of this array. Any bits occuring
/// before the offset are replaced by the given fill_value.
pub fn leadingByte(self: *const Self, fill_value: u1) u8 {
    _ = self;
    _ = fill_value;
    std.debug.panic("Not Implemented", .{});
}

/// Returns the trailing byte of this array. Any bits occuring
/// after the end are replaced by the given fill_value.
pub fn trailingByte(self: *const Self, fill_value: u1) u8 {
    _ = self;
    _ = fill_value;
    std.debug.panic("Not Implemented", .{});
}

/// Simple implenentation that writes array bytes into the writer
pub fn toBytes(self: *const Self, writer: anytype) !void {
    try std.leb.writeULEB128(writer, self.array.len);
    const slice = self.asSlice();

    if (slice.bit_offset == 0) {
        try writer.writeAll(slice.bytes);
    } else {
        const alignedBuffer = try self.allocator.alloc(u8, slice.bytes.len);
        const shift: u3 = @intCast(@as(u4, 8) - slice.bit_offset);
        defer self.allocator.free(alignedBuffer);
        for (0..alignedBuffer.len - 1) |i| {
            alignedBuffer[i] = slice.bytes[i] << slice.bit_offset | slice.bytes[i + 1] >> shift;
        }
        alignedBuffer[slice.bytes.len - 1] = slice.bytes[slice.bytes.len - 1] << slice.bit_offset;

        try writer.writeAll(alignedBuffer);
    }
}

pub fn fromBytes(reader: anytype, allocator: std.mem.Allocator) !*Self {
    const len = try std.leb.readULEB128(u64, reader);
    const byteLen = try std.math.divCeil(u64, len, 8);
    const bufferBytes = try allocator.alignedAlloc(u8, 128, byteLen);
    const readBytes = try reader.readAll(bufferBytes);
    if (byteLen != readBytes) {
        return error.EndOfStream;
    }
    const buf = try enc.Buffer.allocWithOwnedSlice(allocator, bufferBytes);
    return Self.allocWithOwnedBuffer(allocator, buf, 0, len);
}

//
// Encoding Functions
//
const Encoding = struct {
    pub fn from(array: anytype) enc.Array.Downcast(@TypeOf(array), Self) {
        return Self.from(array);
    }

    pub inline fn retain(self: *const Self) *enc.Array {
        return &self.retain().array;
    }

    pub inline fn release(self: *Self) void {
        self.release();
    }

    pub fn getNBytes(self: *const Self) !usize {
        return std.math.divCeil(usize, self.array.len, 8);
    }

    pub fn getScalar(self: *const Self, allocator: std.mem.Allocator, index: usize) !enc.Scalar {
        _ = allocator;
        return enc.BoolScalar.init(self.asSlice().get(index) == 1);
    }

    pub fn getSlice(self: *const Self, gpa: std.mem.Allocator, start: usize, stop: usize) !*enc.Array {
        const sliced = try allocWithOwnedBuffer(gpa, self.buffer.retain(), self.offset + start, stop - start);
        return &sliced.array;
    }

    pub fn getMasked(self: *const Self, allocator: std.mem.Allocator, mask: *const enc.Array) !*enc.Array {
        _ = allocator;
        _ = mask;
        _ = self;
        std.debug.panic("Boolean getMasked not supported", .{});
    }

    pub inline fn computeStatistic(self: *const Self, allocator: std.mem.Allocator, stat: enc.Stats.Stat) !enc.Scalar {
        return stats.compute(self, allocator, stat);
    }

    pub fn iterPlain(self: *const Self, allocator: std.mem.Allocator) !enc.Array.Iterator {
        return try enc.Array.Iterator.WithState(struct {
            const Iter = @This();

            self: *Self,
            exported: bool = false,

            pub fn next(state: *Iter, gpa: std.mem.Allocator) !?*enc.Array {
                _ = gpa;
                if (state.exported) {
                    return null;
                }
                state.exported = true;
                return state.self.array.retain();
            }

            pub fn deinit(state: *Iter) void {
                state.self.release();
            }
        }).alloc(allocator, .{ .self = self.retain() });
    }

    pub fn exportToArrow(self: *const Self, allocator: std.mem.Allocator) !arrow.Array {
        const PrivateData = struct {
            buffer: *enc.Buffer,

            pub fn deinit(data: *const @This()) void {
                data.buffer.release();
            }
        };

        const new_buffer = self.buffer.retain();
        return try arrow.ArrayExporter(PrivateData).exportToC(
            allocator,
            .{ .buffer = new_buffer },
            .{
                .length = self.array.len,
                .buffers = &.{ null, new_buffer.bytes },
                .offset = self.offset,
            },
        );
    }
};
