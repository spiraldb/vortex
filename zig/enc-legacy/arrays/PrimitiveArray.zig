const std = @import("std");
const arrow = @import("arrow");
const enc = @import("../enc.zig");
const rc = @import("../refcnt.zig");
const stats = @import("../stats/primitive.zig");
const serde = @import("../serde.zig");

const Self = @This();

const RefCnt = rc.SingleThreadedRefCnt(Self, "refcnt");
pub usingnamespace RefCnt.Fns();

buffer: *enc.Buffer,
ptype: enc.PType,
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

/// Allocate an uninitialized primitive array of the requested length.
pub fn allocEmpty(gpa: std.mem.Allocator, ptype: enc.PType, len: usize) !*Self {
    return allocWithOwnedBuffer(
        gpa,
        try enc.Buffer.allocEmpty(gpa, ptype.sizeOf() * len),
        ptype,
        0,
        len,
    );
}

/// Create a primitive array from a copy of the given slice.
pub fn allocWithCopy(gpa: std.mem.Allocator, comptime T: type, slice: []const T) !*Self {
    const buffer = try enc.Buffer.allocWithCopy(gpa, std.mem.sliceAsBytes(slice));
    return allocWithOwnedBuffer(gpa, buffer, enc.PType.fromType(T), 0, slice.len);
}

/// Create a primitive array from a Zig slice.
pub fn allocWithOwnedSlice(gpa: std.mem.Allocator, comptime T: type, slice: []align(enc.Buffer.Alignment) const T) !*Self {
    const buffer = try enc.Buffer.allocWithOwnedSlice(gpa, std.mem.sliceAsBytes(slice));
    return allocWithOwnedBuffer(gpa, buffer, enc.PType.fromType(T), 0, slice.len);
}

/// Allocate a primitive array with an owned buffer.
pub fn allocWithOwnedBuffer(gpa: std.mem.Allocator, buffer: *enc.Buffer, ptype: enc.PType, offset: usize, len: usize) !*Self {
    // TODO(ngates): check bounds on buffer size?

    const self = try gpa.create(Self);
    self.* = .{
        .buffer = buffer,
        .ptype = ptype,
        .offset = offset,
        .allocator = gpa,
        .array = try enc.Array.init("enc.primitive", &vtable, gpa, enc.DType.fromPType(ptype), len),
    };

    return self;
}

// Note: we cannot guarantee the alignment when slicing with an offset
pub fn asBytes(self: *const Self) []const u8 {
    return self.buffer.bytes[self.ptype.sizeOf() * self.offset ..][0 .. self.ptype.sizeOf() * self.array.len];
}

pub fn asMutableBytes(self: *Self) []u8 {
    std.debug.assert(self.buffer.isMutable());
    return @constCast(self.asBytes());
}

pub fn asSlice(self: *const Self, comptime T: type) []const T {
    switch (self.ptype) {
        inline else => |p| {
            if (p.astype() != T) {
                std.debug.panic(
                    "PrimitiveArray of type {s} cannot be sliced as {s}",
                    .{ @typeName(p.astype()), @typeName(T) },
                );
            }
        },
    }
    const slice: []const T = @alignCast(std.mem.bytesAsSlice(T, self.buffer.bytes));
    return slice[self.offset .. self.offset + self.array.len];
}

pub fn asMutableSlice(self: *Self, comptime T: type) []T {
    std.debug.assert(self.buffer.isMutable());
    return @constCast(self.asSlice(T));
}

pub fn view(self: *const Self, gpa: std.mem.Allocator, new_ptype: enc.PType) !*Self {
    std.debug.assert(self.ptype.sizeOf() == new_ptype.sizeOf());
    return allocWithOwnedBuffer(gpa, self.buffer.retain(), new_ptype, self.offset, self.array.len);
}

/// Simple implenentation that writes array bytes into the writer
pub fn toBytes(self: *const Self, writer: anytype) !void {
    try writer.writeByte(@as(u8, @intFromEnum(self.ptype)));

    const byteOffset = self.offset * self.ptype.sizeOf();
    try serde.writeByteSlice(self.buffer.bytes[byteOffset..], writer);
}

pub fn fromBytes(reader: anytype, allocator: std.mem.Allocator) !*Self {
    const ptypeByte = try reader.readByte();
    const ptype = enc.PType.fromId(ptypeByte) orelse std.debug.panic("Couldn't construct PType from byte {}", .{ptypeByte});

    const bufferBytes = try serde.readByteSliceAligned(reader, allocator);
    const buf = try enc.Buffer.allocWithOwnedSlice(allocator, bufferBytes);

    return Self.allocWithOwnedBuffer(allocator, buf, ptype, 0, bufferBytes.len / ptype.sizeOf());
}

//
// Encoding Functions
//
const Encoding = struct {
    pub inline fn from(array: anytype) enc.Array.Downcast(@TypeOf(array), Self) {
        return Self.from(array);
    }

    pub inline fn retain(self: *const Self) *enc.Array {
        return &self.retain().array;
    }

    pub inline fn release(self: *Self) void {
        self.release();
    }

    pub fn getNBytes(self: *const Self) !usize {
        return self.array.len * self.ptype.sizeOf();
    }

    pub fn getScalar(self: *const Self, gpa: std.mem.Allocator, index: usize) !enc.Scalar {
        _ = gpa;
        return switch (self.ptype) {
            inline else => |p| enc.Scalar.init(self.asSlice(p.astype())[index]),
        };
    }

    pub fn getSlice(self: *const Self, gpa: std.mem.Allocator, start: usize, stop: usize) !*enc.Array {
        var sliced = try allocWithOwnedBuffer(
            gpa,
            self.buffer.retain(),
            self.ptype,
            self.offset + start,
            stop - start,
        );
        return &sliced.array;
    }

    pub fn getMasked(self: *const Self, gpa: std.mem.Allocator, mask: *const enc.Array) !*enc.Array {
        var maskIter = try mask.iterPlain(gpa);
        defer maskIter.deinit();

        const maskArray = try maskIter.next(gpa) orelse std.debug.panic("No chunks?", .{});
        defer maskArray.release();
        std.debug.assert(maskArray.kind == .bool);

        if (try maskIter.next(gpa)) |_| {
            std.debug.panic("Chunked arrays not yet supported. TOO MANY CHUNKS", .{});
        }

        const maskBools = enc.BoolArray.from(maskArray).asSlice();

        var newLength: usize = 0;
        for (0..maskBools.len) |i| {
            newLength += maskBools.get(i);
        }

        var newArray = try allocEmpty(gpa, self.ptype, newLength);
        switch (self.ptype) {
            inline else => |p| {
                const newSlice = newArray.asMutableSlice(p.astype());
                var offset: usize = 0;
                for (self.asSlice(p.astype()), 0..) |elem, i| {
                    if (maskBools.get(i) == 1) {
                        newSlice[offset] = elem;
                        offset += 1;
                    }
                }
            },
        }

        return &newArray.array;
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

    pub fn exportToArrow(self: *const Self, gpa: std.mem.Allocator) !arrow.Array {
        const PrivateData = struct {
            buffer: *enc.Buffer,

            pub fn deinit(data: *const @This()) void {
                data.buffer.release();
            }
        };

        const new_buffer = self.buffer.retain();
        return try arrow.ArrayExporter(PrivateData).exportToC(
            gpa,
            .{ .buffer = new_buffer },
            .{
                .length = self.array.len,
                .buffers = &.{ null, new_buffer.bytes },
                .offset = self.offset,
            },
        );
    }
};
