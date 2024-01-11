const std = @import("std");
const arrow = @import("arrow");
const enc = @import("../enc.zig");
const rc = @import("../refcnt.zig");
const stats = @import("../stats/binary.zig");
const serde = @import("../serde.zig");

const Self = @This();

const RefCnt = rc.SingleThreadedRefCnt(Self, "refcnt");
pub usingnamespace RefCnt.Fns();

//TODO(robert): Change this to array once we support union arrays
views: *enc.Buffer,
data: []*enc.Array,
offset: usize,
refcnt: RefCnt = RefCnt.init(&destroy),
allocator: std.mem.Allocator,
array: enc.Array,

const vtable = enc.VTable.Lift(Encoding);

const InlineLen: u8 = 12;

const BinaryView = union(enum) {
    const ViewSelf = @This();
    inlined: InlinedBinary,
    ref: RefBinary,

    pub const InlinedBinary = extern struct {
        size: u32,
        data: [InlineLen]u8,
    };

    pub const RefBinary = extern struct {
        size: u32,
        prefix: [4]u8,
        bufferIndex: u32,
        offset: u32,
    };

    pub inline fn size(self: ViewSelf) u32 {
        return switch (self) {
            inline else => |v| v.size,
        };
    }
};

fn destroy(self: *Self) void {
    self.views.release();
    for (self.data) |arr| arr.release();
    self.allocator.free(self.data);

    self.array.deinit();
    self.allocator.destroy(self);
}

pub fn from(array: anytype) enc.Array.Downcast(@TypeOf(array), Self) {
    return @fieldParentPtr(Self, "array", array);
}

// TODO(robert): Need to split slices into 4GB chunks
pub fn allocWithCopy(gpa: std.mem.Allocator, slice: []const []const u8) !*Self {
    const views = try buildViews(gpa, slice);
    const bytesSlice = std.mem.sliceAsBytes(slice);
    const array = try enc.PrimitiveArray.allocWithOwnedBuffer(gpa, try enc.Buffer.allocWithCopy(gpa, bytesSlice), enc.PType.u8, 0, bytesSlice.len);
    const arrays = try gpa.dupe(*enc.Array, &.{&array.array});
    return allocWithOwnedArrays(gpa, views, arrays, 0, slice.len);
}

pub fn allocWithOwnedSlice(gpa: std.mem.Allocator, slice: []align(enc.Buffer.Alignment) const []const u8) !*Self {
    const views = try buildViews(gpa, slice);
    const bytesSlice = std.mem.sliceAsBytes(slice);
    const array = try enc.PrimitiveArray.allocWithOwnedBuffer(gpa, try enc.Buffer.allocWithOwnedSlice(gpa, bytesSlice), enc.PType.u8, 0, bytesSlice.len);
    const arrays = try gpa.dupe(*enc.Array, &.{&array.array});
    return allocWithOwnedArrays(gpa, views, arrays, 0, slice.len);
}

/// Allocate a primitive array with an owned buffer.
pub fn allocWithOwnedArrays(gpa: std.mem.Allocator, views: *enc.Buffer, data: []*enc.Array, offset: usize, len: ?usize) !*Self {
    const size = if (len) |l| l else views.bytes.len / @sizeOf(BinaryView);
    const self = try gpa.create(Self);
    self.* = .{
        .views = views,
        .data = data,
        .offset = offset,
        .allocator = gpa,
        .array = try enc.Array.init("enc.utf8", &vtable, gpa, enc.dtypes.utf8, size),
    };

    return self;
}

fn buildViews(gpa: std.mem.Allocator, slices: []const []const u8) !*enc.Buffer {
    const bufSlices = try gpa.alignedAlloc(BinaryView, enc.Buffer.Alignment, slices.len);
    var offset: u32 = 0;
    for (slices, bufSlices) |slice, *bufSlice| {
        if (slice.len <= InlineLen) {
            bufSlice.* = .{ .inlined = .{
                .size = @intCast(slice.len),
                .data = undefined,
            } };
            @memcpy(bufSlice.inlined.data[0..slice.len], slice);
        } else {
            bufSlice.* = .{ .ref = .{
                .size = @intCast(slice.len),
                .prefix = slice[0..4].*,
                .bufferIndex = 0,
                .offset = offset,
            } };
        }
        offset += @intCast(slice.len);
    }
    return enc.Buffer.allocWithOwnedSlice(gpa, std.mem.sliceAsBytes(bufSlices));
}

pub fn viewsSlice(self: *const Self) []const BinaryView {
    return std.mem.bytesAsSlice(BinaryView, self.views.bytes)[self.offset..][0..self.array.len];
}

pub fn plainSize(self: *const Self) usize {
    var totalSize: usize = 0;
    for (self.viewsSlice()) |view| totalSize += view.size();
    return totalSize;
}

/// Produce data arrays in plain encoding. Returns new references
fn plainDataArrays(self: *const Self, gpa: std.mem.Allocator) ![]*enc.PrimitiveArray {
    const decodedArrays = try gpa.alloc(*enc.PrimitiveArray, self.data.len);

    for (self.data, decodedArrays) |arr, *decoded| {
        const iter = try arr.iterPlain(gpa);
        defer iter.deinit();
        if (try iter.next(gpa)) |plainArr| {
            decoded.* = enc.PrimitiveArray.from(plainArr);
        }
    }

    return decodedArrays;
}

pub fn fromOffsetsAndData(gpa: std.mem.Allocator, offsets: *const enc.PrimitiveArray, data: *const enc.PrimitiveArray) !*Self {
    const viewSlice = try gpa.alignedAlloc(BinaryView, enc.Buffer.Alignment, offsets.array.len - 1);
    const dataSlice = data.asBytes();
    var offset: i32 = offsets.asSlice(i32)[0];
    for (offsets.asSlice(i32)[1..], viewSlice) |nextOffset, *view| {
        const size = nextOffset - offset;
        if (nextOffset - offset <= InlineLen) {
            view.* = .{ .inlined = .{
                .size = @intCast(size),
                .data = undefined,
            } };
            @memcpy(view.inlined.data[0..@intCast(size)], dataSlice[@intCast(offset)..][0..@intCast(size)]);
        } else {
            view.* = .{ .ref = .{
                .size = @intCast(size),
                .prefix = dataSlice[@intCast(offset)..][0..4].*,
                .bufferIndex = 0,
                .offset = @intCast(offset),
            } };
        }
        offset = nextOffset;
    }
    const viewBuf = try enc.Buffer.allocWithOwnedSlice(gpa, std.mem.sliceAsBytes(viewSlice));
    const dataArrays = try gpa.dupe(*enc.Array, &.{data.array.retain()});
    return Self.allocWithOwnedArrays(gpa, viewBuf, dataArrays, 0, viewSlice.len);
}

pub fn toPlainBinaryArray(self: *const Self, gpa: std.mem.Allocator) !struct { offsets: *enc.PrimitiveArray, data: *enc.PrimitiveArray } {
    const totalSize = self.plainSize();
    const decodedArrays = try self.plainDataArrays(gpa);
    defer {
        for (decodedArrays) |arr| arr.release();
        gpa.free(decodedArrays);
    }

    var offset: i32 = 0;
    var dataBuf = try enc.PrimitiveArray.allocEmpty(gpa, enc.PType.u8, totalSize);
    var offsets = try enc.PrimitiveArray.allocEmpty(gpa, enc.PType.i32, self.array.len + 1);
    var offsetsSlice = offsets.asMutableSlice(i32);
    var dataBytes = dataBuf.asMutableBytes();

    for (self.viewsSlice(), 0..) |view, i| {
        offsetsSlice[i] = offset;

        switch (view) {
            .inlined => |ib| @memcpy(dataBytes[@intCast(offset)..][0..ib.size], ib.data[0..ib.size]),
            .ref => |r| @memcpy(dataBytes[@intCast(offset)..][0..r.size], decodedArrays[r.bufferIndex].asBytes()[r.offset..][0..r.size]),
        }
        offset += @intCast(view.size());
    }
    offsetsSlice[self.array.len] = offset;
    return .{ .offsets = offsets, .data = dataBuf };
}

// TODO(robert): Implement buffer compaction
pub fn compact(self: *Self, gpa: std.mem.Allocator) !*Self {
    _ = gpa;
    return self;
}

// TOOD(robert): Prune buffers based on offset?
/// Simple implenentation that writes array bytes into the writer
pub fn toBytes(self: *const Self, writer: anytype) !void {
    try serde.writeByteSlice(std.mem.sliceAsBytes(self.viewsSlice()), writer);
    try std.leb.writeULEB128(writer, self.data.len);
    // TODO(robert): Promote toBytes to array interface
    for (self.data) |arr| {
        try enc.PrimitiveArray.from(arr).toBytes(writer);
    }
}

pub fn fromBytes(reader: anytype, allocator: std.mem.Allocator) !*Self {
    const views = try serde.readByteSliceAligned(reader, allocator);
    const viewBuf = try enc.Buffer.allocWithOwnedSlice(allocator, views);
    const bufCount = try std.leb.readULEB128(usize, reader);
    const arrays = try allocator.alloc(*enc.Array, bufCount);
    for (arrays) |*arr| {
        arr.* = &(try enc.PrimitiveArray.fromBytes(reader, allocator)).array;
    }

    const len = views.len / @sizeOf(BinaryView);
    return Self.allocWithOwnedArrays(allocator, viewBuf, arrays, 0, len);
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
        var size = self.views.bytes.len;
        for (self.data) |arr| size += try arr.getNBytes();

        return size;
    }

    pub fn getScalar(self: *const Self, gpa: std.mem.Allocator, index: usize) !enc.Scalar {
        const viewSlice = self.viewsSlice()[index];

        switch (viewSlice) {
            .inlined => |i| return enc.Scalar.initComplex(gpa, i.data),
            .ref => |r| {
                const slice = try self.data[r.bufferIndex].getSlice(gpa, r.offset, r.offset + r.size);
                const iter = try slice.iterPlain(gpa);
                defer iter.deinit();
                if (try iter.next(gpa)) |chunk| {
                    defer chunk.release();
                    return enc.Scalar.initComplex(gpa, enc.PrimitiveArray.from(chunk).asBytes());
                }
                return enc.Error.EmptyChunk;
            },
        }
    }

    pub fn getSlice(self: *const Self, gpa: std.mem.Allocator, start: usize, stop: usize) !*enc.Array {
        const newArrays = try gpa.alloc(*enc.Array, self.data.len);
        for (newArrays, self.data) |*newArr, arr| newArr.* = arr.retain();

        var sliced = try allocWithOwnedArrays(
            gpa,
            self.views.retain(),
            newArrays,
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

        const newViewBuf = try enc.Buffer.allocEmpty(gpa, newLength * @sizeOf(BinaryView));
        var newViews = std.mem.bytesAsSlice(BinaryView, newViewBuf.asMutable());
        const views = self.viewsSlice();
        var offset: usize = 0;
        for (views, 0..) |view, i| {
            if (maskBools.get(i) == 1) {
                newViews[offset] = view;
                offset += 1;
            }
        }

        const arrays = try gpa.alloc(*enc.Array, self.data.len);
        for (arrays, self.data) |*newArr, arr| newArr.* = arr.retain();

        const newArr = try Self.allocWithOwnedArrays(gpa, newViewBuf, arrays, 0, newLength);
        return &newArr.array;
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

    // TODO(robert): Just decode data arrays instead of converting to plain binary array
    pub fn exportToArrow(self: *const Self, gpa: std.mem.Allocator) !arrow.Array {
        const PrivateData = struct {
            arrays: []*enc.Array,
            allocator: std.mem.Allocator,

            pub fn deinit(data: *const @This()) void {
                for (data.arrays) |buffer| buffer.release();
                data.allocator.free(data.arrays);
            }
        };

        const plainArrays = try self.toPlainBinaryArray(gpa);
        const newArrays = try gpa.dupe(*enc.Array, &.{ &plainArrays.offsets.array, &plainArrays.data.array });

        return try arrow.ArrayExporter(PrivateData).exportToC(
            gpa,
            .{ .arrays = newArrays, .allocator = gpa },
            .{
                .length = self.array.len,
                .buffers = &.{ null, plainArrays.offsets.buffer.bytes, plainArrays.data.buffer.bytes },
                .offset = 0,
            },
        );
    }
};
