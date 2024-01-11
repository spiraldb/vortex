const std = @import("std");
const enc = @import("../enc.zig");
const rc = @import("../refcnt.zig");
const stats = @import("../stats/dictionary.zig");
const codecs = @import("codecs");

const Self = @This();

const RefCnt = rc.SingleThreadedRefCnt(Self, "refcnt");
pub usingnamespace RefCnt.Fns();

dictionary: *enc.Array,
codes: *enc.Array,
refcnt: RefCnt = RefCnt.init(&destroy),
allocator: std.mem.Allocator,
array: enc.Array,

const vtable = enc.VTable.Lift(Encoding);

fn destroy(self: *Self) void {
    self.dictionary.release();
    self.codes.release();
    self.array.deinit();
    self.allocator.destroy(self);
}

pub fn from(array: anytype) enc.Array.Downcast(@TypeOf(array), Self) {
    return @fieldParentPtr(Self, "array", array);
}

pub fn fromOwnedCodesAndDict(gpa: std.mem.Allocator, codes: *enc.Array, dictionary: *enc.Array) !*Self {
    const self = try gpa.create(Self);
    self.* = .{
        .dictionary = dictionary,
        .codes = codes,
        .allocator = gpa,
        .array = try enc.Array.init("enc.dictionary", &vtable, gpa, try dictionary.dtype.clone(gpa), codes.len),
    };
    return self;
}

pub fn encode(gpa: std.mem.Allocator, array: *const enc.Array) !*Self {
    var dictionary: *enc.Array = undefined;
    var codes: *enc.Array = undefined;

    if (array.kind) |k| {
        switch (k) {
            .primitive => {
                const primitiveArray = enc.PrimitiveArray.from(array);
                switch (primitiveArray.ptype) {
                    inline else => |p| {
                        const pT = p.astype();
                        const result = try codecs.Dictionary(pT, u64, enc.Buffer.Alignment).encode(gpa, primitiveArray.asSlice(pT));
                        dictionary = &(try enc.PrimitiveArray.allocWithOwnedSlice(gpa, pT, result.dictionary)).array;
                        codes = &(try enc.PrimitiveArray.allocWithOwnedSlice(gpa, u64, result.codes)).array;
                    },
                }
            },
            else => return enc.Error.UnsupportedTypeForDictionaryEncoding,
        }
    } else {
        return enc.Error.UnsupportedTypeForDictionaryEncoding;
    }

    const dictArray = try fromOwnedCodesAndDict(gpa, codes, dictionary);

    // Inherit stats of the array being encoded
    for (comptime std.enums.values(enc.Stats.Stat)) |stat| {
        if (array.stats.get(stat)) |statValue| {
            dictArray.array.stats.put(stat, try statValue.clone(gpa));
        }
    }

    return dictArray;
}

// TODO(robert): Implement dictionary compaction
pub fn compact(self: *Self, gpa: std.mem.Allocator) !*Self {
    _ = gpa;
    return self;
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
        return try self.dictionary.getNBytes() + try self.codes.getNBytes();
    }

    pub fn getScalar(self: *const Self, gpa: std.mem.Allocator, index: usize) !enc.Scalar {
        const codeScalar = try self.codes.getScalar(gpa, index);
        return try self.dictionary.getScalar(gpa, codeScalar.as(usize));
    }

    pub fn getSlice(self: *const Self, gpa: std.mem.Allocator, start: usize, stop: usize) !*enc.Array {
        return &(try fromOwnedCodesAndDict(gpa, try self.codes.getSlice(gpa, start, stop), self.dictionary.retain())).array;
    }

    pub fn getMasked(self: *const Self, gpa: std.mem.Allocator, mask: *const enc.Array) !*enc.Array {
        return &(try fromOwnedCodesAndDict(gpa, try self.codes.getMasked(gpa, mask), self.dictionary.retain())).array;
    }

    pub inline fn computeStatistic(self: *const Self, allocator: std.mem.Allocator, stat: enc.Stats.Stat) !enc.Scalar {
        return stats.compute(self, allocator, stat);
    }

    pub fn iterPlain(self: *const Self, allocator: std.mem.Allocator) !enc.Array.Iterator {
        return try enc.Array.Iterator.WithState(struct {
            const Iter = @This();

            self: *Self,
            exported: bool = false,
            ctx: enc.Ctx,

            pub fn next(state: *Iter, gpa: std.mem.Allocator) !?*enc.Array {
                if (state.exported) {
                    return null;
                }
                const res = try enc.ops.take(state.ctx, state.self.dictionary, state.self.codes);
                defer res.release();
                const iter = try res.iterPlain(gpa);
                defer iter.deinit();
                const plainArray = try iter.next(gpa);
                state.exported = true;
                return plainArray;
            }

            pub fn deinit(state: *Iter) void {
                state.self.release();
                state.ctx.deinit();
            }
        }).alloc(allocator, .{ .self = self.retain(), .ctx = try enc.Ctx.init(allocator) });
    }
};

test "dictionary array" {
    const arr = try enc.PrimitiveArray.allocWithCopy(std.testing.allocator, u32, &.{ 0, 1, 2, 8, 8, 3, 2, 9, 1, 8, 9 });
    defer arr.release();

    const dictArray = try encode(std.testing.allocator, &arr.array);
    defer dictArray.release();
    try std.testing.expectEqualSlices(u64, &.{ 0, 1, 2, 4, 4, 3, 2, 5, 1, 4, 5 }, enc.PrimitiveArray.from(dictArray.codes).asSlice(u64));
    try std.testing.expectEqualSlices(u32, &.{ 0, 1, 2, 3, 8, 9 }, enc.PrimitiveArray.from(dictArray.dictionary).asSlice(u32));
}

test "dictionary array stats" {
    const arr = try enc.PrimitiveArray.allocWithCopy(std.testing.allocator, u32, &.{ 0, 1, 2, 8, 8, 3, 2, 9, 1, 8, 9 });
    defer arr.release();
    for (comptime std.enums.values(enc.Stats.Stat)) |stat| {
        const sc: ?enc.Scalar = arr.array.computeStatistic(std.testing.allocator, stat) catch null;
        if (sc) |s| s.deinit();
    }

    const dictArray = try encode(std.testing.allocator, &arr.array);
    defer dictArray.release();
    for (comptime std.enums.values(enc.Stats.Stat)) |stat| {
        if (arr.array.stats.get(stat)) |origStat| {
            try std.testing.expect(origStat.equal(dictArray.array.stats.get(stat).?));
        }
    }
}
