const std = @import("std");
const arrow = @import("arrow");
const encArrow = @import("../arrow.zig");
const enc = @import("../enc.zig");
const cloning = @import("../cloning.zig");
const rc = @import("../refcnt.zig");
const itertools = @import("../itertools.zig");

const Self = @This();

const RefCnt = rc.SingleThreadedRefCnt(Self, "refcnt");
pub usingnamespace RefCnt.Fns();

names: []const []const u8,
fields: []*enc.Array,
refcnt: RefCnt = RefCnt.init(&destroy),
allocator: std.mem.Allocator,
array: enc.Array,

const vtable = enc.VTable.Lift(Encoding);

fn destroy(self: *Self) void {
    for (self.names) |name| self.allocator.free(name);
    self.allocator.free(self.names);

    for (self.fields) |field| field.release();
    self.allocator.free(self.fields);

    self.array.deinit();
    self.allocator.destroy(self);
}

pub fn from(array: anytype) enc.Array.Downcast(@TypeOf(array), Self) {
    return @fieldParentPtr(Self, "array", array);
}

pub fn allocWithOwnedNamesAndFields(gpa: std.mem.Allocator, names: []const []const u8, fields: []*enc.Array) !*Self {
    // Ensure all fields report the same length
    // FIXME(ngates): improve the error here
    var len: ?usize = null;
    for (fields) |field| {
        if (len) |existing_len| {
            if (existing_len != field.len) {
                std.debug.panic("Struct fields have different lengths", .{});
            }
        } else {
            len = field.len;
        }
    }

    const new_fields = try gpa.alloc(enc.DType, fields.len);
    for (fields, 0..) |field, i| {
        new_fields[i] = try field.dtype.clone(gpa);
    }
    const dtype = .{
        .struct_ = .{
            .names = try cloning.cloneStrings(gpa, names),
            .fields = new_fields,
            .allocator = gpa,
        },
    };

    const self = try gpa.create(Self);
    self.* = .{
        .names = names,
        .fields = fields,
        .allocator = gpa,
        .array = try enc.Array.init("enc.struct", &vtable, gpa, dtype, len orelse 0),
    };
    return self;
}

pub fn findField(self: *const Self, name: []const u8) ?*const enc.Array {
    for (self.names, self.fields) |field_name, field| {
        if (std.mem.eql(u8, name, field_name)) {
            return field;
        }
    }
    return null;
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
        var nbytes: usize = 0;
        for (self.fields) |field| {
            nbytes += try field.getNBytes();
        }
        return nbytes;
    }

    pub fn getScalar(self: *const Self, gpa: std.mem.Allocator, index: usize) !enc.Scalar {
        const new_values = try gpa.alloc(enc.Scalar, self.fields.len);
        for (self.fields, 0..) |field, i| {
            new_values[i] = try field.getScalar(gpa, index);
        }
        return .{
            .struct_ = .{
                .names = try cloning.cloneStrings(gpa, self.names),
                .values = new_values,
                .allocator = gpa,
            },
        };
    }

    pub fn getSlice(self: *const Self, gpa: std.mem.Allocator, start: usize, stop: usize) !*enc.Array {
        const new_fields = try gpa.alloc(*enc.Array, self.fields.len);
        var len: ?usize = null;
        for (self.fields, 0..) |field, i| {
            new_fields[i] = try field.getSlice(gpa, start, stop);
            if (len) |existing_len| {
                if (existing_len != field.len) {
                    std.debug.panic("Struct fields have different lengths", .{});
                }
            } else {
                len = field.len;
            }
        }

        var slice = try allocWithOwnedNamesAndFields(gpa, try cloning.cloneStrings(gpa, self.names), new_fields);
        return &slice.array;
    }

    pub fn getMasked(self: *const Self, gpa: std.mem.Allocator, mask: *const enc.Array) !*enc.Array {
        const new_fields = try gpa.alloc(*enc.Array, self.fields.len);
        for (self.fields, 0..) |field, i| {
            new_fields[i] = try field.getMasked(gpa, mask);
        }
        var masked = try allocWithOwnedNamesAndFields(gpa, try cloning.cloneStrings(gpa, self.names), new_fields);
        return &masked.array;
    }

    pub inline fn computeStatistic(self: *const Self, allocator: std.mem.Allocator, stat: enc.Stats.Stat) !enc.Scalar {
        _ = stat;
        _ = allocator;
        _ = self;
        return enc.Error.StatisticNotSupported;
    }

    pub fn iterPlain(self: *const Self, allocator: std.mem.Allocator) !enc.Array.Iterator {
        return enc.Array.Iterator.WithState(struct {
            const Iter = @This();

            self: *Self,
            aligned_iter: itertools.AlignedIterator,

            pub fn next(iter: *Iter, gpa: std.mem.Allocator) !?*enc.Array {
                if (try iter.aligned_iter.next(gpa)) |aligned_fields| {
                    const struct_chunk = try allocWithOwnedNamesAndFields(
                        gpa,
                        try cloning.cloneStrings(gpa, iter.self.names),
                        aligned_fields.items,
                    );
                    return &struct_chunk.array;
                }
                return null;
            }

            pub fn deinit(iter: *Iter) void {
                iter.aligned_iter.deinit();
                iter.self.release();
            }
        }).alloc(
            allocator,
            .{
                .self = self.retain(),
                .aligned_iter = try itertools.alignedIterator(allocator, self.fields),
            },
        );
    }

    pub fn exportToArrow(self: *const Self, allocator: std.mem.Allocator) !arrow.Array {
        // TODO(ngates): I wonder if all arrays should just have this as their private data?
        const PrivateData = struct {
            self: *Self,

            pub fn deinit(data: *const @This()) void {
                data.self.release();
            }
        };

        const children = try allocator.alloc(arrow.Array, self.fields.len);
        defer allocator.free(children);

        for (self.fields, 0..) |field, i| {
            children[i] = try field.exportToArrow(allocator);
        }

        return try arrow.ArrayExporter(PrivateData).exportToC(
            allocator,
            .{ .self = self.retain() },
            .{
                .length = self.array.len,
                .buffers = &.{null},
                .children = children,
            },
        );
    }
};
