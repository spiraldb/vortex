const std = @import("std");
const Allocator = std.mem.Allocator;
const enc = @import("./enc.zig");
const serde = @import("serde.zig");

pub const BoolScalar = @import("./scalar/BoolScalar.zig");
pub const BinaryScalar = @import("./scalar/bytes.zig").BinaryScalar;
pub const UTF8Scalar = @import("./scalar/bytes.zig").UTF8Scalar;
pub const ExtensionScalar = @import("./scalar/ExtensionScalar.zig");
pub const LocalDateScalar = @import("./scalar/datetime.zig").LocalDateScalar;
pub const LocalTimeScalar = @import("./scalar/datetime.zig").LocalTimeScalar;
pub const InstantScalar = @import("./scalar/datetime.zig").InstantScalar;
pub const FloatScalar = @import("./scalar/float.zig").FloatScalar;
pub const IntScalar = @import("./scalar/int.zig").IntScalar;
pub const UIntScalar = @import("./scalar/int.zig").UIntScalar;
pub const ListScalar = @import("./scalar/ListScalar.zig");
pub const NullableScalar = @import("./scalar/nullable.zig").NullableScalar;
pub const NullScalar = @import("./scalar/NullScalar.zig");
pub const StructScalar = @import("./scalar/StructScalar.zig");

pub const Scalar = union(enc.dtypes.DTypeKind) {
    null: NullScalar,
    nullable: NullableScalar,
    bool: BoolScalar,
    int: IntScalar,
    uint: UIntScalar,
    float: FloatScalar,
    utf8: UTF8Scalar,
    binary: BinaryScalar,
    localtime: LocalTimeScalar,
    localdate: LocalDateScalar,
    instant: InstantScalar,
    struct_: StructScalar,
    list: ListScalar,
    extension: ExtensionScalar,

    pub fn init(value: anytype) Scalar {
        switch (@typeInfo(@TypeOf(value))) {
            .Null => return .{ .null = .{} },
            .ComptimeInt => return init(@as(i64, value)),
            .ComptimeFloat => return init(@as(f64, value)),
            .Bool => return BoolScalar.init(value),
            .Int => |i| return switch (i.signedness) {
                .signed => IntScalar.init(value),
                .unsigned => UIntScalar.init(value),
            },
            .Float => |f| switch (f.bits) {
                16 => return Scalar{ .float = .{ ._16 = value } },
                32 => return Scalar{ .float = .{ ._32 = value } },
                64 => return Scalar{ .float = .{ ._64 = value } },
                else => {},
            },
            else => {},
        }
        @compileError("Unsupported Scalar type " ++ @typeName(@TypeOf(value)));
    }

    pub fn initComplex(gpa: std.mem.Allocator, value: anytype) !Scalar {
        switch (@typeInfo(@TypeOf(value))) {
            .Null, .ComptimeInt, .ComptimeFloat, .Bool, .Int, .Float => return Scalar.init(value),
            .Array => |a| {
                if (a.child == u8) {
                    return UTF8Scalar.initOwned(try gpa.dupe(u8, &value), gpa);
                }
            },
            .Pointer => |p| {
                // []u8 results in UTF8Scalar?
                if (p.child == u8) {
                    return UTF8Scalar.initOwned(try gpa.dupe(u8, value), gpa);
                }
                if (p.size == .Slice or (p.size == .One and @typeInfo(p.child) == .Array)) {
                    // Note(ngates): requires a non-empty list
                    var dt: ?enc.DType = null;
                    const elements = try gpa.alloc(Scalar, value.len);
                    for (value, 0..) |v, i| {
                        elements[i] = try Scalar.initComplex(gpa, v);
                        if (dt == null) {
                            dt = try elements[i].getDType(gpa);
                        }
                    }
                    return .{ .list = .{ .dtype = dt orelse std.debug.panic("No elements?", .{}), .values = elements, .allocator = gpa } };
                }
            },
            .Optional => |o| {
                if (value == null) {
                    std.debug.panic("Null valued optionals are not supported", .{});
                } else {
                    const nullableScalar = try gpa.create(Scalar);
                    nullableScalar.* = try Scalar.initComplex(gpa, @as(o.child, value.?));
                    return Scalar{ .nullable = .{ .present = .{ .scalar = nullableScalar, .allocator = gpa } } };
                }
            },
            else => {},
        }
        @compileError("Unsupported Scalar type " ++ @typeName(@TypeOf(value)));
    }

    pub fn as(self: Scalar, comptime T: type) T {
        switch (self) {
            .bool => |b| {
                if (T == bool) return b.value;
                if (T == u1) return @intFromBool(b.value);
            },
            inline .int, .uint, .float => |s| {
                if (s.as(T)) |t| {
                    return t;
                } else |_| {}
            },
            else => {},
        }
        std.debug.panic("Cannot convert scalar {} into type {s}", .{ self, @typeName(T) });
    }

    /// Cast a scalar to the given dtype.
    /// Steals the given scalar.
    pub fn cast(self: Scalar, allocator: std.mem.Allocator, dtype: enc.DType) anyerror!enc.Scalar {
        return switch (self) {
            inline else => |s| s.cast(allocator, dtype),
        };
    }

    /// Return a new reference to the scalar's dtype.
    pub fn getDType(self: Scalar, allocator: std.mem.Allocator) anyerror!enc.DType {
        return switch (self) {
            .null => .null,
            .bool => .bool,
            .int => |i| enc.DType{ .int = i },
            .uint => |i| enc.DType{ .uint = i },
            .float => |f| enc.DType{ .float = f },
            .utf8 => enc.dtypes.utf8,
            .binary => enc.dtypes.binary,
            .localtime => |unit| enc.DType{ .localtime = unit },
            .localdate => .localdate,
            .instant => |unit| enc.DType{ .instant = unit },
            inline else => |s| s.getDType(allocator),
        };
    }

    pub fn isNull(self: Scalar) bool {
        return switch (self) {
            .null => true,
            .nullable => |n| n == .absent,
            else => false,
        };
    }

    pub fn nbytes(self: Scalar) usize {
        _ = self;
        // TODO(ngates): is this right?
        return @sizeOf(Scalar);
    }

    // Default implementation as invoked by Zig's formatters.
    pub fn format(value: Scalar, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = options;
        _ = fmt;
        switch (value) {
            .null => try writer.writeAll("null"),
            inline else => |s| try writer.print("{}", .{s}),
        }
    }

    pub fn clone(self: Scalar, allocator: std.mem.Allocator) anyerror!Scalar {
        return switch (self) {
            .null, .bool, .int, .uint, .float, .localdate, .localtime, .instant => self,
            inline else => |s, tag| @unionInit(enc.Scalar, @tagName(tag), try s.clone(allocator)),
        };
    }

    pub fn deinit(self: Scalar) void {
        switch (self) {
            .null, .bool, .int, .uint, .float, .localdate, .localtime, .instant => {},
            inline else => |s| s.deinit(),
        }
    }

    pub fn equal(self: Scalar, other: Scalar) bool {
        if (@intFromEnum(self) != @intFromEnum(other)) {
            return false;
        }
        switch (self) {
            // Override equality for any hierarchical types.
            .nullable => |n| switch (n) {
                inline .present => |s| return s.scalar.equal(other.nullable.present.scalar.*),
                inline .absent => |d| return d.equal(other.nullable.absent),
            },
            .utf8 => |u| return std.mem.eql(u8, u.bytes, other.utf8.bytes),
            .binary => |b| return std.mem.eql(u8, b.bytes, other.binary.bytes),
            .struct_ => |s| {
                const o = other.struct_;
                if (s.names.len != o.names.len) {
                    return false;
                }
                if (s.values.len != o.values.len) {
                    return false;
                }
                for (s.names, o.names) |sname, oname| {
                    if (!std.mem.eql(u8, sname, oname)) {
                        return false;
                    }
                }
                for (s.values, o.values) |svalue, ovalue| {
                    if (!svalue.equal(ovalue)) {
                        return false;
                    }
                }
                return true;
            },
            .list => |l| {
                const o = other.list;
                if (l.values.len != o.values.len) {
                    return false;
                }
                if (!l.dtype.equal(o.dtype)) {
                    return false;
                }
                for (l.values, o.values) |lv, ov| {
                    if (!lv.equal(ov)) {
                        return false;
                    }
                }
                return true;
            },
            .extension => |s| {
                const o = other.extension;
                if (!std.mem.eql(u8, s.id, o.id)) {
                    return false;
                }
                return s.vtable.equal(s.ptr, o.ptr);
            },
            else => return std.meta.eql(self, other),
        }
    }

    /// Serialize Scalar to bytes, all numbers are little endian
    pub fn toBytes(self: Scalar, writer: anytype) anyerror!void {
        try writer.writeByte(@intFromEnum(std.meta.activeTag(self)));
        switch (self) {
            .null => {},
            .extension => std.debug.panic("Extension scalars cannot currently be serialized to bytes", .{}),
            inline else => |s| try s.toBytes(writer),
        }
    }

    /// Construct Scalar from it's binary representation produced by toBytes
    pub fn fromBytes(reader: anytype, allocator: std.mem.Allocator) anyerror!Scalar {
        const typeTag: enc.dtypes.DTypeKind = @enumFromInt(try reader.readByte());
        return switch (typeTag) {
            .null => .{ .null = .{} },
            .extension => std.debug.panic("Extension scalars cannot be deserialized from bytes", .{}),
            inline else => |s| @unionInit(
                Scalar,
                @tagName(s),
                try std.meta.TagPayload(Scalar, s).fromBytes(reader, allocator),
            ),
        };
    }
};

const testing = std.testing;

test "scalar sizeof" {
    // FIXME(ngates): should we make ExtensionScalar a pointer?
    try testing.expectEqual(@as(usize, 112), @sizeOf(Scalar));
}

test "scalar cast" {
    const n64 = Scalar.init(1234);
    const nu32 = try n64.cast(std.testing.allocator, enc.dtypes.uint32);
    try std.testing.expectEqual(Scalar.init(@as(u32, 1234)), nu32);
}

test "scalar from bytes" {
    try testScalarSerde(Scalar.init(null), &.{0});
    try testScalarSerde(try Scalar.initComplex(testing.allocator, @as(?i32, 42)), &.{ 1, 0, 3, 3, 42 });
    try testScalarSerde(.{ .nullable = .{ .absent = enc.dtypes.int32 } }, &.{ 1, 1, 3, 3 });
    try testScalarSerde(Scalar.init(false), &.{ 2, 0 });
    try testScalarSerde(Scalar.init(true), &.{ 2, 1 });
    try testScalarSerde(Scalar.init(@as(i8, std.math.minInt(i8))), &.{ 3, 1, 128, 127 });
    try testScalarSerde(Scalar.init(@as(i16, std.math.minInt(i16))), &.{ 3, 2, 128, 128, 126 });
    try testScalarSerde(Scalar.init(@as(i32, std.math.minInt(i32))), &.{ 3, 3, 128, 128, 128, 128, 120 });
    try testScalarSerde(Scalar.init(@as(i64, std.math.minInt(i64))), &.{ 3, 4, 128, 128, 128, 128, 128, 128, 128, 128, 128, 127 });
    try testScalarSerde(.{ .int = .{ .Unknown = std.math.minInt(i64) } }, &.{ 3, 0, 128, 128, 128, 128, 128, 128, 128, 128, 128, 127 });
    try testScalarSerde(Scalar.init(@as(u8, std.math.maxInt(u8))), &.{ 4, 1, 255, 1 });
    try testScalarSerde(Scalar.init(@as(u16, std.math.maxInt(u16))), &.{ 4, 2, 255, 255, 3 });
    try testScalarSerde(Scalar.init(@as(u32, std.math.maxInt(u32))), &.{ 4, 3, 255, 255, 255, 255, 15 });
    try testScalarSerde(Scalar.init(@as(u64, std.math.maxInt(u64))), &.{ 4, 4, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1 });
    try testScalarSerde(.{ .uint = .{ .Unknown = std.math.maxInt(u64) } }, &.{ 4, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1 });
    try testScalarSerde(Scalar.init(@as(f16, std.math.floatTrueMin(f16))), &.{ 5, 1, 1 });
    try testScalarSerde(Scalar.init(@as(f32, std.math.floatMax(f32))), &.{ 5, 2, 255, 255, 255, 251, 7 });
    try testScalarSerde(Scalar.init(@as(f64, std.math.floatTrueMin(f64))), &.{ 5, 3, 1 });
    try testScalarSerde(.{ .float = .{ .Unknown = std.math.floatTrueMin(f64) } }, &.{ 5, 0, 1 });
    try testScalarSerde(.{ .utf8 = .{ .bytes = "HELLO WORLD", .allocator = null } }, &.{ 6, 11, 72, 69, 76, 76, 79, 32, 87, 79, 82, 76, 68 });
    try testScalarSerde(.{ .binary = .{ .bytes = "WORLD HELLO", .allocator = null } }, &.{ 7, 11, 87, 79, 82, 76, 68, 32, 72, 69, 76, 76, 79 });
    try testScalarSerde(.{ .localtime = .{ .ns = .{ ._64 = 1697469002 * 1000 * 1000 * 1000 } } }, &.{ 8, 0, 4, 128, 200, 201, 139, 133, 226, 167, 199, 23 });
    try testScalarSerde(.{ .localtime = .{ .us = .{ ._64 = 1697469002 * 1000 * 1000 } } }, &.{ 8, 1, 4, 128, 205, 134, 231, 236, 250, 129, 3 });
    try testScalarSerde(.{ .localtime = .{ .ms = .{ ._64 = 1697469002 * 1000 } } }, &.{ 8, 2, 4, 144, 226, 165, 200, 179, 49 });
    try testScalarSerde(.{ .localtime = .{ .s = .{ ._64 = 1697469002 } } }, &.{ 8, 3, 4, 202, 164, 181, 169, 6 });
    try testScalarSerde(.{ .localdate = .{ .days = .{ ._32 = 738808 } } }, &.{ 9, 3, 248, 139, 45 });
    try testScalarSerde(.{ .instant = .{ .ns = .{ ._64 = 1697469002 * 1000 * 1000 * 1000 } } }, &.{ 10, 0, 4, 128, 200, 201, 139, 133, 226, 167, 199, 23 });
    try testScalarSerde(.{ .instant = .{ .us = .{ ._64 = 1697469002 * 1000 * 1000 } } }, &.{ 10, 1, 4, 128, 205, 134, 231, 236, 250, 129, 3 });
    try testScalarSerde(.{ .instant = .{ .ms = .{ ._64 = 1697469002 * 1000 } } }, &.{ 10, 2, 4, 144, 226, 165, 200, 179, 49 });
    try testScalarSerde(.{ .instant = .{ .s = .{ ._64 = 1697469002 } } }, &.{ 10, 3, 4, 202, 164, 181, 169, 6 });
    try testScalarSerde(
        .{ .struct_ = .{
            .names = &.{
                "field",
                "secondField",
                "third",
            },
            .values = &.{
                enc.Scalar.init(false),
                .{ .utf8 = .{ .bytes = "word", .allocator = null } },
                .{ .int = .{ ._64 = 42 } },
            },
        } },
        &.{ 11, 3, 5, 102, 105, 101, 108, 100, 11, 115, 101, 99, 111, 110, 100, 70, 105, 101, 108, 100, 5, 116, 104, 105, 114, 100, 2, 0, 6, 4, 119, 111, 114, 100, 3, 4, 42 },
    );
    try testScalarSerde(
        try Scalar.initComplex(testing.allocator, &[_]u64{ 42, 42 }),
        &.{ 12, 2, 4, 4, 42, 4, 4, 42, 4, 4 },
    );
}

fn testScalarSerde(actual: Scalar, expected: []const u8) !void {
    var buf: [256]u8 = .{0} ** 256;
    var bytesStream = std.io.fixedBufferStream(&buf);
    try actual.toBytes(bytesStream.writer());
    const writtenScalar = bytesStream.getWritten();
    try testing.expectEqualSlices(u8, expected, writtenScalar);

    var persistedStream = std.io.fixedBufferStream(expected);
    const persistedScalar = try Scalar.fromBytes(persistedStream.reader(), testing.allocator);
    defer persistedScalar.deinit();

    testing.expect(actual.equal(persistedScalar)) catch |err| {
        std.debug.print("Bytes {any} did not deserialize to '{}'. Expected {any}\n", .{ expected, actual, writtenScalar });
        return err;
    };

    actual.deinit();
}
