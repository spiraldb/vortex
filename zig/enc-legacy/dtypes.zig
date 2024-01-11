const std = @import("std");
const arrow = @import("arrow");
const enc = @import("./enc.zig");
const PType = enc.PType;
const serde = @import("serde.zig");

pub const DTypeKind = @typeInfo(DType).Union.tag_type.?;

/// Shorthand constructors for DTypes
pub const null_: DType = .null;
pub const bool_: DType = .bool;
pub const int: DType = .{ .int = .Unknown };
pub const int8: DType = .{ .int = ._8 };
pub const int16: DType = .{ .int = ._16 };
pub const int32: DType = .{ .int = ._32 };
pub const int64: DType = .{ .int = ._64 };
pub const uint: DType = .{ .uint = .Unknown };
pub const uint8: DType = .{ .uint = ._8 };
pub const uint16: DType = .{ .uint = ._16 };
pub const uint32: DType = .{ .uint = ._32 };
pub const uint64: DType = .{ .uint = ._64 };
pub const float: DType = .{ .float = .Unknown };
pub const float16: DType = .{ .float = ._16 };
pub const float32: DType = .{ .float = ._32 };
pub const float64: DType = .{ .float = ._64 };
pub const utf8: DType = .utf8;
pub const binary: DType = .binary;
pub const localdate: DType = .localdate;

pub fn nullable(dtype: enc.DType) DType {
    return .{ .nullable = .{ .child = &dtype, .allocator = null } };
}

pub fn localtime(unit: DType.TimeUnit) DType {
    return .{ .localtime = unit };
}

pub fn instant(unit: DType.TimeUnit) DType {
    return .{ .instant = unit };
}

pub fn struct_(names: []const []const u8, fields: []const DType, allocator: ?std.mem.Allocator) DType {
    return .{ .struct_ = .{ .names = names, .fields = fields, .allocator = allocator } };
}

pub fn list(child: *const DType, allocator: ?std.mem.Allocator) DType {
    return .{ .list = .{ .child = child, .allocator = allocator } };
}

pub fn intForRange(min: i65, max: i65) error{Overflow}!enc.DType {
    // Note(ngates): we use 65 bit ints to ensure we never overflow. This may end up being
    // quite expensive though since CPUs may not have 128 bit instructions...
    // This shouldn't be on the hot path though.
    std.debug.assert(min <= max);

    var width: u9 = if (max == 0) 0 else std.math.log2_int_ceil(u65, @abs(max) + 1);
    if (min < 0) {
        // Add 1 for the sign bit
        width += 1;
        width = @max(width, std.math.log2_int_ceil(u65, @abs(min)) + 1);
    }

    if (width > 64) {
        // Cannot fit into our biggest ints
        return error.Overflow;
    }

    // Round to the nearest power of two (minimum of 8 bits though)
    width = try std.math.ceilPowerOfTwo(u9, @max(8, width));

    const intWidth = enc.DType.IntWidth.fromInt(@intCast(width)) orelse unreachable;
    return if (min < 0) .{ .int = intWidth } else .{ .uint = intWidth };
}

test "intForRange" {
    try std.testing.expectEqual(uint8, try intForRange(0, 1));
    try std.testing.expectEqual(int8, try intForRange(-1, 1));
    try std.testing.expectEqual(int8, try intForRange(-120, 120));
    try std.testing.expectEqual(int8, try intForRange(-128, 127));

    try std.testing.expectEqual(uint8, try intForRange(0, 127));
    try std.testing.expectEqual(uint8, try intForRange(0, 255));
    try std.testing.expectEqual(uint16, try intForRange(0, 256));

    try std.testing.expectEqual(int64, try intForRange(std.math.minInt(i64), std.math.maxInt(i64)));

    try std.testing.expectError(error.Overflow, intForRange(std.math.minInt(i64), std.math.maxInt(i64) + 1));
}

pub fn widestFloat(left: enc.DType, right: enc.DType) enc.DType {
    if (left == .float and right != .float) {
        return left;
    }
    if (right == .float and left != .float) {
        return right;
    }
    return .{ .float = enc.DType.FloatWidth.fromInt(@max(left.float.asInt(), right.float.asInt())) orelse unreachable };
}

pub const DType = union(enum) {
    // Always null
    null: void,

    // Wraps a DType as nullable
    nullable: Nullable,

    // Booleans
    bool: void,

    // Integers are logically defined as signed or unsigned.
    // Integer widths are optional, with physical layout kept as an implementation detail.
    int: IntWidth,
    uint: IntWidth,

    float: FloatWidth,

    // Decimals are exact representations of base10 integers.
    // They are always signed.
    // decimal: void,

    // Represents utf-8 encoded bytes.
    utf8: void,

    // Represents opaque bytes.
    binary: void,

    // Since we have structural encodings and logical dtypes, we can do slightly better
    // than Arrow's physical type system.
    // We base our types on JSR310 which is generally agreed to be the correct abstractions.
    // See https://news.ycombinator.com/item?id=26283566.

    // Duration since midnight.
    localtime: TimeUnit,

    // Calendar day as measured since Unix epoch
    localdate: void,

    // Duration since Unix epoch
    instant: TimeUnit,

    // TODO(ngates): add a ZonedDateTime array
    // zoneddatetime:

    struct_: Struct,

    list: List,
    // Tuple, (fixed-length list? Maybe our encoding handles this nicely with constant delta offsets.)

    extension: Extension,

    pub const Nullable = struct {
        child: *const DType,
        allocator: ?std.mem.Allocator,
    };

    pub const IntWidth = enum {
        Unknown,
        _8,
        _16,
        _32,
        _64,

        pub fn format(value: IntWidth, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
            _ = options;
            _ = fmt;
            try writer.print("{}", .{value.asInt()});
        }

        pub fn fromInt(i: u8) ?IntWidth {
            return switch (i) {
                8 => ._8,
                16 => ._16,
                32 => ._32,
                64 => ._64,
                else => null,
            };
        }

        pub fn asInt(self: IntWidth) u8 {
            return switch (self) {
                .Unknown => 64,
                ._8 => 8,
                ._16 => 16,
                ._32 => 32,
                ._64 => 64,
            };
        }

        pub fn asType(comptime self: IntWidth, comptime signedness: std.builtin.Signedness) type {
            return std.meta.Int(signedness, self.asInt());
        }
    };

    pub const FloatWidth = enum {
        Unknown,
        _16,
        _32,
        _64,

        pub fn format(value: FloatWidth, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
            _ = options;
            _ = fmt;
            try writer.print("{}", .{value.asInt()});
        }

        pub fn fromInt(i: u8) ?FloatWidth {
            return switch (i) {
                16 => ._16,
                32 => ._32,
                64 => ._64,
                else => null,
            };
        }

        pub fn asInt(self: FloatWidth) u8 {
            return switch (self) {
                .Unknown => 64,
                ._16 => 16,
                ._32 => 32,
                ._64 => 64,
            };
        }

        pub fn asType(comptime self: FloatWidth) type {
            return switch (self) {
                .Unknown => f64,
                ._16 => f16,
                ._32 => f32,
                ._64 => f64,
            };
        }
    };

    pub const TimeUnit = enum {
        ns,
        us,
        ms,
        s,

        pub fn asString(self: TimeUnit) [:0]const u8 {
            return @tagName(self);
        }

        pub fn fromString(str: []const u8) ?TimeUnit {
            if (std.mem.eql(u8, str, "ns")) {
                return .ns;
            } else if (std.mem.eql(u8, str, "us")) {
                return .us;
            } else if (std.mem.eql(u8, str, "ms")) {
                return .ms;
            } else if (std.mem.eql(u8, str, "s")) {
                return .s;
            }
            return null;
        }

        pub fn format(value: TimeUnit, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
            _ = options;
            _ = fmt;
            try writer.writeAll(@tagName(value));
        }
    };

    pub const Struct = struct {
        names: []const []const u8,
        fields: []const DType,
        allocator: ?std.mem.Allocator,
    };

    pub const List = struct {
        child: *const DType,
        allocator: ?std.mem.Allocator,
    };

    /// An extension dtype that delegates its implementation to a vtable.
    pub const Extension = struct {
        id: []const u8,
        ptr: *anyopaque,
        vtable: *const VTable,

        pub const VTable = struct {
            clone: *const fn (*anyopaque) Extension,
            deinit: *const fn (*anyopaque) void,
            equal: *const fn (*anyopaque, other: *anyopaque) bool,
        };
    };

    pub fn toNullable(self: DType, allocator: std.mem.Allocator) !DType {
        // Shortcut already nullable types
        if (self == .nullable) {
            return self.clone(allocator);
        }

        const child = try allocator.create(enc.DType);
        child.* = try self.clone(allocator);
        return .{ .nullable = .{ .child = child, .allocator = allocator } };
    }

    /// Attempt to create a DType from a Zig type.
    pub fn fromZigType(comptime T: type) ?DType {
        return switch (@typeInfo(T)) {
            .Int, .Float => |_| fromPType(enc.PType.fromType(T)),
            .Bool => |_| bool_,
            .Pointer => |p| blk: {
                if (p.child == @typeInfo(u8)) {
                    break :blk binary;
                }
                break :blk null;
            },
            else => null,
        };
    }

    /// Create a DType from a PType.
    pub fn fromPType(ptype: enc.PType) DType {
        return switch (ptype) {
            inline .i8, .i16, .i32, .i64 => |p| @field(enc.dtypes, "int" ++ std.fmt.comptimePrint("{d}", .{@bitSizeOf(p.astype())})),
            inline .u8, .u16, .u32, .u64 => |p| @field(enc.dtypes, "uint" ++ std.fmt.comptimePrint("{d}", .{@bitSizeOf(p.astype())})),
            inline .f16, .f32, .f64 => |f| @field(enc.dtypes, "float" ++ std.fmt.comptimePrint("{d}", .{@bitSizeOf(f.astype())})),
        };
    }

    pub fn toPType(self: DType) ?enc.PType {
        return switch (self) {
            .int => |i| switch (i) {
                .Unknown => .i64,
                ._8 => .i8,
                ._16 => .i16,
                ._32 => .i32,
                ._64 => .i64,
            },
            .uint => |i| switch (i) {
                .Unknown => .u64,
                ._8 => .u8,
                ._16 => .u16,
                ._32 => .u32,
                ._64 => .u64,
            },
            .float => |f| switch (f) {
                .Unknown => .f64,
                ._16 => .f16,
                ._32 => .f32,
                ._64 => .f64,
            },
            else => null,
        };
    }

    // Default implementation as invoked by Zig's formatters.
    pub fn format(value: DType, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) @TypeOf(writer).Error!void {
        switch (value) {
            .bool => try writer.writeAll("bool"),
            .null => try writer.writeAll("null"),
            .nullable => |n| {
                try writer.writeAll("?");
                try n.child.format("{}", options, writer);
            },
            .int => |i| {
                if (i == .Unknown) {
                    try writer.writeAll("int");
                } else {
                    try writer.print("int({})", .{i.asInt()});
                }
            },
            .uint => |i| {
                try writer.writeAll("u");
                try format(.{ .int = i }, fmt, options, writer);
            },
            .float => |f| {
                if (f == .Unknown) {
                    try writer.writeAll("float");
                } else {
                    try writer.print("float({})", .{f.asInt()});
                }
            },
            .localdate => try writer.writeAll("localdate"),
            .localtime => |unit| try writer.print("localtime({})", .{unit}),
            .instant => |unit| try writer.print("instant({})", .{unit}),
            .struct_ => |s| {
                try writer.writeAll("{ ");
                for (s.names, s.fields, 0..) |name, field, i| {
                    try writer.print("{s}: {}", .{ name, field });
                    if (i < s.fields.len - 1) {
                        try writer.writeAll(", ");
                    }
                }
                try writer.writeAll(" }");
            },
            .list => |l| try writer.print("list({})", .{l.child.*}),
            .utf8 => try writer.writeAll("utf8"),
            .binary => try writer.writeAll("binary"),
            .extension => |ext| try writer.print("ext:{s}", .{ext.id}),
        }
    }

    pub fn hash(self: DType, hasher: anytype) void {
        _ = hasher;
        _ = self;
        // // Always include the enum key
        // std.hash.autoHash(hasher, self);
        // switch (self) {
        //     .bool => |b| std.hash.autoHash(hasher, b),
        //     .null => {},
        //     .nullable => |n| n.child.hash(hasher),
        //     .int => |i| std.hash.autoHash(hasher, i),
        //     .uint => |i| std.hash.autoHash(hasher, i),
        //     .float => |f| std.hash.autoHash(hasher, f),
        //     .localdate => {},
        //     .localtime => |unit| std.hash.autoHash(hasher, unit),
        //     .instant => |unit| std.hash.autoHash(hasher, unit),
        //     .struct_ => |s| {
        //         hasher.hash(.{9});
        //         for (s.names, s.fields) |name, field| {
        //             std.hash.autoHashStrat(hasher, name, .Deep);
        //             field.hash(hasher);
        //         }
        //     },
        //     .utf8 => |s| hasher.update(std.mem.asBytes(s)),
        //     .binary => |b| hasher.update(std.mem.asBytes(b)),
        //     .extension => |ext| {
        //         hasher.update(std.mem.asBytes(ext.id));
        //         if (ext.metadata) |m| hasher.update(std.mem.asBytes(m));
        //         ext.storage_dtype.hash(hasher);
        //     },
        // }
    }

    pub fn equal(self: DType, other: DType) bool {
        if (@intFromEnum(self) != @intFromEnum(other)) {
            return false;
        }
        switch (self) {
            // Override equality for any hierarchical types.
            .nullable => |n| return n.child.equal(other.nullable.child.*),
            .struct_ => |s| {
                const o = other.struct_;
                if (s.names.len != o.names.len) {
                    return false;
                }
                if (s.fields.len != o.fields.len) {
                    return false;
                }
                for (s.names, o.names) |sname, oname| {
                    if (!std.mem.eql(u8, sname, oname)) {
                        return false;
                    }
                }
                for (s.fields, o.fields) |sfield, ofield| {
                    if (!sfield.equal(ofield)) {
                        return false;
                    }
                }
                return true;
            },
            .list => |l| return l.child.equal(other.list.child.*),
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

    pub fn clone(self: DType, allocator: std.mem.Allocator) !DType {
        return switch (self) {
            .nullable => |n| blk: {
                const new_dtype = try allocator.create(DType);
                new_dtype.* = try n.child.clone(allocator);
                break :blk .{ .nullable = .{ .child = new_dtype, .allocator = allocator } };
            },
            .struct_ => |s| blk: {
                const new_names = try allocator.alloc([]const u8, s.names.len);
                for (s.names, new_names) |name, *new_name| {
                    new_name.* = try allocator.dupe(u8, name);
                }

                const new_fields = try allocator.alloc(DType, s.fields.len);
                for (s.fields, new_fields) |f, *new_f| {
                    new_f.* = try f.clone(allocator);
                }

                break :blk .{
                    .struct_ = .{
                        .names = new_names,
                        .fields = new_fields,
                        .allocator = allocator,
                    },
                };
            },
            .list => |l| blk: {
                const new_child = try allocator.create(DType);
                new_child.* = try l.child.clone(allocator);
                break :blk .{ .list = .{ .child = new_child, .allocator = allocator } };
            },
            .extension => |ext| .{ .extension = ext.vtable.clone(ext.ptr) },
            else => self,
        };
    }

    pub fn deinit(self: DType) void {
        switch (self) {
            .null, .bool, .int, .uint, .float, .utf8, .binary, .localtime, .localdate, .instant => {},
            .nullable => |n| {
                n.child.deinit();
                if (n.allocator) |ally| ally.destroy(n.child);
            },
            .struct_ => |s| {
                for (s.fields) |field| {
                    field.deinit();
                }
                if (s.allocator) |ally| {
                    for (s.names) |name| ally.free(name);
                    ally.free(s.names);
                    ally.free(s.fields);
                }
            },
            .list => |l| {
                l.child.deinit();
                if (l.allocator) |ally| ally.destroy(l.child);
            },
            .extension => |ext| ext.vtable.deinit(ext.ptr),
        }
    }

    pub fn isNumeric(self: *const DType) bool {
        return switch (self.*) {
            .int, .uint, .float => true,
            else => false,
        };
    }

    /// Serialize DType to bytes, all numbers are little endian
    pub fn toBytes(self: DType, writer: anytype) !void {
        try writer.writeByte(@intFromEnum(std.meta.activeTag(self)));

        switch (self) {
            .null, .bool, .utf8, .binary, .localdate => {},
            .nullable => |n| try n.child.toBytes(writer),
            .int => |width| try writer.writeByte(@intFromEnum(width)),
            .uint => |width| try writer.writeByte(@intFromEnum(width)),
            .float => |width| try writer.writeByte(@intFromEnum(width)),
            .localtime => |unit| try writer.writeByte(@intFromEnum(unit)),
            .instant => |unit| try writer.writeByte(@intFromEnum(unit)),
            .struct_ => |s| {
                try std.leb.writeULEB128(writer, s.names.len);
                for (s.names) |name| {
                    try serde.writeByteSlice(name, writer);
                }
                for (s.fields) |field| {
                    try field.toBytes(writer);
                }
            },
            .list => |l| try l.child.toBytes(writer),
            .extension => std.debug.panic("Extension DTypes cannot be serialized to bytes", .{}),
        }
    }

    /// Construct DType from it's binary representation produced by toBytes
    pub fn fromBytes(reader: anytype, allocator: std.mem.Allocator) !DType {
        const dtypeTag: DTypeKind = @enumFromInt(try reader.readByte());
        return switch (dtypeTag) {
            inline .null, .bool, .utf8, .binary, .localdate => |t| @unionInit(DType, @tagName(t), {}),
            .nullable => blk: {
                const new_dtype = try allocator.create(DType);
                new_dtype.* = try DType.fromBytes(reader, allocator);
                break :blk .{
                    .nullable = .{
                        .child = new_dtype,
                        .allocator = allocator,
                    },
                };
            },
            .int => .{ .int = @enumFromInt(try reader.readByte()) },
            .uint => .{ .uint = @enumFromInt(try reader.readByte()) },
            .float => .{ .float = @enumFromInt(try reader.readByte()) },
            .localtime => .{ .localtime = @enumFromInt(try reader.readByte()) },
            .instant => .{ .instant = @enumFromInt(try reader.readByte()) },
            .struct_ => blk: {
                const numFields = try std.leb.readULEB128(u64, reader);
                const names = try allocator.alloc([]const u8, numFields);

                for (names) |*name| {
                    name.* = try serde.readByteSlice(reader, allocator);
                }

                const fields = try allocator.alloc(DType, numFields);
                for (fields) |*field| {
                    field.* = try DType.fromBytes(reader, allocator);
                }

                break :blk .{
                    .struct_ = .{
                        .names = names,
                        .fields = fields,
                        .allocator = allocator,
                    },
                };
            },
            .list => blk: {
                const new_dtype = try allocator.create(DType);
                new_dtype.* = try DType.fromBytes(reader, allocator);
                break :blk .{ .list = .{ .child = new_dtype, .allocator = allocator } };
            },
            .extension => std.debug.panic("Extension DTypes cannot be deserialized from bytes", .{}),
        };
    }
};

test "dtype struct size" {
    try std.testing.expectEqual(@as(usize, 64), @sizeOf(DType));
}

test "dtype roundtrip" {
    try testDTypeSerde(null_, &.{0});
    try testDTypeSerde(try null_.toNullable(std.testing.allocator), &.{ 1, 0 });
    try testDTypeSerde(try int.toNullable(std.testing.allocator), &.{ 1, 3, 0 });
    try testDTypeSerde(bool_, &.{2});
    try testDTypeSerde(int8, &.{ 3, 1 });
    try testDTypeSerde(int16, &.{ 3, 2 });
    try testDTypeSerde(int32, &.{ 3, 3 });
    try testDTypeSerde(int64, &.{ 3, 4 });
    try testDTypeSerde(int, &.{ 3, 0 });
    try testDTypeSerde(uint8, &.{ 4, 1 });
    try testDTypeSerde(uint16, &.{ 4, 2 });
    try testDTypeSerde(uint32, &.{ 4, 3 });
    try testDTypeSerde(uint64, &.{ 4, 4 });
    try testDTypeSerde(uint, &.{ 4, 0 });
    try testDTypeSerde(float16, &.{ 5, 1 });
    try testDTypeSerde(float32, &.{ 5, 2 });
    try testDTypeSerde(float64, &.{ 5, 3 });
    try testDTypeSerde(float, &.{ 5, 0 });
    try testDTypeSerde(utf8, &.{6});
    try testDTypeSerde(binary, &.{7});
    try testDTypeSerde(localtime(.ns), &.{ 8, 0 });
    try testDTypeSerde(localtime(.us), &.{ 8, 1 });
    try testDTypeSerde(localtime(.ms), &.{ 8, 2 });
    try testDTypeSerde(localtime(.s), &.{ 8, 3 });
    try testDTypeSerde(localdate, &.{9});
    try testDTypeSerde(instant(.ns), &.{ 10, 0 });
    try testDTypeSerde(instant(.us), &.{ 10, 1 });
    try testDTypeSerde(instant(.ms), &.{ 10, 2 });
    try testDTypeSerde(instant(.s), &.{ 10, 3 });
    try testDTypeSerde(
        struct_(
            &.{ "somename", "othername", "onemore" },
            &[_]DType{ .utf8, .{ .int = ._32 }, .{ .localtime = .ns } },
            null,
        ),
        &.{ 11, 3, 8, 115, 111, 109, 101, 110, 97, 109, 101, 9, 111, 116, 104, 101, 114, 110, 97, 109, 101, 7, 111, 110, 101, 109, 111, 114, 101, 6, 3, 3, 8, 0 },
    );
    try testDTypeSerde(
        .{ .list = .{ .child = &DType{ .null = {} }, .allocator = null } },
        &.{ 12, 0 },
    );
}

fn testDTypeSerde(actual: DType, expected: []const u8) !void {
    var buf: [256]u8 = .{0} ** 256;
    var bytesStream = std.io.fixedBufferStream(&buf);
    try actual.toBytes(bytesStream.writer());
    const writtenDType = bytesStream.getWritten();
    try std.testing.expectEqualSlices(u8, expected, writtenDType);

    var persistedStream = std.io.fixedBufferStream(expected);
    const persistedDtype = try DType.fromBytes(persistedStream.reader(), std.testing.allocator);
    defer persistedDtype.deinit();
    std.testing.expect(actual.equal(persistedDtype)) catch |err| {
        std.debug.print("Bytes {any} did not deserialize to '{}'. Expected {any}\n", .{ expected, actual, writtenDType });
        return err;
    };

    actual.deinit();
}
