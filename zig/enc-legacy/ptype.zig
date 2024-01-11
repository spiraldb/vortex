const std = @import("std");

/// Enum of primitive types supported by enc.
pub const PType = enum(u8) {
    i8 = 0,
    u8 = 1,
    i16 = 2,
    u16 = 3,
    i32 = 4,
    u32 = 5,
    i64 = 6,
    u64 = 7,
    f16 = 8,
    f32 = 9,
    f64 = 10,

    pub fn format(value: PType, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = options;
        _ = fmt;
        try writer.writeAll(value.name());
    }

    pub fn name(self: PType) []const u8 {
        return @tagName(self);
    }

    pub fn fromType(comptime T: type) PType {
        if (!@hasField(PType, @typeName(T))) {
            @compileError("Unsupported Zig type " ++ @typeName(T));
        }
        return @field(PType, @typeName(T));
    }

    pub fn fromId(id: u8) ?PType {
        for (std.enums.values(PType)) |ptype| {
            if (@intFromEnum(ptype) == id) {
                return ptype;
            }
        }
        return null;
    }

    pub fn isInteger(self: PType) bool {
        return switch (self) {
            .i8, .i16, .i32, .i64, .u8, .u16, .u32, .u64 => true,
            else => false,
        };
    }

    pub fn isSignedInteger(self: PType) bool {
        return switch (self) {
            .i8, .i16, .i32, .i64 => true,
            else => false,
        };
    }

    pub fn isUnsignedInteger(self: PType) bool {
        return switch (self) {
            .u8, .u16, .u32, .u64 => true,
            else => false,
        };
    }

    pub fn isFloat(self: PType) bool {
        return switch (self) {
            .f16, .f32, .f64 => true,
            else => false,
        };
    }

    pub fn astype(comptime self: PType) type {
        return switch (self) {
            .i8 => i8,
            .u8 => u8,
            .i16 => i16,
            .u16 => u16,
            .i32 => i32,
            .u32 => u32,
            .i64 => i64,
            .u64 => u64,
            .f16 => f16,
            .f32 => f32,
            .f64 => f64,
        };
    }

    pub fn sizeOf(self: PType) u8 {
        return switch (self) {
            inline else => |p| @sizeOf(p.astype()),
        };
    }

    pub fn bitSizeOf(self: PType) u8 {
        return self.sizeOf() * 8;
    }
};
