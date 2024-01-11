const std = @import("std");
const enc = @import("enc.zig");

pub inline fn writeByteSlice(buf: []const u8, writer: anytype) !void {
    try std.leb.writeULEB128(writer, buf.len);
    try writer.writeAll(buf);
}

pub inline fn readByteSliceAligned(reader: anytype, allocator: std.mem.Allocator) ![]align(enc.Buffer.Alignment) u8 {
    const len = try std.leb.readULEB128(u64, reader);
    const buf = try allocator.alignedAlloc(u8, enc.Buffer.Alignment, len);
    const readBytes = try reader.readAll(buf);
    if (readBytes != len) {
        return error.EndOfStream;
    }
    return buf;
}

pub inline fn readByteSlice(reader: anytype, allocator: std.mem.Allocator) ![]u8 {
    const len = try std.leb.readULEB128(u64, reader);
    const buf = try allocator.alloc(u8, len);
    const readBytes = try reader.readAll(buf);
    if (readBytes != len) {
        return error.EndOfStream;
    }
    return buf;
}
