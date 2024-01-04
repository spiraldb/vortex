const std = @import("std");
const builtin = @import("builtin");
const zimd = @import("zimd");

comptime {
    if (!builtin.link_libc) {
        @compileError("Must be built with libc to export enc-zig symbols");
    }
}

//
// max
//
pub export fn zimd_max_u8(elems: [*c]u8, len: usize) callconv(.C) u8 {
    return zimd.math.max(u8, elems[0..len]);
}

pub export fn zimd_max_u16(elems: [*c]u16, len: usize) callconv(.C) u16 {
    return zimd.math.max(u16, elems[0..len]);
}

pub export fn zimd_max_u32(elems: [*c]u32, len: usize) callconv(.C) u32 {
    return zimd.math.max(u32, elems[0..len]);
}

pub export fn zimd_max_u64(elems: [*c]u64, len: usize) callconv(.C) u64 {
    return zimd.math.max(u64, elems[0..len]);
}

pub export fn zimd_max_i8(elems: [*c]i8, len: usize) callconv(.C) i8 {
    return zimd.math.max(i8, elems[0..len]);
}

pub export fn zimd_max_i16(elems: [*c]i16, len: usize) callconv(.C) i16 {
    return zimd.math.max(i16, elems[0..len]);
}

pub export fn zimd_max_i32(elems: [*c]i32, len: usize) callconv(.C) i32 {
    return zimd.math.max(i32, elems[0..len]);
}

pub export fn zimd_max_i64(elems: [*c]i64, len: usize) callconv(.C) i64 {
    return zimd.math.max(i64, elems[0..len]);
}

pub export fn zimd_max_f32(elems: [*c]f32, len: usize) callconv(.C) f32 {
    return zimd.math.max(f32, elems[0..len]);
}

pub export fn zimd_max_f64(elems: [*c]f64, len: usize) callconv(.C) f64 {
    return zimd.math.max(f64, elems[0..len]);
}

//
// min
//
pub export fn zimd_min_u8(elems: [*c]u8, len: usize) callconv(.C) u8 {
    return zimd.math.min(u8, elems[0..len]);
}

pub export fn zimd_min_u16(elems: [*c]u16, len: usize) callconv(.C) u16 {
    return zimd.math.min(u16, elems[0..len]);
}

pub export fn zimd_min_u32(elems: [*c]u32, len: usize) callconv(.C) u32 {
    return zimd.math.min(u32, elems[0..len]);
}

pub export fn zimd_min_u64(elems: [*c]u64, len: usize) callconv(.C) u64 {
    return zimd.math.min(u64, elems[0..len]);
}

pub export fn zimd_min_i8(elems: [*c]i8, len: usize) callconv(.C) i8 {
    return zimd.math.min(i8, elems[0..len]);
}

pub export fn zimd_min_i16(elems: [*c]i16, len: usize) callconv(.C) i16 {
    return zimd.math.min(i16, elems[0..len]);
}

pub export fn zimd_min_i32(elems: [*c]i32, len: usize) callconv(.C) i32 {
    return zimd.math.min(i32, elems[0..len]);
}

pub export fn zimd_min_i64(elems: [*c]i64, len: usize) callconv(.C) i64 {
    return zimd.math.min(i64, elems[0..len]);
}

pub export fn zimd_min_f32(elems: [*c]f32, len: usize) callconv(.C) f32 {
    return zimd.math.min(f32, elems[0..len]);
}

pub export fn zimd_min_f64(elems: [*c]f64, len: usize) callconv(.C) f64 {
    return zimd.math.min(f64, elems[0..len]);
}

//
// isSorted
//
pub export fn zimd_isSorted_u8(elems: [*c]u8, len: usize) callconv(.C) bool {
    return zimd.math.isSorted(u8, elems[0..len]);
}

pub export fn zimd_isSorted_u16(elems: [*c]u16, len: usize) callconv(.C) bool {
    return zimd.math.isSorted(u16, elems[0..len]);
}

pub export fn zimd_isSorted_u32(elems: [*c]u32, len: usize) callconv(.C) bool {
    return zimd.math.isSorted(u32, elems[0..len]);
}

pub export fn zimd_isSorted_u64(elems: [*c]u64, len: usize) callconv(.C) bool {
    return zimd.math.isSorted(u64, elems[0..len]);
}

pub export fn zimd_isSorted_i8(elems: [*c]i8, len: usize) callconv(.C) bool {
    return zimd.math.isSorted(i8, elems[0..len]);
}

pub export fn zimd_isSorted_i16(elems: [*c]i16, len: usize) callconv(.C) bool {
    return zimd.math.isSorted(i16, elems[0..len]);
}

pub export fn zimd_isSorted_i32(elems: [*c]i32, len: usize) callconv(.C) bool {
    return zimd.math.isSorted(i32, elems[0..len]);
}

pub export fn zimd_isSorted_i64(elems: [*c]i64, len: usize) callconv(.C) bool {
    return zimd.math.isSorted(i64, elems[0..len]);
}

pub export fn zimd_isSorted_f32(elems: [*c]f32, len: usize) callconv(.C) bool {
    return zimd.math.isSorted(f32, elems[0..len]);
}

pub export fn zimd_isSorted_f64(elems: [*c]f64, len: usize) callconv(.C) bool {
    return zimd.math.isSorted(f64, elems[0..len]);
}

//
// isConstant
//
pub export fn zimd_isConstant_u8(elems: [*c]u8, len: usize) callconv(.C) bool {
    return zimd.math.isConstant(u8, elems[0..len]);
}

pub export fn zimd_isConstant_u16(elems: [*c]u16, len: usize) callconv(.C) bool {
    return zimd.math.isConstant(u16, elems[0..len]);
}

pub export fn zimd_isConstant_u32(elems: [*c]u32, len: usize) callconv(.C) bool {
    return zimd.math.isConstant(u32, elems[0..len]);
}

pub export fn zimd_isConstant_u64(elems: [*c]u64, len: usize) callconv(.C) bool {
    return zimd.math.isConstant(u64, elems[0..len]);
}

pub export fn zimd_isConstant_i8(elems: [*c]i8, len: usize) callconv(.C) bool {
    return zimd.math.isConstant(i8, elems[0..len]);
}

pub export fn zimd_isConstant_i16(elems: [*c]i16, len: usize) callconv(.C) bool {
    return zimd.math.isConstant(i16, elems[0..len]);
}

pub export fn zimd_isConstant_i32(elems: [*c]i32, len: usize) callconv(.C) bool {
    return zimd.math.isConstant(i32, elems[0..len]);
}

pub export fn zimd_isConstant_i64(elems: [*c]i64, len: usize) callconv(.C) bool {
    return zimd.math.isConstant(i64, elems[0..len]);
}

pub export fn zimd_isConstant_f32(elems: [*c]f32, len: usize) callconv(.C) bool {
    return zimd.math.isConstant(f32, elems[0..len]);
}

pub export fn zimd_isConstant_f64(elems: [*c]f64, len: usize) callconv(.C) bool {
    return zimd.math.isConstant(f64, elems[0..len]);
}

//
// runLengthStats
//
pub export fn zimd_runLengthStats_u8(elems: [*c]u8, len: usize) callconv(.C) zimd.math.RunLengthStats {
    return zimd.math.runLengthStats(u8, elems[0..len]);
}

pub export fn zimd_runLengthStats_u16(elems: [*c]u16, len: usize) callconv(.C) zimd.math.RunLengthStats {
    return zimd.math.runLengthStats(u16, elems[0..len]);
}

pub export fn zimd_runLengthStats_u32(elems: [*c]u32, len: usize) callconv(.C) zimd.math.RunLengthStats {
    return zimd.math.runLengthStats(u32, elems[0..len]);
}

pub export fn zimd_runLengthStats_u64(elems: [*c]u64, len: usize) callconv(.C) zimd.math.RunLengthStats {
    return zimd.math.runLengthStats(u64, elems[0..len]);
}

pub export fn zimd_runLengthStats_i8(elems: [*c]i8, len: usize) callconv(.C) zimd.math.RunLengthStats {
    return zimd.math.runLengthStats(i8, elems[0..len]);
}

pub export fn zimd_runLengthStats_i16(elems: [*c]i16, len: usize) callconv(.C) zimd.math.RunLengthStats {
    return zimd.math.runLengthStats(i16, elems[0..len]);
}

pub export fn zimd_runLengthStats_i32(elems: [*c]i32, len: usize) callconv(.C) zimd.math.RunLengthStats {
    return zimd.math.runLengthStats(i32, elems[0..len]);
}

pub export fn zimd_runLengthStats_i64(elems: [*c]i64, len: usize) callconv(.C) zimd.math.RunLengthStats {
    return zimd.math.runLengthStats(i64, elems[0..len]);
}

pub export fn zimd_runLengthStats_f32(elems: [*c]f32, len: usize) callconv(.C) zimd.math.RunLengthStats {
    return zimd.math.runLengthStats(f32, elems[0..len]);
}

pub export fn zimd_runLengthStats_f64(elems: [*c]f64, len: usize) callconv(.C) zimd.math.RunLengthStats {
    return zimd.math.runLengthStats(f64, elems[0..len]);
}

// pub fn main() !void {
//     @setEvalBranchQuota(100_000);

//     var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
//     defer arena.deinit();
//     const allocator = arena.allocator();

//     var buffer = std.ArrayList(u8).init(allocator);
//     try buffer.writer().print(
//         \\ //////////////////////////////////////////////////////////
//         \\ // This file was auto-generated by header.zig           //
//         \\ //              Do not manually modify.                 //
//         \\ //////////////////////////////////////////////////////////
//     , .{});
// }
