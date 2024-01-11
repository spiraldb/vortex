const std = @import("std");
const arrow = @import("arrow");
const enc = @import("./enc.zig");
const itertools = @import("./itertools.zig");

pub const Error = error{ArrowConversionFailed} || std.mem.Allocator.Error;

pub const Iterator = itertools.Iterator(arrow.Array);

/// Convert an enc DType to an Arrow Schema.
pub fn dtypeToArrow(allocator: std.mem.Allocator, dtype: enc.DType) Error!arrow.Schema {
    const ArrowFns = struct {
        /// The Arrow release callback.
        /// See: https://arrow.apache.org/docs/format/CDataInterface.html#release-callback-semantics-for-producers
        fn release(schema: *arrow.Schema) callconv(.C) void {
            const ally: *const std.mem.Allocator = @alignCast(@ptrCast(schema.private_data));

            ally.free(std.mem.span(schema.format));
            if (schema.name) |name| ally.free(std.mem.span(name));
            if (schema.n_children > 0) {
                for (schema.children.?[0..@intCast(schema.n_children)]) |child| {
                    child.deinit();
                    ally.destroy(child);
                }
            }
            ally.destroy(ally); // free-ception

            // Mark ourselves released
            schema.release = null;
        }
    };

    const fmt: [:0]u8 = try allocator.allocSentinel(u8, try arrowFormatSize(dtype), 0);
    var fbs = std.io.fixedBufferStream(fmt);
    writeArrowFormat(dtype, fbs.writer()) catch return Error.ArrowConversionFailed;

    const children = switch (dtype) {
        .struct_ => |st| blk: {
            const children = try allocator.alloc(*arrow.Schema, st.fields.len);
            for (st.names, st.fields, children) |name, sDtype, *childRef| {
                const child = try allocator.create(arrow.Schema);
                child.* = try dtypeToArrow(allocator, sDtype);
                child.name = try allocator.dupeZ(u8, name);
                childRef.* = child;
            }
            break :blk children;
        },
        else => null,
    };

    // We store the allocator in the array's private data so we can release everything again later.
    const ally = try allocator.create(std.mem.Allocator);
    ally.* = allocator;

    return arrow.Schema{
        // Mandatory. A null-terminated, UTF8-encoded string describing the data type.
        // If the data type is nested, child types are not encoded here but in the ArrowSchema.children structures.
        .format = fmt.ptr,
        .name = null,
        .metadata = null,
        .flags = .{},
        .n_children = if (children) |c| @intCast(c.len) else 0,
        .children = if (children) |c| c.ptr else null,
        .dictionary = null,
        // Mandatory. A pointer to a producer-provided release callback.
        // Optional because it needs to be set to null on release.
        .release = &ArrowFns.release,
        // Optional. An opaque pointer to producer-provided private data.
        .private_data = @ptrCast(ally),
    };
}

fn writeArrowFormat(dtype: enc.DType, writer: anytype) !void {
    try switch (dtype) {
        .null => writer.writeAll("n"),
        .nullable => |n| try writeArrowFormat(n.child.*, writer),
        .bool => writer.writeAll("b"),
        .int => |intwidth| switch (intwidth) {
            .Unknown => writer.writeAll("l"),
            ._8 => writer.writeAll("c"),
            ._16 => writer.writeAll("s"),
            ._32 => writer.writeAll("i"),
            ._64 => writer.writeAll("l"),
        },
        .uint => |intwidth| switch (intwidth) {
            .Unknown => writer.writeAll("l"),
            ._8 => writer.writeAll("C"),
            ._16 => writer.writeAll("S"),
            ._32 => writer.writeAll("I"),
            ._64 => writer.writeAll("L"),
        },
        .float => |floatwidth| switch (floatwidth) {
            .Unknown => writer.writeAll("g"),
            ._16 => writer.writeAll("e"),
            ._32 => writer.writeAll("f"),
            ._64 => writer.writeAll("g"),
        },
        .instant => Error.ArrowConversionFailed,
        .localtime => Error.ArrowConversionFailed,
        .localdate => writer.writeAll("D"),
        // FIXME(ngates): small string or large string?
        .utf8 => writer.writeAll("u"),
        // FIXME(ngates): small binary or large binary?
        .binary => writer.writeAll("z"),
        .list => Error.ArrowConversionFailed,
        .struct_ => writer.writeAll("+s"),
        .extension => Error.ArrowConversionFailed,
    };
}

fn arrowFormatSize(dtype: enc.DType) Error!usize {
    var counting_writer = std.io.countingWriter(std.io.null_writer);
    try writeArrowFormat(dtype, counting_writer.writer());
    return counting_writer.bytes_written;
}

/// DType from Arrow Schema
pub fn dtypeFromArrow(allocator: std.mem.Allocator, schema: arrow.Schema) Error!enc.DType {
    const fmt = std.mem.span(schema.format);
    switch (fmt[0]) {
        'n' => return .null,
        'b' => return .bool,
        'C' => return .{ .uint = ._8 },
        'S' => return .{ .uint = ._16 },
        'I' => return .{ .uint = ._32 },
        'L' => return .{ .uint = ._64 },
        'c' => return .{ .int = ._8 },
        's' => return .{ .int = ._16 },
        'i' => return .{ .int = ._32 },
        'l' => return .{ .int = ._64 },
        'e' => return .{ .float = ._16 },
        'f' => return .{ .float = ._32 },
        'g' => return .{ .float = ._64 },
        'z' => return .binary,
        'u' => return .utf8,
        // 'w' => return ID.FIXED_SIZE_BINARY,
        // 'd' => decimal
        't' => {
            switch (fmt[1]) {
                // 'd' => {
                //     switch (fmt[2]) {
                //         'D' => return ID.DATE32,
                //         'm' => return ID.DATE64,
                //         else => return Error.ArrowConversionFailed,
                //     }
                // },
                // 's' => return ID.TIMESTAMP,
                // 't' => {
                //     switch (fmt[2]) {
                //         'm' => return ID.TIME32,
                //         'n' => return ID.TIME64,
                //         else => return Error.ArrowConversionFailed,
                //     }
                // },
                // 'i' => {
                //     switch (fmt[2]) {
                //         'M' => return ID.INTERVAL_MONTHS,
                //         'D' => return ID.INTERVAL_DAY_TIME,
                //         'n' => return ID.INTERVAL_MONTH_DAY_NANO,
                //         else => return Error.ArrowConversionFailed,
                //     }
                // },
                //'D' => return ID.DURATION,
                else => return Error.ArrowConversionFailed,
            }
        },
        '+' => {
            switch (fmt[1]) {
                //'l' => return ID.LIST,
                's' => {
                    const n_children: usize = @intCast(schema.n_children);
                    const names = try allocator.alloc([]const u8, n_children);
                    const fields = try allocator.alloc(enc.DType, n_children);
                    for (names, fields, schema.children.?[0..n_children]) |*name, *field, child| {
                        name.* = try allocator.dupe(u8, std.mem.span(child.name.?));
                        field.* = try dtypeFromArrow(allocator, child.*);
                    }
                    return .{ .struct_ = .{ .names = names, .fields = fields, .allocator = allocator } };
                },
                //'u' => switch (fmt[2]) {
                //    's' => return ID.SPARSE_UNION,
                //    'd' => return ID.DENSE_UNION,
                //    else => return Error.ArrowConversionFailed,
                //},
                //'m' => return ID.MAP,
                //'w' => return ID.FIXED_SIZE_LIST,
                //'L' => return ID.LARGE_LIST,
                else => return Error.ArrowConversionFailed,
            }
        },
        'U' => return .utf8,
        'Z' => return .binary,
        else => return Error.ArrowConversionFailed,
    }
}
