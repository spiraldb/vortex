//! Python bindings for the pyenc library.
const std = @import("std");
const py = @import("pydust");
const enc = @import("pyenc");
const pyenc = @import("./pyenc.zig");

const FormatCodeToPType = std.ComptimeStringMap(enc.PType, &.{
    .{ "b", .i8 },
    .{ "B", .u8 },
    .{ "h", .i16 },
    .{ "H", .u16 },
    .{ "i", .i32 },
    .{ "I", .u32 },
    .{ "l", .i64 },
    .{ "L", .u64 },
    .{ "e", .f16 },
    .{ "f", .f32 },
    .{ "d", .f64 },
});

pub fn ptypeFromCode(format: []const u8) !enc.PType {
    return FormatCodeToPType.get(format) orelse {
        return py.TypeError.raiseFmt("Unsupported buffer format {s}", .{format});
    };
}

/// Wrap a PyBuffer in an Enc buffer. The Enc buffer will release the PyBuffer when it is freed.
pub fn bufferFromPyBuffer(gpa: std.mem.Allocator, obj: py.PyObject) !*enc.Buffer {
    const buffer = try obj.getBuffer(py.PyBuffer.Flags.RECORDS_RO);
    const len: usize = @intCast(buffer.len);

    // TODO(ngates): what should we do about bad alignment?
    // It would be nice not to have to memcpy here...?
    // But if we are going to copy, this is probably the best place to do it?
    if (@intFromPtr(buffer.buf) % enc.Buffer.Alignment != 0) {
        // Incorrect alignment, so we copy the buffer.
        defer buffer.release();

        const bytes = try gpa.alignedAlloc(u8, enc.Buffer.Alignment, len);
        @memcpy(bytes, buffer.buf[0..len]);

        return try enc.Buffer.allocWithOwnedSlice(gpa, bytes);
    }

    const Closure = struct {
        fn deinit(b: *enc.Buffer) void {
            if (b.ptr) |ptr| {
                const pybuffer: *py.PyBuffer = @ptrCast(@alignCast(ptr));
                pybuffer.release();
                b.gpa.destroy(pybuffer);
            } else {
                std.debug.panic("pybuffer pointer is missing during deinit! may indicate memory corruption", .{});
            }
        }
    };

    const pybuffer = try gpa.create(py.PyBuffer);
    pybuffer.* = buffer;

    const encBuffer = try gpa.create(enc.Buffer);
    encBuffer.* = enc.Buffer{
        .bytes = @alignCast(pybuffer.buf[0..len]),
        .is_mutable = !pybuffer.readonly,
        .ptr = pybuffer,
        .deinit = &Closure.deinit,
        .gpa = gpa,
    };
    return encBuffer;
}

pub const Buffer = py.class(struct {
    const Self = @This();

    wrapped: *enc.Buffer,

    nbytes: py.property(struct {
        pub fn get(self: *const Self) !usize {
            return self.wrapped.bytes.len;
        }
    }) = .{},

    pub fn __del__(self: *const Self) void {
        self.wrapped.release();
    }

    pub fn __repr__(self: *const Self) !py.PyString {
        return try py.PyString.createFmt("pyenc.Buffer({d})", .{self.wrapped.bytes.len});
    }

    pub fn __len__(self: *const Self) !usize {
        return self.wrapped.bytes.len;
    }

    pub fn __buffer__(self: *const Self, view: *py.PyBuffer, flags: c_int) !void {
        _ = flags;
        // We are responsible for creating a new reference to .obj
        // However the consumer will call decref it.
        py.incref(self);

        // Nothing here is allocated, so no __release_buffer__ is necessary.
        view.* = .{
            .buf = @constCast(self.wrapped.bytes.ptr),
            .obj = py.object(self).py,
            .len = @intCast(self.wrapped.bytes.len),
            .itemsize = 1,
            .readonly = true,
            .ndim = 1,
            .format = "B",
            .shape = null,
            .strides = null,
        };
    }

    fn getFormat(self: *const Self) [:0]const u8 {
        return switch (self.wrapped.ptype) {
            inline else => |p| {
                return py.PyBuffer.getFormat(p.astype());
            },
        };
    }
});

test "fromPyBuffer" {
    py.initialize();
    defer py.finalize();

    const bytes = try py.PyBytes.create("hello world");
    defer bytes.decref();

    const buffer = try bufferFromPyBuffer(std.testing.allocator, bytes.obj);
    defer buffer.release();
}
