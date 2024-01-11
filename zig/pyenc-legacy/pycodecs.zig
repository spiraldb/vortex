const std = @import("std");
const py = @import("pydust");
const enc = @import("pyenc");
const pyenc = @import("pyenc.zig");
const Buffer = pyenc.Buffer;
const codecs = @import("codecs");

const svb = codecs.StreamVByte(u32);

pub fn encode_svb(args: struct { array: *const pyenc.PrimitiveArray }) !struct { *const pyenc.PrimitiveArray, *const pyenc.PrimitiveArray } {
    const array = args.array.unwrap();

    if (array.ptype != enc.PType.u32) {
        return py.TypeError.raiseFmt("SVB encoding is only supported for u32, not {s}", .{@tagName(array.ptype)});
    }
    const elems = array.asSlice(u32);
    if (elems.len == 0) {
        return py.ValueError.raise("Why are you trying to compress zero elements?");
    }

    var control = try enc.PrimitiveArray.allocEmpty(pyenc.allocator(), .u8, svb.controlBytesSize(elems.len));
    errdefer control.release();
    var data = try pyenc.allocator().alignedAlloc(u8, enc.Buffer.Alignment, svb.maxCompressedDataSize(elems.len));

    const compressed_size = svb.encode(elems, control.asMutableBytes(), data);

    // Update the data slice to now known compressed size
    if (pyenc.allocator().resize(data, compressed_size)) {
        data = data[0..compressed_size];
    } else {
        errdefer pyenc.allocator().free(data);
        // The resize failed so we must copy into a new slice
        const new_data = try pyenc.allocator().alignedAlloc(u8, enc.Buffer.Alignment, compressed_size);
        @memcpy(new_data[0..compressed_size], data[0..compressed_size]);
        pyenc.allocator().free(data);
        data = new_data;
    }

    const dataArray = try enc.PrimitiveArray.allocWithOwnedSlice(pyenc.allocator(), u8, data);
    errdefer dataArray.release();

    return .{ try pyenc.PrimitiveArray.wrapOwned(control), try pyenc.PrimitiveArray.wrapOwned(dataArray) };
}

pub fn encode_bitpacked(args: struct { array: *const pyenc.PrimitiveArray, width: u8, exception_count: usize }) !struct { *const pyenc.PrimitiveArray, ?*const pyenc.PrimitiveArray, ?*const pyenc.PrimitiveArray } {
    const array = args.array.unwrap();
    const ex = args.exception_count > 0;

    // Setup the output arrays.
    // FIXME(ngates): this is really weird.
    var packedInts: *enc.PrimitiveArray = undefined;
    var exception_indices: ?*enc.PrimitiveArray = null;
    var exceptions: ?*enc.PrimitiveArray = null;

    switch (array.ptype) {
        inline .u8, .u16, .u32, .u64 => |p| {
            switch (args.width) {
                inline 1, 2, 4, 8, 16, 32, 64 => |w| {
                    if (comptime w < p.bitSizeOf()) {
                        const ints = codecs.PackedInts(p.bitSizeOf(), w);
                        var encoded = try ints.encode(array.asSlice(p.astype()), pyenc.allocator());
                        errdefer encoded.deinit();
                        packedInts = try enc.PrimitiveArray.allocWithOwnedSlice(pyenc.allocator(), u8, encoded.bytes);
                        // FIXME(wmanning): there's a double free here since encoded doesn't realize that ownership is transferred
                        // errdefer packedInts.deinit();

                        if (encoded.exception_indices) |indices| {
                            const raw_indices = try codecs.codecmath.toIndexArray(
                                u64,
                                enc.Buffer.Alignment,
                                indices,
                                pyenc.allocator(),
                            );
                            errdefer pyenc.allocator().free(raw_indices);
                            exception_indices = try enc.PrimitiveArray.allocWithOwnedSlice(
                                pyenc.allocator(),
                                u64,
                                raw_indices,
                            );
                        }

                        if (encoded.exceptions) |raw_exc| {
                            exceptions = try enc.PrimitiveArray.allocWithOwnedSlice(pyenc.allocator(), p.astype(), raw_exc);
                        }
                    } else {
                        return py.ValueError.raiseFmt("Unsupported width {}", .{args.width});
                    }
                },
                else => return py.ValueError.raiseFmt("Unsupported width {}", .{args.width}),
            }
        },
        else => return py.TypeError.raiseFmt("Unsupported ptype {}", .{array.ptype}),
    }

    return .{
        try pyenc.PrimitiveArray.wrapOwned(packedInts),
        if (ex) try pyenc.PrimitiveArray.wrapOwned(exception_indices.?) else null,
        if (ex) try pyenc.PrimitiveArray.wrapOwned(exceptions.?) else null,
    };
}

pub fn decode_bitpacked(args: struct { packedInts: *const pyenc.PrimitiveArray, length: usize, packed_width: u8, unpacked_width: u8 }) !*const pyenc.PrimitiveArray {
    const packedInts = args.packedInts.unwrap();

    // Setup the output arrays.
    switch (args.unpacked_width) {
        inline 8, 16, 32, 64 => |t| {
            switch (args.packed_width) {
                inline 1, 2, 4, 8, 16, 32, 64 => |w| {
                    if (comptime w < t) {
                        const ints = codecs.PackedInts(t, w);
                        const decoded = try ints.decode(ints.Encoded{ .bytes = packedInts.buffer.bytes, .elems_len = args.length }, pyenc.allocator());
                        errdefer pyenc.allocator().free(decoded);
                        return pyenc.PrimitiveArray.wrapOwned(try enc.PrimitiveArray.allocWithOwnedSlice(pyenc.allocator(), ints.V, decoded));
                    }
                },
                else => return py.ValueError.raiseFmt("Unsupported packed width {}", .{args.packed_width}),
            }
        },
        else => return py.TypeError.raiseFmt("Unsupported unpacked width {}", .{args.unpacked_width}),
    }
    return py.ValueError.raiseFmt("Unsupported packed width {}", .{args.packed_width});
}

pub fn encode_zigzag(args: struct { ints: *const pyenc.PrimitiveArray }) !*const pyenc.PrimitiveArray {
    const ints = args.ints.unwrap();

    switch (ints.ptype) {
        inline .i8, .i16, .i32, .i64 => |ptype| {
            const zz = codecs.ZigZag(ptype.astype());
            var out = try enc.PrimitiveArray.allocEmpty(pyenc.allocator(), enc.PType.fromType(zz.Unsigned), ints.array.len);
            zz.encode(ints.asSlice(ptype.astype()), out.asMutableSlice(zz.Unsigned));
            return pyenc.PrimitiveArray.wrapOwned(out);
        },
        else => |ptype| return py.TypeError.raiseFmt("Unsupported type for zigzag encoding {}", .{ptype}),
    }
}

pub fn decode_zigzag(args: struct { encoded: *const pyenc.PrimitiveArray }) !*const pyenc.PrimitiveArray {
    const encoded = args.encoded.unwrap();

    switch (encoded.ptype) {
        inline .u8, .u16, .u32, .u64 => |ptype| {
            const zz = codecs.ZigZag(std.meta.Int(.signed, ptype.bitSizeOf()));
            var out = try enc.PrimitiveArray.allocEmpty(pyenc.allocator(), ptype, encoded.array.len);
            zz.decode(encoded.asSlice(ptype.astype()), out.asMutableSlice(zz.Signed));
            return pyenc.PrimitiveArray.wrapOwned(out);
        },
        else => |ptype| return py.TypeError.raiseFmt("Unsupported type for zigzag encoding {}", .{ptype}),
    }
}
