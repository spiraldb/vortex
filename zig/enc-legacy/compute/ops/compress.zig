const std = @import("std");
const enc = @import("../../enc.zig");
const ec = @import("../compute.zig");
const codecmath = @import("codecs").codecmath;

pub fn compress(ctx: enc.Ctx, array: *const enc.Array, options: ?*CompressOptions) !*enc.Array {
    const result = try ctx.registry.call("compress", ctx, &.{.{ .array = array }}, if (options) |op| @alignCast(@ptrCast(op)) else &struct {});
    return result.array;
}

const BoolCompressors = [_]enc.ArrayKind{ .bool, .roaring_bool, .constant };
const IntegerCompressors = [_]enc.ArrayKind{ .constant, .roaring_uint, .dictionary };
const FloatCompressors = [_]enc.ArrayKind{.dictionary};
const StringCompressors = [_]enc.ArrayKind{.dictionary};
const NoCompressors = [_]enc.ArrayKind{};

pub const CompressOptions = struct {
    const Options = struct {
        blockSize: u32 = 65536,
        sampleSize: u16 = 64,
        sampleCount: u16 = 10,
        maxDepth: u8 = 3,
        reeAverageRunThreshold: f32 = 2.0,
        encodings: []enc.ArrayKind = &.{},
        skip_encodings: []enc.ArrayKind = &.{},
    };

    options: *const Options,
    depth: u8 = 0,
    isSample: bool = false,

    pub fn allocWithDefaults(gpa: std.mem.Allocator) !CompressOptions {
        const opts = try gpa.create(Options);
        opts.* = Options{};
        return CompressOptions{ .options = opts };
    }

    pub fn forSample(self: CompressOptions) CompressOptions {
        return .{ .options = self.options, .depth = self.depth, .isSample = true };
    }

    pub fn nextLevel(self: CompressOptions) CompressOptions {
        return .{ .options = self.options, .depth = self.depth + 1, .isSample = self.isSample };
    }

    pub fn isEnabled(self: CompressOptions, kind: enc.ArrayKind) bool {
        const enabled = blk: {
            for (self.options.encodings) |encKind| {
                if (kind == encKind) {
                    break :blk true;
                }
            }
            break :blk self.options.encodings.len == 0;
        };
        const disabled = blk: {
            for (self.options.skip_encodings) |skipEnc| {
                if (kind == skipEnc) {
                    break :blk true;
                }
            }
            break :blk false;
        };
        return enabled and !disabled;
    }
};

pub const Compress = ec.UnaryFunction(.{
    .name = "compress",
    .doc = "Compress an array",
    .Options = CompressOptions,
    .Impls = &.{Primitive},
});

const Primitive = struct {
    pub fn compress(ctx: enc.Ctx, array: *const enc.PrimitiveArray, options: *const anyopaque) !*enc.Array {
        if (array.array.len == 0) {
            return @constCast(&array.array);
        }

        if (array.array.stats.get(.is_constant)) |isConst| {
            if (isConst.bool.value) {
                return &(try enc.ConstantArray.fromArray(ctx.gpa, &array.array)).array;
            }
        }

        const opts: *const CompressOptions = @alignCast(@ptrCast(options));

        const candidates = compressors(array.ptype);
        var compressionKinds = std.ArrayList(enc.ArrayKind).init(ctx.gpa);
        defer compressionKinds.deinit();
        for (candidates) |kind| {
            // TODO(robert): filtered encodings for ones that support the array
            if (opts.isEnabled(kind)) {
                try compressionKinds.append(kind);
            }
        }

        if (compressionKinds.items.len == 0) {
            return @constCast(&array.array);
        }

        if (opts.isSample) {
            var bestCompressed = array.array.retain();
            for (compressionKinds.items) |encKind| {
                const compressed = try compressWithKind(encKind, &array.array, opts);
                var toRelease = compressed;
                defer toRelease.release();

                if (try compressed.getNBytes() < try bestCompressed.getNBytes()) {
                    toRelease = bestCompressed;
                    bestCompressed = compressed;
                }
            }
            return @constCast(bestCompressed);
        }

        const sample: *enc.PrimitiveArray = blk: {
            switch (array.ptype) {
                inline else => |p| {
                    const T = p.astype();
                    const zigArray: []const T = array.asSlice(T);

                    var sampleIter = try codecmath.SampleSliceIterator(T).init(
                        ctx.gpa,
                        zigArray,
                        opts.options.sampleSize,
                        opts.options.sampleCount,
                    );
                    defer sampleIter.deinit();

                    var sampleList = try std.ArrayListAligned(T, enc.Buffer.Alignment).initCapacity(ctx.gpa, sampleIter.totalNumSamples());
                    defer sampleList.deinit();
                    while (sampleIter.next()) |slice| {
                        sampleList.appendSliceAssumeCapacity(slice);
                    }
                    break :blk try enc.PrimitiveArray.allocWithOwnedSlice(ctx.gpa, T, try sampleList.toOwnedSlice());
                },
            }
        };
        defer sample.release();

        const sampleOpts = opts.forSample();

        var compressionRatios = std.AutoArrayHashMap(enc.ArrayKind, f64).init(ctx.gpa);
        defer compressionRatios.deinit();
        for (compressionKinds.items) |encKind| {
            const compressedSample = try compressWithKind(encKind, &sample.array, &sampleOpts);
            defer compressedSample.release();
            const kindSamplesSize = try compressedSample.getNBytes();
            const sampleBytes = try sample.array.getNBytes();
            try compressionRatios.put(encKind, @as(f64, @floatFromInt(kindSamplesSize)) / @as(f64, @floatFromInt(sampleBytes)));
        }

        var ratiosIter = compressionRatios.iterator();
        var bestEncoding: ?enc.ArrayKind = null;
        var bestRatio: f64 = 1;
        while (ratiosIter.next()) |next| {
            if (next.value_ptr.* < bestRatio) {
                bestRatio = next.value_ptr.*;
                bestEncoding = next.key_ptr.*;
            }
        }

        if (bestEncoding == null or bestRatio >= 1) {
            return @constCast(&array.array);
        }

        return compressWithKind(bestEncoding.?, &array.array, opts);
    }

    // TODO(robert): implement compressors
    fn compressors(ptype: enc.PType) []const enc.ArrayKind {
        if (ptype.isInteger()) {
            return &IntegerCompressors;
        } else {
            return &NoCompressors;
        }
    }

    fn compressWithKind(encKind: enc.ArrayKind, array: *const enc.Array, opts: *const CompressOptions) !*enc.Array {
        _ = opts;
        _ = array;
        switch (encKind) {
            else => std.debug.panic("Unsupported encoding kind for compression {s}", .{@tagName(encKind)}),
        }
    }
};
