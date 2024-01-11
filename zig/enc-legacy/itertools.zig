const std = @import("std");
const enc = @import("./enc.zig");

/// Defines a generic heap-allocated iterator.
pub fn Iterator(comptime T: type) type {
    return struct {
        const Iter = @This();

        ptr: *anyopaque,
        vtable: struct {
            next: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator) anyerror!?T,
            deinit: *const fn (state: *anyopaque) void,
        },

        pub fn WithState(comptime S: type) type {
            return struct {
                /// Create an iterator with a heap-allocated state.
                pub fn alloc(allocator: std.mem.Allocator, state: S) !Iter {
                    const AllocatedState = struct {
                        wrapped: S,
                        allocator: std.mem.Allocator,

                        fn next(ptr: *anyopaque, gpa: std.mem.Allocator) !?T {
                            const allocated_state: *@This() = @alignCast(@constCast(@ptrCast(ptr)));
                            return try @call(.always_inline, S.next, .{ &allocated_state.wrapped, gpa });
                        }

                        fn deinit(ptr: *anyopaque) void {
                            const allocated_state: *@This() = @alignCast(@constCast(@ptrCast(ptr)));
                            @call(.always_inline, S.deinit, .{&allocated_state.wrapped});
                            allocated_state.allocator.destroy(allocated_state);
                        }
                    };

                    const ptr = try allocator.create(AllocatedState);
                    ptr.* = .{ .wrapped = state, .allocator = allocator };

                    return .{
                        .ptr = ptr,
                        .vtable = .{ .next = &AllocatedState.next, .deinit = &AllocatedState.deinit },
                    };
                }

                /// Create an iterator with a given state pointer.
                /// The caller must ensure the state lives as long as the iterator.
                pub fn init(state: *S) !Iter {
                    const InitState = struct {
                        fn next(ptr: *anyopaque, gpa: std.mem.Allocator) !?T {
                            const state_ptr: *@This() = @alignCast(@constCast(@ptrCast(ptr)));
                            return try @call(.always_inline, S.next, .{ state_ptr, gpa });
                        }

                        fn deinit(ptr: *anyopaque) void {
                            const state_ptr: *@This() = @alignCast(@constCast(@ptrCast(ptr)));
                            @call(.always_inline, S.deinit, .{state_ptr});
                        }
                    };

                    return .{
                        .state = @ptrCast(state),
                        .vtable = .{ .next = &InitState.next, .deinit = &InitState.deinit },
                    };
                }
            };
        }

        pub fn next(self: Iter, allocator: std.mem.Allocator) !?T {
            return try self.vtable.next(self.ptr, allocator);
        }

        pub fn deinit(self: Iter) void {
            self.vtable.deinit(self.ptr);
        }
    };
}

pub const AlignedIterator = Iterator(enc.Array.Slice);

/// Utility for iterating k arrays in aligned chunks of the same size.
///
/// Each emitted slice contains new references to k arrays of equal size. If the input
/// arrays have misaligned chunking, the smallest chunk size will be sliced from each
/// of the k arrays. Note, this can lead to fragmentation.
pub fn alignedIterator(allocator: std.mem.Allocator, arrays: []const *const enc.Array) !AlignedIterator {
    // State for a single array in the aligned iterator.
    const AlignedArray = struct {
        const Self = @This();

        iter: enc.Array.Iterator,
        current: *enc.Array,
        offset: usize,

        pub fn length(self: Self) usize {
            return self.current.len - self.offset;
        }
    };

    // TODO(ngates): ensure all arrays are the same length
    const aligned_arrays = try allocator.alloc(AlignedArray, arrays.len);
    for (arrays, 0..) |array, i| {
        const iter = try array.iterPlain(allocator);
        if (try iter.next(allocator)) |chunk| {
            aligned_arrays[i] = .{ .iter = iter, .current = chunk, .offset = 0 };
        } else {
            // TODO(ngates): if they're all empty, then this is fine? But we should know this from the array lengths.
            std.debug.panic("Empty aligned array", .{});
        }
    }

    return AlignedIterator.WithState(struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        aligned_arrays: []AlignedArray,

        pub fn next(self: *Self, gpa: std.mem.Allocator) !?enc.Array.Slice {
            // Fetch the next chunks if we have consumed all of the current chunk (offset == len).
            var null_count: usize = 0;
            for (self.aligned_arrays) |*aligned| {
                if (aligned.length() == 0) {
                    if (try aligned.iter.next(gpa)) |chunk| {
                        aligned.current.release();
                        aligned.current = chunk;
                        aligned.offset = 0;
                    } else {
                        null_count += 1;
                    }
                }
            }

            // If all chunks are null, then we've finished successfully.
            if (null_count == self.aligned_arrays.len) {
                return null;
            } else if (null_count > 0) {
                std.debug.panic("Detected misaligned arrays", .{});
            }

            // Find the smallest chunk size, this is what we're going to slice from each of the available chunks.
            var smallest_chunk: usize = self.aligned_arrays[0].length();
            for (self.aligned_arrays) |aligned| {
                smallest_chunk = @min(smallest_chunk, aligned.length());
            }

            // Slice each chunk to the smallest chunk size.
            const out = try gpa.alloc(*enc.Array, self.aligned_arrays.len);
            for (self.aligned_arrays, 0..) |*aligned, i| {
                // If the array doesn't need any slicing, then just emit it
                if (aligned.current.len == smallest_chunk) {
                    out[i] = aligned.current.retain();
                } else {
                    out[i] = try aligned.current.getSlice(gpa, aligned.offset, aligned.offset + smallest_chunk);
                }
                aligned.offset += smallest_chunk;
            }

            return .{ .items = out, .allocator = gpa };
        }

        pub fn deinit(self: *Self) void {
            for (self.aligned_arrays) |aligned| {
                aligned.current.release();
                aligned.iter.deinit();
            }
            self.allocator.free(self.aligned_arrays);
        }
    }).alloc(allocator, .{
        .allocator = allocator,
        .aligned_arrays = aligned_arrays,
    });
}

test "aligned iter" {
    // Note(ngates): we don't actually have any chunked arrays.... so we're just going to test aligned single chunks.
    const a = &(try enc.PrimitiveArray.allocWithCopy(std.testing.allocator, u32, &.{ 0, 1, 2, 3 })).array;
    defer a.release();

    const iter = try alignedIterator(std.testing.allocator, &.{ a, a });
    defer iter.deinit();

    const aligned = (try iter.next(std.testing.allocator)).?;
    defer aligned.deinit();

    try std.testing.expectEqualSlices(*enc.Array, &.{ a, a }, aligned.items);
    try std.testing.expectEqual(@as(?enc.Array.Slice, null), try iter.next(std.testing.allocator));
}
