// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! A worker's pool of read buffers.
//!
//! Reads land in buffers taken from the pool of the worker that performs them, in power-of-two
//! size classes. A buffer returns to that worker's pool when the last reference to the bytes read
//! into it is dropped, on whatever thread that happens, so a steady stream of reads reuses memory
//! that is already allocated and resident instead of faulting in fresh pages.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use crossbeam_deque::Injector;
use crossbeam_deque::Steal;
use vortex_buffer::Alignment;
use vortex_buffer::BufferMut;
use vortex_buffer::ByteBuffer;

/// The smallest size class, 4 KiB.
const MIN_CLASS: u32 = 12;
/// The largest size class, 64 MiB. Larger reads use buffers of their own.
const MAX_CLASS: u32 = 26;
/// Every pooled buffer starts on a page boundary, which satisfies any smaller alignment.
const POOL_ALIGNMENT: usize = 4096;
/// The most memory a worker's pool keeps idle. Buffers returned beyond it are freed.
const MAX_IDLE_BYTES: usize = 256 << 20;

/// The buffers a worker reads into.
pub(super) struct BufferPool {
    shared: Arc<PoolShared>,
    /// Idle buffers by size class, owned by the worker.
    free: Vec<Vec<BufferMut<u8>>>,
}

/// The part of a pool that dropped buffers return through, from any thread.
struct PoolShared {
    /// Dropped buffers, with the size class each was taken from.
    returned: Injector<(u32, BufferMut<u8>)>,
    /// Bytes held by idle buffers, whether returned or already in a free list.
    idle_bytes: AtomicUsize,
}

impl BufferPool {
    pub(super) fn new() -> Self {
        Self {
            shared: Arc::new(PoolShared {
                returned: Injector::new(),
                idle_bytes: AtomicUsize::new(0),
            }),
            free: (MIN_CLASS..=MAX_CLASS).map(|_| Vec::new()).collect(),
        }
    }

    /// Whether a read of `len` bytes at `alignment` can use a pooled buffer.
    pub(super) fn fits(len: usize, alignment: Alignment) -> bool {
        len <= 1 << MAX_CLASS && alignment.as_usize() <= POOL_ALIGNMENT
    }

    /// A buffer of exactly `len` bytes, which the caller must fill before reading it, and its
    /// size class. Requires [`fits`](Self::fits).
    pub(super) fn take(&mut self, len: usize) -> (u32, BufferMut<u8>) {
        self.collect_returned();
        let class = class_of(len);
        let mut buffer = match self.free[(class - MIN_CLASS) as usize].pop() {
            Some(buffer) => {
                self.shared
                    .idle_bytes
                    .fetch_sub(buffer.capacity(), Ordering::Relaxed);
                buffer
            }
            None => BufferMut::with_capacity_aligned(1 << class, Alignment::new(POOL_ALIGNMENT)),
        };
        // SAFETY: the capacity is at least `len`, and the caller fills the bytes before they
        // are read.
        unsafe { buffer.set_len(len) };
        (class, buffer)
    }

    /// Hands out the filled `buffer` as bytes that return it to this pool once dropped.
    pub(super) fn freeze(
        &self,
        class: u32,
        buffer: BufferMut<u8>,
        alignment: Alignment,
    ) -> ByteBuffer {
        let owner = Pooled {
            class,
            buffer: Some(buffer),
            pool: Arc::clone(&self.shared),
        };
        ByteBuffer::from_bytes_aligned(Bytes::from_owner(owner), alignment)
    }

    fn collect_returned(&mut self) {
        loop {
            match self.shared.returned.steal() {
                Steal::Success((class, buffer)) => {
                    self.free[(class - MIN_CLASS) as usize].push(buffer);
                }
                Steal::Empty => return,
                Steal::Retry => {}
            }
        }
    }
}

/// The size class holding `len` bytes.
fn class_of(len: usize) -> u32 {
    len.max(1 << MIN_CLASS)
        .next_power_of_two()
        .trailing_zeros()
        .clamp(MIN_CLASS, MAX_CLASS)
}

/// Bytes read into a pooled buffer, which goes back to its pool when they are dropped.
struct Pooled {
    class: u32,
    buffer: Option<BufferMut<u8>>,
    pool: Arc<PoolShared>,
}

impl AsRef<[u8]> for Pooled {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_ref().map_or(&[], |buffer| buffer.as_slice())
    }
}

impl Drop for Pooled {
    fn drop(&mut self) {
        let Some(mut buffer) = self.buffer.take() else {
            return;
        };
        let capacity = buffer.capacity();
        if self.pool.idle_bytes.fetch_add(capacity, Ordering::Relaxed) + capacity > MAX_IDLE_BYTES {
            self.pool.idle_bytes.fetch_sub(capacity, Ordering::Relaxed);
            return;
        }
        buffer.clear();
        self.pool.returned.push((self.class, buffer));
    }
}
