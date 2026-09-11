// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! `unsplit` must only merge windows that really do share a region.
//!
//! A window that has never been split describes its region inline, as a size and an alignment.
//! That word is a *description*, not an identity: two windows allocated independently with the
//! same layout carry the same word. If they also happen to sit next to each other in memory, a
//! merge based on that word alone would extend the first window over the second's region and then
//! free it.
//!
//! Whether two independent allocations land adjacent is up to the allocator, so this test installs
//! one that guarantees it. To keep everything else untouched, only allocations of exactly
//! [`PROBE_SIZE`] bytes with alignment 1 are served from the arena; everything else goes to the
//! system allocator. Freed arena blocks are filled with [`POISON`] and never handed out again, so
//! reading a merged-then-freed region shows up as wrong bytes rather than as luck.

#![expect(
    clippy::tests_outside_test_module,
    reason = "integration tests are their own crate"
)]

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::cell::UnsafeCell;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use vortex_bytes::Alignment;
use vortex_bytes::UniqueBytes;

/// An odd size nothing but this test asks for.
const PROBE_SIZE: usize = 4093;
const ARENA_SIZE: usize = PROBE_SIZE * 4;
const POISON: u8 = 0xAA;

struct Arena(UnsafeCell<[u8; ARENA_SIZE]>);

// SAFETY: the arena is only ever handed out in disjoint `PROBE_SIZE` blocks, each claimed by a
// single `fetch_add`, and blocks are never reused.
unsafe impl Sync for Arena {}

static ARENA: Arena = Arena(UnsafeCell::new([0; ARENA_SIZE]));
static NEXT: AtomicUsize = AtomicUsize::new(0);

fn arena_base() -> *mut u8 {
    ARENA.0.get().cast::<u8>()
}

fn in_arena(ptr: *mut u8) -> bool {
    let base = arena_base().addr();
    (base..base + ARENA_SIZE).contains(&ptr.addr())
}

/// Hands out adjacent blocks for probe-sized requests, and defers everything else.
struct Adjacent;

// SAFETY: probe-sized requests get disjoint, never-reused blocks of the static arena, which
// satisfies any alignment of 1. Every other request, and every deallocation of a pointer outside
// the arena, is forwarded to `System` unchanged.
unsafe impl GlobalAlloc for Adjacent {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.size() == PROBE_SIZE && layout.align() == 1 {
            let offset = NEXT.fetch_add(PROBE_SIZE, Ordering::Relaxed);
            if offset + PROBE_SIZE <= ARENA_SIZE {
                // SAFETY: the offset is within the arena, and no other block covers it.
                return unsafe { arena_base().add(offset) };
            }
        }
        // SAFETY: the caller upholds `GlobalAlloc`'s contract, which we pass along unchanged.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if in_arena(ptr) {
            // Stand in for the allocator reusing the memory: anything still reading it is reading
            // freed bytes, and will see that rather than the values that used to be there.
            // SAFETY: the block is `layout.size()` bytes of the arena, and its owner just gave
            // it up.
            unsafe { ptr.write_bytes(POISON, layout.size()) };
            return;
        }
        // SAFETY: the pointer is not ours, so it came from `System`.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Adjacent = Adjacent;

fn probe_window(fill: u8) -> UniqueBytes {
    let mut window = UniqueBytes::with_capacity(PROBE_SIZE, Alignment::none());
    window.extend_from_slice(&[fill; PROBE_SIZE]);
    window
}

/// Both cases live in one test so that the arena blocks they take stay consecutive: separate
/// `#[test]` functions run concurrently and would interleave their allocations.
#[test]
fn unsplit_distinguishes_neighbouring_regions_from_split_halves() {
    merges_the_two_halves_of_one_region();
    copies_out_of_a_neighbouring_region();
}

fn copies_out_of_a_neighbouring_region() {
    let mut first = probe_window(1);
    let second = probe_window(2);

    assert!(
        std::ptr::eq(first.as_ptr().wrapping_add(first.len()), second.as_ptr()),
        "the arena is supposed to hand out adjacent blocks"
    );

    first.unsplit(second);

    assert_eq!(first.len(), PROBE_SIZE * 2);
    assert!(
        first.as_slice()[..PROBE_SIZE].iter().all(|&b| b == 1),
        "the first window's own bytes must survive"
    );
    assert!(
        first.as_slice()[PROBE_SIZE..].iter().all(|&b| b == 2),
        "the second window's bytes must be copied out before its region is released"
    );
}

fn merges_the_two_halves_of_one_region() {
    let mut first = probe_window(1);
    let second = first.split_off(PROBE_SIZE / 2);
    let start = first.as_ptr();

    first.unsplit(second);

    assert_eq!(first.len(), PROBE_SIZE);
    assert!(
        std::ptr::eq(first.as_ptr(), start),
        "halves of one region merge in place, without copying"
    );
    assert!(first.as_slice().iter().all(|&b| b == 1));
}
