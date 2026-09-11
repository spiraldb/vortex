// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use rstest::rstest;

use super::SharedBytes;
use super::UniqueBytes;
use crate::Alignment;

/// A window of `len` bytes filled with the low byte of each index.
fn filled(len: usize, alignment: Alignment) -> UniqueBytes {
    let mut bytes = UniqueBytes::with_capacity(len, alignment);
    bytes.extend_from_slice(&pattern(len));
    bytes
}

/// The low byte of each index in `0..len`.
#[expect(
    clippy::cast_possible_truncation,
    reason = "truncating to the low byte is the point of the pattern"
)]
fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| i as u8).collect()
}

#[rstest]
#[case(1)]
#[case(8)]
#[case(64)]
#[case(256)]
#[case(4096)]
fn allocations_meet_their_alignment(#[case] alignment: usize) {
    let alignment = Alignment::new(alignment);
    let bytes = filled(1000, alignment);
    assert!(alignment.is_ptr_aligned(bytes.as_ptr()));
    assert_eq!(bytes.len(), 1000);
    assert!(bytes.capacity() >= 1000);
}

#[test]
fn empty_allocates_nothing_and_satisfies_any_alignment() {
    let bytes = UniqueBytes::with_capacity(0, Alignment::new(4096));
    assert_eq!(bytes.capacity(), 0);
    assert!(Alignment::MAX.is_ptr_aligned(bytes.as_ptr()));

    let shared = SharedBytes::empty();
    assert_eq!(shared.len(), 0);
    assert!(Alignment::MAX.is_ptr_aligned(shared.as_ptr()));
    assert!(shared.as_slice().is_empty());
}

#[test]
fn zeroed_is_zeroed() {
    let bytes = UniqueBytes::zeroed(129, Alignment::new(64));
    assert_eq!(bytes.len(), 129);
    assert_eq!(bytes.as_slice(), &[0u8; 129]);
}

#[test]
fn growth_preserves_contents_and_alignment() {
    let alignment = Alignment::new(512);
    let mut bytes = filled(16, alignment);
    for i in 0..1000u32 {
        bytes.extend_from_slice(&i.to_le_bytes());
    }
    assert!(alignment.is_ptr_aligned(bytes.as_ptr()));
    assert_eq!(bytes.len(), 16 + 4000);
    assert_eq!(&bytes.as_slice()[..16], &pattern(16)[..]);
    assert_eq!(&bytes.as_slice()[16..20], &0u32.to_le_bytes());
    assert_eq!(&bytes.as_slice()[4012..4016], &999u32.to_le_bytes());
}

#[test]
fn shared_slices_share_the_allocation() {
    let shared = filled(64, Alignment::none()).freeze();
    assert!(shared.is_unique());

    let head = shared.slice(0, 8);
    assert!(!shared.is_unique());
    assert_eq!(head.as_slice(), &pattern(8)[..]);
    assert_eq!(head.as_ptr(), shared.as_ptr());

    let tail = shared.slice(8, 16);
    assert_eq!(tail.as_slice(), &pattern(16)[8..]);

    drop(head);
    drop(tail);
    assert!(shared.is_unique());
}

#[test]
fn slice_ref_recovers_the_offset() {
    let shared = filled(64, Alignment::none()).freeze();
    let subset = &shared.as_slice()[10..20];
    let sliced = shared.slice_ref(subset);
    assert_eq!(sliced.len(), 10);
    assert_eq!(sliced.as_ptr(), subset.as_ptr());
}

#[test]
#[should_panic(expected = "subset pointer")]
fn slice_ref_rejects_foreign_slices() {
    let shared = filled(64, Alignment::none()).freeze();
    let other = filled(64, Alignment::none()).freeze();
    shared.slice_ref(&other.as_slice()[..8]);
}

#[test]
fn try_into_unique_requires_sole_ownership() {
    let shared = filled(16, Alignment::none()).freeze();
    let clone = shared.clone();

    let shared = shared.try_into_unique().expect_err("two handles exist");
    drop(clone);
    assert!(shared.try_into_unique().is_ok());
}

#[test]
fn try_into_unique_recovers_capacity_to_the_end_of_the_region() {
    let mut bytes = UniqueBytes::with_capacity(1024, Alignment::none());
    bytes.extend_from_slice(&[1, 2, 3, 4]);
    let unique = bytes
        .freeze()
        .try_into_unique()
        .expect("sole handle to the region");
    assert_eq!(unique.len(), 4);
    assert!(unique.capacity() >= 1024);
}

#[test]
fn split_off_windows_are_disjoint_and_rejoin_in_place() {
    let mut a = filled(64, Alignment::none());
    let ptr = a.as_ptr();
    let b = a.split_off(16);

    assert_eq!(a.len(), 16);
    assert_eq!(a.capacity(), 16);
    assert_eq!(b.len(), 48);
    assert_eq!(b.as_ptr(), unsafe { ptr.add(16) });

    // Neither half can grow in place while the other is alive.
    assert!(!a.freeze().is_unique());

    let mut a = filled(64, Alignment::none());
    let b = a.split_off(16);
    a.unsplit(b);
    assert_eq!(a.len(), 64);
    assert_eq!(a.as_slice(), &pattern(64)[..]);
}

#[test]
fn unsplit_of_unrelated_windows_copies() {
    let mut a = filled(4, Alignment::none());
    let b = filled(4, Alignment::none());
    a.unsplit(b);
    assert_eq!(a.as_slice(), &[0, 1, 2, 3, 0, 1, 2, 3]);
}

#[test]
fn reclaim_after_sibling_is_dropped() {
    let mut a = UniqueBytes::with_capacity(1024, Alignment::none());
    a.extend_from_slice(&[1, 2, 3, 4]);
    let b = a.split_off(4);
    let ptr = a.as_ptr();
    assert_eq!(a.capacity(), 4);

    drop(b);
    // The whole region is ours again, so growing must not move the data.
    a.reserve(500);
    assert_eq!(a.as_ptr(), ptr);
    assert!(a.capacity() >= 504);
    assert_eq!(a.as_slice(), &[1, 2, 3, 4]);
}

#[test]
fn advance_gives_up_the_front_of_the_window() {
    let mut bytes = filled(16, Alignment::none());
    bytes.advance(4);
    assert_eq!(bytes.len(), 12);
    assert_eq!(bytes.as_slice()[0], 4);

    let mut shared = filled(16, Alignment::none()).freeze();
    shared.advance(4);
    assert_eq!(shared.len(), 12);
    assert_eq!(shared.as_slice()[0], 4);
}

#[test]
fn vec_round_trip_keeps_the_allocation() {
    let vec: Vec<u32> = (0..100).collect();
    let ptr = vec.as_ptr();

    let bytes = UniqueBytes::from_vec(vec);
    assert_eq!(bytes.as_ptr(), ptr.cast());
    assert_eq!(bytes.len(), 400);

    let vec = bytes
        .try_into_vec::<u32>()
        .expect("adopted from a Vec<u32>");
    assert_eq!(vec.as_ptr(), ptr);
    assert_eq!(vec.len(), 100);
    assert_eq!(vec[99], 99);
}

#[test]
fn vec_round_trip_survives_growth() {
    let vec: Vec<u32> = (0..100).collect();
    let mut bytes = UniqueBytes::from_vec(vec);
    bytes.extend_from_slice(&[0u8; 4096]);

    let vec = bytes.try_into_vec::<u32>().expect("still a u32 allocation");
    assert_eq!(vec.len(), 1124);
    assert_eq!(vec[99], 99);
}

#[test]
fn vec_round_trip_rejects_a_mismatched_layout() {
    // Over-aligned: `Vec<u32>` would free it with an alignment of 4.
    let bytes = filled(64, Alignment::new(256));
    assert!(bytes.try_into_vec::<u32>().is_err());

    // Offset: `Vec` requires the pointer to be the start of the allocation.
    let mut bytes = filled(64, Alignment::of::<u32>());
    bytes.advance(4);
    assert!(bytes.try_into_vec::<u32>().is_err());
}

#[test]
fn owned_regions_release_their_owner() {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    struct Counted {
        data: Vec<u8>,
        dropped: Arc<AtomicUsize>,
    }

    impl AsRef<[u8]> for Counted {
        fn as_ref(&self) -> &[u8] {
            &self.data
        }
    }

    impl Drop for Counted {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    let dropped = Arc::new(AtomicUsize::new(0));
    let owner = Counted {
        data: vec![1, 2, 3, 4],
        dropped: Arc::clone(&dropped),
    };

    let shared = SharedBytes::from_owner::<_, u8>(owner);
    assert_eq!(shared.as_slice(), &[1, 2, 3, 4]);
    let slice = shared.slice(1, 3);
    drop(shared);
    assert_eq!(dropped.load(Ordering::SeqCst), 0, "still referenced");

    assert_eq!(slice.as_slice(), &[2, 3]);
    // Read-only provenance: the owner only ever lent us a shared reference.
    assert!(slice.try_into_unique().is_err());
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn static_regions_are_never_writable() {
    static VALUES: [u8; 4] = [1, 2, 3, 4];
    let shared = SharedBytes::from_static(&VALUES);
    assert_eq!(shared.as_ptr(), VALUES.as_ptr());
    assert!(shared.try_into_unique().is_err());
}

/// Regions from a custom allocator embed their `Shared` in the block they describe.
mod custom_allocator {
    use std::alloc::Layout;
    use std::ptr::NonNull;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;

    use allocator_api2::alloc::AllocError;
    use allocator_api2::alloc::Allocator;
    use allocator_api2::alloc::Global;
    use rstest::rstest;

    use super::pattern;
    use crate::Alignment;
    use crate::BufferAllocatorRef;
    use crate::UniqueBytes;

    #[derive(Debug, Default)]
    struct Counts {
        allocations: AtomicUsize,
        deallocations: AtomicUsize,
        grows: AtomicUsize,
        largest_alignment: AtomicUsize,
    }

    /// Forwards to the global allocator and counts what it is asked for.
    #[derive(Debug)]
    struct Counting(Arc<Counts>);

    // SAFETY: every call is forwarded unchanged to `Global`.
    unsafe impl Allocator for Counting {
        fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            self.0.allocations.fetch_add(1, Relaxed);
            self.0.largest_alignment.fetch_max(layout.align(), Relaxed);
            Global.allocate(layout)
        }

        fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            self.0.allocations.fetch_add(1, Relaxed);
            self.0.largest_alignment.fetch_max(layout.align(), Relaxed);
            Global.allocate_zeroed(layout)
        }

        unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
            self.0.deallocations.fetch_add(1, Relaxed);
            // SAFETY: forwarded unchanged.
            unsafe { Global.deallocate(ptr, layout) }
        }

        unsafe fn grow(
            &self,
            ptr: NonNull<u8>,
            old_layout: Layout,
            new_layout: Layout,
        ) -> Result<NonNull<[u8]>, AllocError> {
            self.0.grows.fetch_add(1, Relaxed);
            self.0
                .largest_alignment
                .fetch_max(new_layout.align(), Relaxed);
            // SAFETY: forwarded unchanged.
            unsafe { Global.grow(ptr, old_layout, new_layout) }
        }
    }

    fn counting() -> (BufferAllocatorRef, Arc<Counts>) {
        let counts = Arc::new(Counts::default());
        (
            BufferAllocatorRef::new(Counting(Arc::clone(&counts))),
            counts,
        )
    }

    #[test]
    fn regions_cost_one_allocation_freed_with_the_last_handle() {
        let alignment = Alignment::new(256);
        let (allocator, counts) = counting();
        let mut bytes = UniqueBytes::with_capacity_in(100, alignment, allocator.clone());
        bytes.extend_from_slice(&pattern(100));
        assert!(alignment.is_ptr_aligned(bytes.as_ptr()));
        assert!(bytes.allocator().ptr_eq(&allocator));

        // The refcount lives inside the block, so sharing allocates nothing more.
        let shared = bytes.freeze();
        let clone = shared.clone();
        let slice = shared.slice(10, 50);
        assert!(slice.allocator().ptr_eq(&allocator));
        assert_eq!(counts.allocations.load(Relaxed), 1);
        // Alignment is reached by shifting, never by asking the allocator for it.
        assert!(counts.largest_alignment.load(Relaxed) <= 16);

        drop((shared, clone));
        assert_eq!(counts.deallocations.load(Relaxed), 0);
        assert_eq!(slice.as_slice(), &pattern(100)[10..50]);
        drop(slice);
        assert_eq!(counts.deallocations.load(Relaxed), 1);
    }

    #[rstest]
    #[case(1)]
    #[case(16)]
    #[case(256)]
    #[case(4096)]
    fn regions_grow_in_place(#[case] alignment: usize) {
        let alignment = Alignment::new(alignment);
        let (allocator, counts) = counting();
        let mut bytes = UniqueBytes::with_capacity_in(8, alignment, allocator);
        bytes.extend_from_slice(&pattern(8));
        bytes.extend_from_slice(&pattern(10_000)[8..]);

        assert!(alignment.is_ptr_aligned(bytes.as_ptr()));
        assert_eq!(bytes.as_slice(), &pattern(10_000));
        // The block was grown through the allocator, header and all, never replaced.
        assert_eq!(counts.allocations.load(Relaxed), 1);
        assert!(counts.grows.load(Relaxed) >= 1);
        assert_eq!(counts.deallocations.load(Relaxed), 0);
        drop(bytes);
        assert_eq!(counts.deallocations.load(Relaxed), 1);
    }

    #[test]
    fn advanced_windows_grow_in_place() {
        let alignment = Alignment::new(64);
        let (allocator, counts) = counting();
        let mut bytes = UniqueBytes::with_capacity_in(256, alignment, allocator);
        bytes.extend_from_slice(&pattern(256));
        bytes.advance(64);
        bytes.extend_from_slice(&pattern(4096));

        assert!(alignment.is_ptr_aligned(bytes.as_ptr()));
        assert_eq!(&bytes.as_slice()[..192], &pattern(256)[64..]);
        assert_eq!(&bytes.as_slice()[192..], &pattern(4096));
        assert_eq!(counts.allocations.load(Relaxed), 1);
        assert!(counts.grows.load(Relaxed) >= 1);
    }

    #[test]
    fn split_windows_share_the_header_and_reclaim_it() {
        let alignment = Alignment::new(64);
        let (allocator, counts) = counting();
        let mut bytes = UniqueBytes::with_capacity_in(256, alignment, allocator);
        bytes.extend_from_slice(&pattern(256));
        let other = bytes.split_off(128);
        assert_eq!(counts.allocations.load(Relaxed), 1);
        assert_eq!(bytes.as_slice(), &pattern(256)[..128]);
        assert_eq!(other.as_slice(), &pattern(256)[128..]);

        drop(other);
        assert_eq!(counts.deallocations.load(Relaxed), 0);
        // With the other half gone, the survivor grows back over the region on its own.
        bytes.extend_from_slice(&pattern(256)[128..]);
        assert_eq!(bytes.as_slice(), &pattern(256));
        assert_eq!(counts.allocations.load(Relaxed), 1);
        assert_eq!(counts.grows.load(Relaxed), 0);
        drop(bytes);
        assert_eq!(counts.deallocations.load(Relaxed), 1);
    }

    #[test]
    fn unsplit_rejoins_halves_without_copying() {
        let alignment = Alignment::new(8);
        let (allocator, counts) = counting();
        let mut bytes = UniqueBytes::with_capacity_in(64, alignment, allocator);
        bytes.extend_from_slice(&pattern(64));
        let other = bytes.split_off(32);
        bytes.unsplit(other);
        assert_eq!(bytes.capacity(), 64);
        assert_eq!(bytes.as_slice(), &pattern(64));
        assert_eq!(counts.allocations.load(Relaxed), 1);
        assert_eq!(counts.grows.load(Relaxed), 0);
    }

    #[test]
    fn zeroed_regions_are_zeroed_behind_the_header() {
        let alignment = Alignment::new(128);
        let (allocator, counts) = counting();
        let bytes = UniqueBytes::zeroed_in(1000, alignment, allocator);
        assert_eq!(bytes.as_slice(), &[0u8; 1000]);
        assert!(alignment.is_ptr_aligned(bytes.as_ptr()));
        assert_eq!(counts.allocations.load(Relaxed), 1);
    }

    #[test]
    fn regions_cannot_be_handed_out_as_vecs() {
        let alignment = Alignment::new(1);
        let (allocator, counts) = counting();
        let mut bytes = UniqueBytes::with_capacity_in(16, alignment, allocator);
        bytes.extend_from_slice(&pattern(16));
        // A `Vec` would free the block through the wrong allocator.
        let result = bytes.try_into_vec::<u8>();
        assert!(result.is_err());
        drop(result);
        assert_eq!(counts.deallocations.load(Relaxed), 1);
    }

    #[test]
    fn empty_regions_record_the_allocator_without_allocating() {
        let alignment = Alignment::new(64);
        let (allocator, counts) = counting();
        let mut bytes = UniqueBytes::with_capacity_in(0, alignment, allocator.clone());
        assert!(bytes.allocator().ptr_eq(&allocator));
        assert_eq!(counts.allocations.load(Relaxed), 0);

        bytes.extend_from_slice(&pattern(10));
        assert!(bytes.allocator().ptr_eq(&allocator));
        assert_eq!(counts.allocations.load(Relaxed), 1);
        assert_eq!(counts.grows.load(Relaxed), 0);

        let shared = bytes.freeze();
        assert!(shared.allocator().ptr_eq(&allocator));
        let unique = shared.try_into_unique().ok();
        assert!(
            unique
                .as_ref()
                .is_some_and(|unique| unique.allocator().ptr_eq(&allocator))
        );
        drop(unique);
        assert_eq!(counts.deallocations.load(Relaxed), 1);
    }
}

mod alignment_promise {
    use super::*;

    #[test]
    fn slicing_lowers_the_promise_to_what_the_new_start_satisfies() {
        let mut bytes = UniqueBytes::with_capacity(256, Alignment::new(64));
        bytes.extend_from_slice(&pattern(256));
        let shared = bytes.freeze();
        assert_eq!(shared.alignment(), Alignment::new(64));

        // A cut on a multiple of the promise keeps it.
        assert_eq!(shared.slice(128, 192).alignment(), Alignment::new(64));
        // One that is not lowers it to the strongest alignment the offset does satisfy.
        assert_eq!(shared.slice(16, 32).alignment(), Alignment::new(16));
        assert_eq!(shared.slice(1, 2).alignment(), Alignment::none());
        // Never above the parent's promise, however aligned the offset is.
        assert_eq!(shared.slice(0, 8).alignment(), Alignment::new(64));

        // Whatever it reports, the address really does satisfy it.
        for begin in 0..64 {
            let slice = shared.slice(begin, 128);
            assert!(slice.alignment().is_ptr_aligned(slice.as_ptr()));
            assert_eq!(slice.as_slice(), &pattern(256)[begin..128]);
        }
    }

    #[test]
    fn advancing_lowers_the_promise() {
        let mut bytes = UniqueBytes::with_capacity(256, Alignment::new(64));
        bytes.extend_from_slice(&pattern(256));
        let mut shared = bytes.freeze();

        shared.advance(64);
        assert_eq!(shared.alignment(), Alignment::new(64));
        shared.advance(4);
        assert_eq!(shared.alignment(), Alignment::new(4));
        assert!(shared.alignment().is_ptr_aligned(shared.as_ptr()));
        assert_eq!(shared.as_slice(), &pattern(256)[68..]);
    }

    #[test]
    fn splitting_off_lowers_only_the_half_that_moves() {
        let mut bytes = UniqueBytes::with_capacity(256, Alignment::new(64));
        bytes.extend_from_slice(&pattern(256));

        let other = bytes.split_off(24);
        assert_eq!(bytes.alignment(), Alignment::new(64));
        assert_eq!(other.alignment(), Alignment::new(8));
        assert!(other.alignment().is_ptr_aligned(other.as_ptr()));

        // Rejoining restores the promise of the half that never moved.
        bytes.unsplit(other);
        assert_eq!(bytes.alignment(), Alignment::new(64));
        assert_eq!(bytes.as_slice(), pattern(256).as_slice());
    }

    #[test]
    fn slice_aligned_still_refuses_what_it_cannot_honour() {
        let mut bytes = UniqueBytes::with_capacity(256, Alignment::new(64));
        bytes.extend_from_slice(&pattern(256));
        let shared = bytes.freeze();

        // Asking for an alignment the offset cannot give is still an error, unlike `slice`.
        assert!(
            std::panic::catch_unwind(|| shared.slice_aligned(8, 16, Alignment::new(64))).is_err()
        );
    }
}
