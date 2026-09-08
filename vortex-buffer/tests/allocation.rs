// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Check the allocator boundary through public buffer operations.
//!
//! The allocator records requested layouts, checks ownership, and always moves on growth. Its
//! backing alignment is stronger than requested so metadata-only alignment changes are repeatable.

#![cfg(test)]
#![allow(clippy::expect_used)]
#![allow(
    clippy::disallowed_types,
    reason = "the test recorder does not need parking_lot"
)]

use std::alloc::Layout;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::Mutex;

use allocator_api2::alloc::AllocError;
use allocator_api2::alloc::Allocator;
use allocator_api2::alloc::Global;
use rstest::rstest;
use vortex_buffer::Alignment;
use vortex_buffer::BufferAllocatorRef;
use vortex_buffer::BufferMut;

#[derive(Clone, Debug, Default)]
struct TrackingAllocator {
    state: Arc<Mutex<TrackingState>>,
}

#[derive(Debug, Default)]
struct TrackingState {
    requests: Vec<Request>,
    live: Vec<(usize, Layout, Layout)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Request {
    Allocate { layout: Layout, zeroed: bool },
    Grow { old: Layout, new: Layout },
    Deallocate(Layout),
}

impl TrackingAllocator {
    fn as_ref(&self) -> BufferAllocatorRef {
        BufferAllocatorRef::new(self.clone())
    }

    fn allocate_impl(&self, layout: Layout, zeroed: bool) -> Result<NonNull<[u8]>, AllocError> {
        let backing = Layout::from_size_align(layout.size(), layout.align().max(4096))
            .map_err(|_| AllocError)?;
        let allocation = if zeroed {
            Global.allocate_zeroed(backing)?
        } else {
            Global.allocate(backing)?
        };
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.requests.push(Request::Allocate { layout, zeroed });
        state
            .live
            .push((allocation.cast::<u8>().addr().get(), layout, backing));

        Ok(allocation)
    }

    fn requests(&self) -> Vec<Request> {
        self.state
            .lock()
            .expect("tracking state is not poisoned")
            .requests
            .clone()
    }

    #[track_caller]
    fn assert_all_freed(&self) {
        assert!(
            self.state
                .lock()
                .expect("tracking state is not poisoned")
                .live
                .is_empty()
        );
    }
}

// SAFETY: The shared state keeps each requested layout paired with its Global backing layout.
// Growth allocates before freeing, and every free checks the pointer and the original layout.
unsafe impl Allocator for TrackingAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.allocate_impl(layout, false)
    }

    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.allocate_impl(layout, true)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        let mut state = self.state.lock().expect("tracking state is not poisoned");
        let index = state
            .live
            .iter()
            .position(|(address, ..)| *address == ptr.addr().get())
            .expect("the buffer must free a live allocation base");
        let (_, requested, backing) = state.live.remove(index);
        assert_eq!(layout, requested);
        state.requests.push(Request::Deallocate(layout));

        // SAFETY: The live entry identifies the original pointer and Global layout.
        unsafe { Global.deallocate(ptr, backing) }
    }

    unsafe fn grow(
        &self,
        ptr: NonNull<u8>,
        old: Layout,
        new: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            assert!(state.live.iter().any(|(address, requested, _)| {
                *address == ptr.addr().get() && *requested == old
            }));
            assert!(new.size() >= old.size());
            state.requests.push(Request::Grow { old, new });
        }
        let allocation = self.allocate(new)?;

        // SAFETY: The checked old layout fits the live block. The new block is large enough and
        // cannot overlap it because allocation precedes deallocation.
        unsafe {
            std::ptr::copy_nonoverlapping(ptr.as_ptr(), allocation.cast().as_ptr(), old.size());
            self.deallocate(ptr, old);
        }

        Ok(allocation)
    }
}

#[test]
fn allocator_identity() {
    let static_allocator = BufferAllocatorRef::statically_allocated();
    assert!(static_allocator.ptr_eq(&BufferAllocatorRef::statically_allocated()));

    let custom_allocator = TrackingAllocator::default().as_ref();
    assert!(custom_allocator.ptr_eq(&custom_allocator.clone()));
    assert!(!custom_allocator.ptr_eq(&static_allocator));
    assert!(!custom_allocator.ptr_eq(&TrackingAllocator::default().as_ref()));
}

#[rstest]
#[case::page(4096, None, 4096)]
#[case::preferred_page(4, Some(4096), 4096)]
#[case::requested_wins(4096, Some(256), 4096)]
#[case::default(4, Some(256), 256)]
#[case::natural(4, None, 4)]
fn requested_layout_reaches_allocator(
    #[case] requested: usize,
    #[case] preferred: Option<usize>,
    #[case] effective: usize,
    #[values(false, true)] zeroed: bool,
) {
    let allocator = TrackingAllocator::default();
    let alignment = Alignment::new(requested);
    let preferred = preferred.map(Alignment::new);
    let buffer = if zeroed {
        BufferMut::<u32>::zeroed_preferred_aligned_in(
            1024,
            alignment,
            preferred,
            allocator.as_ref(),
        )
    } else {
        BufferMut::<u32>::with_capacity_preferred_aligned_in(
            1024,
            alignment,
            preferred,
            allocator.as_ref(),
        )
    };
    let layout = Layout::from_size_align(4096, effective).expect("valid test layout");

    assert_eq!(allocator.requests(), [Request::Allocate { layout, zeroed }]);
    assert_eq!(buffer.alignment(), alignment);
    assert!(Alignment::new(effective).is_ptr_aligned(buffer.as_ptr()));
    assert!(buffer.capacity() >= 1024);
    if zeroed {
        assert_eq!(buffer.as_slice(), [0; 1024]);
    } else {
        assert!(buffer.is_empty());
    }

    drop(buffer);
    assert_eq!(
        allocator.requests().last(),
        Some(&Request::Deallocate(layout))
    );
    allocator.assert_all_freed();
}

#[test]
fn allocation_lives_until_last_view() {
    let allocator = TrackingAllocator::default();
    let buffer = allocator.as_ref().copy_from([1u32, 2, 3, 4]).freeze();
    let view = buffer.slice(1..3).into_byte_buffer().into_bytes();
    drop(buffer);

    assert_eq!(allocator.requests().len(), 1);
    assert_eq!(view.len(), 8);
    drop(view);
    allocator.assert_all_freed();
}

#[rstest]
fn growth_moves_initialized_and_spare_contents(#[values(4, 64, 4096)] alignment: usize) {
    let allocator = TrackingAllocator::default();
    let alignment = Alignment::new(alignment);
    let mut buffer = allocator
        .as_ref()
        .with_capacity_aligned::<u32>(17, alignment);
    let capacity = buffer.capacity();
    buffer.push(7);
    buffer
        .spare_capacity_mut()
        .fill(std::mem::MaybeUninit::new(11));
    // SAFETY: The first element and every element of the spare capacity are initialized.
    unsafe { buffer.set_len(capacity) };
    let expected = buffer.as_slice().to_vec();
    let old_ptr = buffer.as_ptr();

    buffer.push(u32::MAX);

    assert_ne!(buffer.as_ptr(), old_ptr);
    assert_eq!(&buffer[..capacity], expected);
    assert_eq!(buffer[capacity], u32::MAX);
    assert!(Alignment::DEFAULT_ALIGNMENT.is_ptr_aligned(buffer.as_ptr()));
    assert!(alignment.is_ptr_aligned(buffer.as_ptr()));
    assert!(matches!(allocator.requests()[1], Request::Grow { .. }));
    drop(buffer);
    allocator.assert_all_freed();
}

#[rstest]
#[case::large_tail(32, true)]
#[case::small_tail(1000, false)]
fn sliced_growth_preserves_contents(#[case] begin: usize, #[case] grows: bool) {
    let allocator = TrackingAllocator::default();
    let mut original = allocator.as_ref().with_capacity::<u32>(1024);
    original.extend(0..1024);
    let original = original.freeze();
    let sliced = original.slice(begin..begin + 8);
    drop(original);
    let mut sliced = sliced.try_into_mut().expect("the slice is uniquely owned");
    let capacity = sliced.capacity();
    sliced.push_n(777, capacity - sliced.len());
    let expected = sliced.as_slice().to_vec();

    sliced.push(u32::MAX);

    assert_eq!(&sliced[..capacity], expected);
    assert_eq!(sliced[capacity], u32::MAX);
    assert!(Alignment::DEFAULT_ALIGNMENT.is_ptr_aligned(sliced.as_ptr()));
    assert_eq!(
        allocator
            .requests()
            .iter()
            .any(|request| matches!(request, Request::Grow { .. })),
        grows
    );
    drop(sliced.freeze());
    allocator.assert_all_freed();
}

#[test]
fn realigning_a_slice_copies_with_its_allocator() {
    let allocator = TrackingAllocator::default();
    let original = allocator.as_ref().copy_from([1u32, 2, 3, 4]).freeze();
    let sliced = original.slice(1..3);
    drop(original);
    let buffer = sliced.try_into_mut().expect("the slice is uniquely owned");
    let capacity = buffer.capacity();
    let alignment = Alignment::new(4096);
    assert!(!alignment.is_ptr_aligned(buffer.as_ptr()));

    let mut buffer = buffer.aligned(alignment);

    assert!(alignment.is_ptr_aligned(buffer.as_ptr()));
    assert_eq!(buffer.capacity(), capacity);
    buffer.push(7);
    buffer.reserve(buffer.capacity());
    assert_eq!(buffer.as_slice(), [2, 3, 7]);
    drop(buffer);
    allocator.assert_all_freed();
}

#[rstest]
#[case::raise(4, 4096)]
#[case::lower(4096, 4)]
fn changing_alignment_uses_original_layout_for_growth(
    #[case] original_alignment: usize,
    #[case] new_alignment: usize,
) {
    let allocator = TrackingAllocator::default();
    let mut buffer = BufferMut::<u32>::with_capacity_preferred_aligned_in(
        4,
        Alignment::new(original_alignment),
        None,
        allocator.as_ref(),
    );
    buffer.extend([1, 2, 3, 4]);
    let ptr = buffer.as_ptr();
    let mut buffer = buffer.aligned(Alignment::new(new_alignment));
    assert_eq!(buffer.as_ptr(), ptr);

    buffer.reserve(buffer.capacity());

    assert_eq!(buffer.as_slice(), [1, 2, 3, 4]);
    assert_eq!(buffer.alignment(), Alignment::new(new_alignment));
    assert!(Alignment::new(4096).is_ptr_aligned(buffer.as_ptr()));
    let Request::Grow { old, new } = allocator.requests()[1] else {
        panic!("the allocator must grow the original block");
    };
    assert_eq!(old.align(), original_alignment);
    assert_eq!(new.align(), 4096);
    drop(buffer);
    allocator.assert_all_freed();
}

#[rstest]
fn zero_capacity_does_not_allocate(#[values(false, true)] zeroed: bool) {
    let allocator = TrackingAllocator::default();
    let alignment = Alignment::new(4096);
    let mut buffer = if zeroed {
        BufferMut::<u32>::zeroed_aligned_in(0, alignment, allocator.as_ref())
    } else {
        BufferMut::<u32>::with_capacity_aligned_in(0, alignment, allocator.as_ref())
    };
    assert_eq!(buffer.capacity(), 0);
    assert!(alignment.is_ptr_aligned(buffer.as_ptr()));
    assert!(allocator.requests().is_empty());

    buffer.push(42);

    assert_eq!(buffer.as_slice(), [42]);
    assert!(alignment.is_ptr_aligned(buffer.as_ptr()));
    assert_eq!(allocator.requests().len(), 1);
    drop(buffer);
    allocator.assert_all_freed();
}

#[test]
fn empty_buffers_preserve_allocator_without_allocating() {
    let allocator = TrackingAllocator::default();
    let handle = allocator.as_ref();
    let buffer = BufferMut::<u32>::zeroed_in(0, handle.clone()).freeze();
    let copy = buffer.clone().into_mut();
    assert!(copy.allocator().ptr_eq(&handle));
    let mut buffer = buffer.try_into_mut().expect("the buffer is uniquely owned");

    buffer.reserve(0);

    assert!(buffer.is_empty());
    assert!(buffer.allocator().ptr_eq(&handle));
    drop((copy, buffer));
    assert!(allocator.requests().is_empty());
}

#[test]
fn shared_into_mut_preserves_allocator() {
    let allocator = TrackingAllocator::default();
    let handle = allocator.as_ref();
    let original = handle.copy_from([1u32, 2, 3]).freeze();
    let mut copy = original.clone().into_mut();
    assert!(copy.allocator().ptr_eq(&handle));

    copy[0] = 42;

    assert_eq!(original.as_slice(), [1, 2, 3]);
    assert_eq!(copy.as_slice(), [42, 2, 3]);
    assert_eq!(allocator.requests().len(), 2);
    drop(copy);
    assert!(matches!(allocator.requests()[2], Request::Deallocate(_)));
    drop(original);
    assert!(matches!(allocator.requests()[3], Request::Deallocate(_)));
    allocator.assert_all_freed();
}

#[test]
fn empty_max_alignment() {
    let buffer = BufferMut::<u8>::zeroed_aligned(0, Alignment::MAX);
    assert!(Alignment::MAX.is_ptr_aligned(buffer.as_ptr()));
    assert_eq!(buffer.capacity(), 0);
    drop(buffer.freeze().into_mut());
}

#[test]
#[should_panic(expected = "must align to the scalar type")]
fn zeroed_rejects_alignment_below_element_alignment() {
    drop(BufferMut::<u64>::zeroed_preferred_aligned(
        1,
        Alignment::none(),
        None,
    ));
}
