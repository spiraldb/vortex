// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Allocators for Vortex buffers.
//!
//! The allocator handle itself, [`BufferAllocatorRef`], lives in `vortex-bytes` alongside the
//! regions it allocates. This module adds the typed conveniences on top of it.

use std::alloc::Layout;
use std::ptr::NonNull;

use allocator_api2::alloc::AllocError;
use allocator_api2::alloc::Allocator;
use allocator_api2::alloc::Global;
pub use vortex_bytes::BufferAllocator;
pub use vortex_bytes::BufferAllocatorRef;

use crate::Alignment;
use crate::BufferMut;

/// Typed constructors on an allocator handle.
pub trait BufferAllocatorExt {
    /// Create a mutable buffer with this allocator.
    fn with_capacity<T>(&self, capacity: usize) -> BufferMut<T>;

    /// Create an aligned mutable buffer with this allocator.
    fn with_capacity_aligned<T>(&self, capacity: usize, alignment: Alignment) -> BufferMut<T>;

    /// Create a zeroed mutable buffer with this allocator.
    fn zeroed<T>(&self, len: usize) -> BufferMut<T>;

    /// Copy values into a mutable buffer made by this allocator.
    fn copy_from<T>(&self, values: impl AsRef<[T]>) -> BufferMut<T>;
}

impl BufferAllocatorExt for BufferAllocatorRef {
    fn with_capacity<T>(&self, capacity: usize) -> BufferMut<T> {
        BufferMut::with_capacity_in(capacity, self.clone())
    }

    fn with_capacity_aligned<T>(&self, capacity: usize, alignment: Alignment) -> BufferMut<T> {
        BufferMut::with_capacity_aligned_in(capacity, alignment, self.clone())
    }

    fn zeroed<T>(&self, len: usize) -> BufferMut<T> {
        BufferMut::zeroed_in(len, self.clone())
    }

    fn copy_from<T>(&self, values: impl AsRef<[T]>) -> BufferMut<T> {
        BufferMut::copy_from_in(values, self.clone())
    }
}

/// The allocator used by buffer APIs that do not take an allocator.
#[derive(Clone, Copy, Debug, Default)]
pub struct StaticBufferAllocator;

impl StaticBufferAllocator {
    /// Create a mutable buffer with the static allocator.
    pub fn with_capacity<T>(capacity: usize) -> BufferMut<T> {
        BufferMut::with_capacity(capacity)
    }

    /// Create an aligned mutable buffer with the static allocator.
    pub fn with_capacity_aligned<T>(capacity: usize, alignment: Alignment) -> BufferMut<T> {
        BufferMut::with_capacity_aligned(capacity, alignment)
    }

    /// Create a zeroed mutable buffer with the static allocator.
    pub fn zeroed<T>(len: usize) -> BufferMut<T> {
        BufferMut::zeroed(len)
    }

    /// Copy values into a mutable buffer made by the static allocator.
    pub fn copy_from<T>(values: impl AsRef<[T]>) -> BufferMut<T> {
        BufferMut::copy_from(values)
    }
}

// SAFETY: Global satisfies the Allocator contract and this type only forwards to it.
unsafe impl Allocator for StaticBufferAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        Global.allocate(layout)
    }

    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        Global.allocate_zeroed(layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        // SAFETY: the caller upholds the Allocator contract.
        unsafe { Global.deallocate(ptr, layout) }
    }

    unsafe fn grow(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        // SAFETY: the caller upholds the Allocator contract.
        unsafe { Global.grow(ptr, old_layout, new_layout) }
    }

    unsafe fn grow_zeroed(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        // SAFETY: the caller upholds the Allocator contract.
        unsafe { Global.grow_zeroed(ptr, old_layout, new_layout) }
    }

    unsafe fn shrink(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        // SAFETY: the caller upholds the Allocator contract.
        unsafe { Global.shrink(ptr, old_layout, new_layout) }
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::Layout;
    use std::ptr::NonNull;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use allocator_api2::alloc::AllocError;
    use allocator_api2::alloc::Allocator;
    use allocator_api2::alloc::Global;
    use rstest::rstest;
    use vortex_error::VortexResult;
    use vortex_error::vortex_err;

    use crate::Alignment;
    use crate::BufferAllocatorExt;
    use crate::BufferAllocatorRef;
    use crate::BufferMut;

    #[derive(Clone, Debug, Default)]
    struct TrackingAllocator {
        state: Arc<TrackingState>,
    }

    #[derive(Debug, Default)]
    struct TrackingState {
        allocations: AtomicUsize,
        deallocations: AtomicUsize,
        grows: AtomicUsize,
        alignment: AtomicUsize,
    }

    // SAFETY: this forwards all memory operations to Global and only records call metadata.
    unsafe impl Allocator for TrackingAllocator {
        fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            self.state.allocations.fetch_add(1, Ordering::Relaxed);
            self.state
                .alignment
                .store(layout.align(), Ordering::Relaxed);
            Global.allocate(layout)
        }

        unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
            self.state.deallocations.fetch_add(1, Ordering::Relaxed);
            // SAFETY: the caller passes the pointer and layout returned by Global.
            unsafe { Global.deallocate(ptr, layout) }
        }

        unsafe fn grow(
            &self,
            ptr: NonNull<u8>,
            old_layout: Layout,
            new_layout: Layout,
        ) -> Result<NonNull<[u8]>, AllocError> {
            self.state.grows.fetch_add(1, Ordering::Relaxed);
            // SAFETY: the caller upholds the Allocator contract.
            unsafe { Global.grow(ptr, old_layout, new_layout) }
        }
    }

    #[test]
    fn allocator_identity() {
        let static_allocator = BufferAllocatorRef::statically_allocated();
        assert!(static_allocator.ptr_eq(&BufferAllocatorRef::statically_allocated()));

        let custom_allocator = BufferAllocatorRef::new(TrackingAllocator::default());
        assert!(custom_allocator.ptr_eq(&custom_allocator.clone()));
        assert!(!custom_allocator.ptr_eq(&static_allocator));
        assert!(!custom_allocator.ptr_eq(&BufferAllocatorRef::new(TrackingAllocator::default())));
    }

    #[test]
    fn allocation_lives_until_last_view() {
        let allocator = TrackingAllocator::default();
        let state = Arc::clone(&allocator.state);
        let buffer = BufferAllocatorRef::new(allocator)
            .copy_from([1u32, 2, 3, 4])
            .freeze();
        let view = buffer.slice(0..2);

        assert_eq!(state.allocations.load(Ordering::Relaxed), 1);
        // Alignment beyond what the allocator gives for free is reached by shifting, never by
        // asking the allocator for it.
        assert!(state.alignment.load(Ordering::Relaxed) <= 16);
        assert!(buffer.is_aligned(Alignment::DEFAULT_ALIGNMENT));
        drop(buffer);
        assert_eq!(state.deallocations.load(Ordering::Relaxed), 0);
        drop(view);
        assert_eq!(state.deallocations.load(Ordering::Relaxed), 1);
    }

    #[rstest]
    fn buffer_growth_uses_allocator_grow(#[values(4, 64, 4096)] alignment: usize) {
        let allocator = TrackingAllocator::default();
        let state = Arc::clone(&allocator.state);
        let alignment = Alignment::new(alignment);
        let mut buffer =
            BufferAllocatorRef::new(allocator).with_capacity_aligned::<u32>(1, alignment);
        let initial_capacity = buffer.capacity();
        buffer.extend(std::iter::repeat_n(7, initial_capacity));

        // Well past anything the region's alignment padding could absorb.
        buffer.extend(std::iter::repeat_n(u32::MAX, 4096));
        assert!(alignment.is_ptr_aligned(buffer.as_ptr()));

        assert_eq!(&buffer[..initial_capacity], vec![7; initial_capacity]);
        assert_eq!(&buffer[initial_capacity..], vec![u32::MAX; 4096]);
        // The region was grown in place through the allocator, never replaced.
        assert_eq!(state.allocations.load(Ordering::Relaxed), 1);
        assert_eq!(state.deallocations.load(Ordering::Relaxed), 0);
        assert!(state.grows.load(Ordering::Relaxed) >= 1);

        drop(buffer);
        assert_eq!(state.deallocations.load(Ordering::Relaxed), 1);
    }

    /// The period over which [`ShiftedAllocator`] varies the address it hands back.
    ///
    /// Matches the alignment buffers are laid out at, so sweeping a gap across it covers every
    /// alignment shift a region can be given.
    const SHIFT_PERIOD: usize = Alignment::DEFAULT_ALIGNMENT.as_usize();

    /// An allocator that hands back blocks `gap` bytes past a [`SHIFT_PERIOD`] boundary.
    ///
    /// How much alignment padding a region gets depends on the address the system allocator
    /// happens to return, which makes it a poor thing for a test to depend on. This pins it.
    #[derive(Clone, Debug)]
    struct ShiftedAllocator {
        gap: usize,
        state: Arc<TrackingState>,
    }

    impl ShiftedAllocator {
        fn new(gap: usize) -> Self {
            assert!(gap < SHIFT_PERIOD, "the gap is an offset within one period");
            Self {
                gap,
                state: Arc::default(),
            }
        }

        /// The block actually asked of `Global`: one period larger, so the gap fits in front, and
        /// aligned to the period, so the gap alone decides the address.
        fn raw_layout(layout: Layout) -> Layout {
            Layout::from_size_align(
                layout.size() + SHIFT_PERIOD,
                layout.align().max(SHIFT_PERIOD),
            )
            .expect("shifted test layout")
        }

        /// Recover the block we asked of `Global` from the pointer we handed out.
        ///
        /// ## Safety
        ///
        /// `ptr` must be a pointer this allocator returned.
        unsafe fn raw_ptr(&self, ptr: NonNull<u8>) -> NonNull<u8> {
            // SAFETY: every pointer we hand out sits exactly `gap` bytes into its block.
            unsafe { NonNull::new_unchecked(ptr.as_ptr().sub(self.gap)) }
        }
    }

    // SAFETY: this forwards all memory operations to Global, offsetting each block by a fixed
    // amount that `raw_layout` reserves room for and `raw_ptr` undoes.
    unsafe impl Allocator for ShiftedAllocator {
        fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            self.state.allocations.fetch_add(1, Ordering::Relaxed);
            let raw = Global.allocate(Self::raw_layout(layout))?;
            // SAFETY: the raw block is a whole period longer than the request, so the window
            // starting `gap` bytes in still holds `layout.size()` bytes.
            Ok(unsafe {
                NonNull::slice_from_raw_parts(
                    NonNull::new_unchecked(raw.cast::<u8>().as_ptr().add(self.gap)),
                    layout.size(),
                )
            })
        }

        unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
            self.state.deallocations.fetch_add(1, Ordering::Relaxed);
            // SAFETY: the caller passes back a pointer we returned, so it sits `gap` bytes into a
            // block Global gave us for `raw_layout(layout)`.
            unsafe { Global.deallocate(self.raw_ptr(ptr), Self::raw_layout(layout)) }
        }

        unsafe fn grow(
            &self,
            ptr: NonNull<u8>,
            old_layout: Layout,
            new_layout: Layout,
        ) -> Result<NonNull<[u8]>, AllocError> {
            self.state.grows.fetch_add(1, Ordering::Relaxed);
            // Growing the raw block would move the gap, so take a fresh one and copy. This counts
            // as a grow and not an allocation, which is exactly what the caller asked for.
            let raw = Global.allocate(Self::raw_layout(new_layout))?;
            // SAFETY: both blocks are live and at least `old_layout.size()` bytes long past their
            // gaps, and they cannot overlap.
            let grown = unsafe {
                let grown = NonNull::new_unchecked(raw.cast::<u8>().as_ptr().add(self.gap));
                std::ptr::copy_nonoverlapping(ptr.as_ptr(), grown.as_ptr(), old_layout.size());
                Global.deallocate(self.raw_ptr(ptr), Self::raw_layout(old_layout));
                grown
            };
            Ok(NonNull::slice_from_raw_parts(grown, new_layout.size()))
        }
    }

    #[rstest]
    fn over_aligned_regions_still_grow_in_place(
        #[values(0, 16, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 240)]
        gap: usize,
    ) {
        // A region is laid out at `DEFAULT_ALIGNMENT` even when the buffer promises no more than
        // its element type's alignment, so the window starts at a shift the promise knows nothing
        // about. Growth has to recover that shift from the window's own address: reading it off
        // the promise mistakes the padding for a window that has walked most of the way through
        // its region, and the buffer then reallocates and copies instead of growing in place.
        //
        // The shift follows the address the allocator returns, so sweep every one it can take.
        let allocator = ShiftedAllocator::new(gap);
        let state = Arc::clone(&allocator.state);
        let mut buffer = BufferAllocatorRef::new(allocator).with_capacity::<u32>(1);
        assert_eq!(buffer.alignment(), Alignment::of::<u32>());

        buffer.extend(std::iter::repeat_n(7u32, 4096));

        assert_eq!(buffer.len(), 4096);
        assert_eq!(buffer.as_slice(), vec![7u32; 4096]);
        assert_eq!(state.allocations.load(Ordering::Relaxed), 1);
        assert!(state.grows.load(Ordering::Relaxed) >= 1);
    }

    #[test]
    fn zero_capacity_does_not_allocate() {
        let allocator = TrackingAllocator::default();
        let state = Arc::clone(&allocator.state);
        let mut buffer = BufferAllocatorRef::new(allocator).with_capacity::<u32>(0);

        assert_eq!(buffer.capacity(), 0);
        assert!(Alignment::DEFAULT_ALIGNMENT.is_offset_aligned(buffer.as_ptr().addr()));
        assert_eq!(state.allocations.load(Ordering::Relaxed), 0);

        buffer.push(42);

        assert_eq!(buffer.as_slice(), [42]);
        assert_eq!(state.allocations.load(Ordering::Relaxed), 1);
        assert_eq!(state.grows.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn empty_buffers_preserve_allocator_without_allocating() -> VortexResult<()> {
        let allocator = TrackingAllocator::default();
        let state = Arc::clone(&allocator.state);
        let allocator = BufferAllocatorRef::new(allocator);
        let buffer = BufferMut::<u32>::zeroed_in(0, allocator.clone());
        let buffer = buffer.freeze();
        let copy = buffer.clone().into_mut();
        assert!(copy.allocator().ptr_eq(&allocator));
        let mut buffer = buffer
            .try_into_mut()
            .map_err(|_| vortex_err!("unique buffer"))?;
        buffer.reserve(0);
        assert!(buffer.is_empty());
        assert!(buffer.allocator().ptr_eq(&allocator));
        drop((copy, buffer));
        assert_eq!(state.allocations.load(Ordering::Relaxed), 0);
        assert_eq!(state.grows.load(Ordering::Relaxed), 0);
        assert_eq!(state.deallocations.load(Ordering::Relaxed), 0);
        Ok(())
    }

    #[test]
    fn shared_into_mut_preserves_allocator() {
        let allocator = TrackingAllocator::default();
        let state = Arc::clone(&allocator.state);
        let allocator = BufferAllocatorRef::new(allocator);
        let original = allocator.copy_from([1u32, 2, 3]).freeze();
        let mut copy = original.clone().into_mut();
        assert!(copy.allocator().ptr_eq(&allocator));
        copy[0] = 42;
        assert_eq!(original.as_slice(), [1, 2, 3]);
        assert_eq!(copy.as_slice(), [42, 2, 3]);
        assert_eq!(state.allocations.load(Ordering::Relaxed), 2);
        drop(copy);
        assert_eq!(state.deallocations.load(Ordering::Relaxed), 1);
        drop(original);
        assert_eq!(state.deallocations.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn custom_allocator_regions_cannot_become_vecs() {
        let allocator = BufferAllocatorRef::new(TrackingAllocator::default());
        let buffer = allocator.copy_from([1u8, 2, 3]).freeze();
        // A `Vec` frees through the global allocator, so a region from anywhere else has to be
        // copied out rather than handed over.
        assert!(buffer.clone().try_into_vec().is_err());
        assert_eq!(buffer.into_vec(), vec![1, 2, 3]);
    }
}
