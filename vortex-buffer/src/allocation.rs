// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Allocator-backed storage for Vortex buffers.
//!
//! Each allocation retains its base pointer and original layout through slicing and ownership
//! transfers. Buffer data pointers and logical alignments do not change the layout used to free it.

use std::alloc::Layout;
use std::any::Any;
use std::fmt;
use std::fmt::Debug;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::sync::Arc;

use allocator_api2::alloc::AllocError;
use allocator_api2::alloc::Allocator;
use allocator_api2::alloc::Global;
use allocator_api2::alloc::handle_alloc_error;
use vortex_error::VortexExpect;

use crate::Alignment;
use crate::BufferMut;

/// An allocator that can back a Vortex buffer.
///
/// Buffer allocations pass their byte capacity and effective alignment through [`Layout`].
pub trait BufferAllocator: Allocator + Debug + Send + Sync + 'static {}

impl<A> BufferAllocator for A where A: Allocator + Debug + Send + Sync + 'static {}

/// A shared reference to a buffer allocator.
///
/// The static allocator does not need shared ownership, so it is stored without an [`Arc`]. This
/// makes cloning the common static allocator a simple value copy.
#[derive(Clone)]
pub struct BufferAllocatorRef(
    // `None` selects the static allocator without allocating or updating an Arc reference count.
    // `Some` keeps a custom allocator alive for as long as its buffers need it.
    Option<Arc<dyn BufferAllocator>>,
);

impl BufferAllocatorRef {
    /// Wrap an allocator in a shared reference.
    pub fn new(allocator: impl BufferAllocator) -> Self {
        Self(Some(Arc::new(allocator)))
    }

    /// Return a shared reference to the static allocator.
    pub fn statically_allocated() -> Self {
        Self(None)
    }

    pub(crate) fn static_ref() -> &'static Self {
        &STATIC_ALLOCATOR
    }

    pub(crate) fn is_statically_allocated(&self) -> bool {
        self.0.is_none()
    }

    /// Returns true if both references point to the same allocator.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (None, None) => true,
            (Some(lhs), Some(rhs)) => Arc::ptr_eq(lhs, rhs),
            _ => false,
        }
    }

    /// Create a mutable buffer with this allocator.
    pub fn with_capacity<T>(&self, capacity: usize) -> BufferMut<T> {
        BufferMut::with_capacity_in(capacity, self.clone())
    }

    /// Create an aligned mutable buffer with this allocator.
    pub fn with_capacity_aligned<T>(&self, capacity: usize, alignment: Alignment) -> BufferMut<T> {
        BufferMut::with_capacity_aligned_in(capacity, alignment, self.clone())
    }

    /// Create a zeroed mutable buffer with this allocator.
    pub fn zeroed<T>(&self, len: usize) -> BufferMut<T> {
        BufferMut::zeroed_in(len, self.clone())
    }

    /// Copy values into a mutable buffer made by this allocator.
    pub fn copy_from<T>(&self, values: impl AsRef<[T]>) -> BufferMut<T> {
        BufferMut::copy_from_in(values, self.clone())
    }
}

impl Debug for BufferAllocatorRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Some(allocator) => allocator.fmt(f),
            None => StaticBufferAllocator.fmt(f),
        }
    }
}

// SAFETY: all calls are forwarded to the same allocator value held by the Arc.
unsafe impl Allocator for BufferAllocatorRef {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        match &self.0 {
            Some(allocator) => allocator.allocate(layout),
            None => Global.allocate(layout),
        }
    }

    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        match &self.0 {
            Some(allocator) => allocator.allocate_zeroed(layout),
            None => Global.allocate_zeroed(layout),
        }
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        // SAFETY: the caller upholds the Allocator contract.
        match &self.0 {
            Some(allocator) => unsafe { allocator.deallocate(ptr, layout) },
            None => unsafe { Global.deallocate(ptr, layout) },
        }
    }

    unsafe fn grow(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        // SAFETY: the caller upholds the Allocator contract.
        match &self.0 {
            Some(allocator) => unsafe { allocator.grow(ptr, old_layout, new_layout) },
            None => unsafe { Global.grow(ptr, old_layout, new_layout) },
        }
    }

    unsafe fn grow_zeroed(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        // SAFETY: the caller upholds the Allocator contract.
        match &self.0 {
            Some(allocator) => unsafe { allocator.grow_zeroed(ptr, old_layout, new_layout) },
            None => unsafe { Global.grow_zeroed(ptr, old_layout, new_layout) },
        }
    }

    unsafe fn shrink(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        // SAFETY: the caller upholds the Allocator contract.
        match &self.0 {
            Some(allocator) => unsafe { allocator.shrink(ptr, old_layout, new_layout) },
            None => unsafe { Global.shrink(ptr, old_layout, new_layout) },
        }
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

static STATIC_ALLOCATOR: BufferAllocatorRef = BufferAllocatorRef(None);

pub(crate) struct Allocation {
    /// Allocation base, or an aligned dangling pointer when the layout has zero size.
    ptr: NonNull<u8>,
    /// Layout used to allocate this block. Allocator excess is not exposed as buffer capacity.
    layout: Layout,
    allocator: BufferAllocatorRef,
}

// SAFETY: Allocation owns its memory, and its allocator is Send + Sync.
unsafe impl Send for Allocation {}
// SAFETY: shared access to Allocation never permits mutation of the allocation.
unsafe impl Sync for Allocation {}

impl Allocation {
    pub(crate) fn allocate(layout: Layout, allocator: BufferAllocatorRef) -> Self {
        Self::allocate_impl(layout, allocator, false)
    }

    pub(crate) fn allocate_zeroed(layout: Layout, allocator: BufferAllocatorRef) -> Self {
        Self::allocate_impl(layout, allocator, true)
    }

    pub(crate) fn from_vec<T>(vec: Vec<T>) -> Self {
        assert!(!std::mem::needs_drop::<T>());

        let mut vec = ManuallyDrop::new(vec);
        let layout = Layout::array::<T>(vec.capacity())
            .unwrap_or_else(|_| unreachable!("a Vec capacity always has a valid layout"));
        let ptr = NonNull::new(vec.as_mut_ptr().cast())
            .vortex_expect("a Vec always has a non-null pointer");

        Self {
            ptr,
            layout,
            allocator: BufferAllocatorRef::statically_allocated(),
        }
    }

    fn allocate_impl(layout: Layout, allocator: BufferAllocatorRef, zeroed: bool) -> Self {
        if layout.size() == 0 {
            return Self {
                ptr: layout.dangling_ptr(),
                layout,
                allocator,
            };
        }

        let allocation = if zeroed {
            allocator.allocate_zeroed(layout)
        } else {
            allocator.allocate(layout)
        }
        .unwrap_or_else(|_| handle_alloc_error(layout));

        Self {
            ptr: allocation.cast(),
            layout,
            allocator,
        }
    }

    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub(crate) fn ptr(&self) -> NonNull<u8> {
        self.ptr
    }

    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub(crate) fn size(&self) -> usize {
        self.layout.size()
    }

    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub(crate) fn alignment(&self) -> usize {
        self.layout.align()
    }

    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub(crate) fn allocator(&self) -> &BufferAllocatorRef {
        &self.allocator
    }

    pub(crate) fn grow(&mut self, new_layout: Layout) {
        debug_assert!(new_layout.size() >= self.layout.size());
        let allocation = if self.layout.size() == 0 {
            self.allocator.allocate(new_layout)
        } else {
            // SAFETY: ptr denotes a live block owned by allocator, old_layout fits the block, and
            // the caller only grows the allocation.
            unsafe { self.allocator.grow(self.ptr, self.layout, new_layout) }
        }
        .unwrap_or_else(|_| handle_alloc_error(new_layout));
        self.ptr = allocation.cast();
        self.layout = new_layout;
    }
}

impl Drop for Allocation {
    fn drop(&mut self) {
        if self.layout.size() == 0 {
            return;
        }
        // SAFETY: ptr and layout describe a live block allocated by self.allocator.
        unsafe { self.allocator.deallocate(self.ptr, self.layout) }
    }
}

pub(crate) enum BufferBacking {
    Owned(Allocation),
    Bytes(bytes::Bytes),
    #[cfg(feature = "arrow")]
    Arrow(arrow_buffer::Buffer),
    External {
        _owner: Box<dyn Any + Send + Sync>,
    },
}

impl BufferBacking {
    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub(crate) fn allocator(&self) -> &BufferAllocatorRef {
        match self {
            Self::Owned(allocation) => allocation.allocator(),
            Self::Bytes(_) | Self::External { .. } => &STATIC_ALLOCATOR,
            #[cfg(feature = "arrow")]
            Self::Arrow(_) => &STATIC_ALLOCATOR,
        }
    }
}
