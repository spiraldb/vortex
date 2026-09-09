// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The allocator a region is obtained from and returned to.

use std::alloc::Layout;
use std::fmt;
use std::fmt::Debug;
use std::ptr::NonNull;
use std::sync::Arc;

use allocator_api2::alloc::AllocError;
use allocator_api2::alloc::Allocator;
use allocator_api2::alloc::Global;

/// An allocator that can back a buffer region.
pub trait BufferAllocator: Allocator + Debug + Send + Sync + 'static {}

impl<A> BufferAllocator for A where A: Allocator + Debug + Send + Sync + 'static {}

/// A shared handle to a [`BufferAllocator`].
///
/// The global allocator is the common case and is represented without an [`Arc`], so that copying
/// the handle is a plain move and no reference count is ever touched for it. A region allocated
/// from the global allocator can be described inline by its handle (see the crate docs); a region
/// allocated from any other allocator has to carry this handle alongside its refcount, so it is
/// refcounted from the start.
#[derive(Clone, Default)]
pub struct BufferAllocatorRef(
    // `None` is the global allocator. `Some` keeps a custom allocator alive for as long as any
    // region allocated from it is.
    Option<Arc<dyn BufferAllocator>>,
);

static GLOBAL: BufferAllocatorRef = BufferAllocatorRef(None);

impl BufferAllocatorRef {
    /// Wrap an allocator in a shared handle.
    pub fn new(allocator: impl BufferAllocator) -> Self {
        Self(Some(Arc::new(allocator)))
    }

    /// The handle for the global allocator.
    #[inline]
    pub const fn statically_allocated() -> Self {
        Self(None)
    }

    /// A `'static` handle for the global allocator, for APIs that hand out a reference.
    #[inline]
    pub fn static_ref() -> &'static Self {
        &GLOBAL
    }

    /// Whether this is the global allocator.
    #[inline]
    pub fn is_statically_allocated(&self) -> bool {
        self.0.is_none()
    }

    /// Whether both handles refer to the same allocator.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (None, None) => true,
            (Some(lhs), Some(rhs)) => Arc::ptr_eq(lhs, rhs),
            _ => false,
        }
    }
}

impl Debug for BufferAllocatorRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Some(allocator) => allocator.fmt(f),
            None => f.write_str("Global"),
        }
    }
}

// SAFETY: every call is forwarded unchanged to one allocator, which upholds the contract itself.
unsafe impl Allocator for BufferAllocatorRef {
    #[inline]
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        match &self.0 {
            Some(allocator) => allocator.allocate(layout),
            None => Global.allocate(layout),
        }
    }

    #[inline]
    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        match &self.0 {
            Some(allocator) => allocator.allocate_zeroed(layout),
            None => Global.allocate_zeroed(layout),
        }
    }

    #[inline]
    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        // SAFETY: the caller upholds the `Allocator` contract for the allocator that produced `ptr`.
        match &self.0 {
            Some(allocator) => unsafe { allocator.deallocate(ptr, layout) },
            None => unsafe { Global.deallocate(ptr, layout) },
        }
    }

    #[inline]
    unsafe fn grow(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        // SAFETY: as for `deallocate`.
        match &self.0 {
            Some(allocator) => unsafe { allocator.grow(ptr, old_layout, new_layout) },
            None => unsafe { Global.grow(ptr, old_layout, new_layout) },
        }
    }

    #[inline]
    unsafe fn grow_zeroed(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        // SAFETY: as for `deallocate`.
        match &self.0 {
            Some(allocator) => unsafe { allocator.grow_zeroed(ptr, old_layout, new_layout) },
            None => unsafe { Global.grow_zeroed(ptr, old_layout, new_layout) },
        }
    }

    #[inline]
    unsafe fn shrink(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        // SAFETY: as for `deallocate`.
        match &self.0 {
            Some(allocator) => unsafe { allocator.shrink(ptr, old_layout, new_layout) },
            None => unsafe { Global.shrink(ptr, old_layout, new_layout) },
        }
    }
}
