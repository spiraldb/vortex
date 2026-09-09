// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::alloc::Layout;
use std::any::TypeId;
use std::cmp::max;
use std::mem::ManuallyDrop;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::atomic::AtomicUsize;

use allocator_api2::alloc::Allocator;
use allocator_api2::alloc::handle_alloc_error;

use crate::Alignment;
use crate::BufferAllocatorRef;
use crate::HEADER;
use crate::Release;
use crate::Shared;
use crate::SharedBytes;
use crate::State;
use crate::allocate_shifted;
use crate::dangling;
use crate::drop_owner;
use crate::embedded_layout;
use crate::free_header;
use crate::panic::bytes_panic;
use crate::shared_state;
use crate::shift;
use crate::shifted_layout;

/// The smallest region a growing window allocates, in bytes.
///
/// Growth from empty would otherwise start with a handful of bytes and reallocate several times
/// before reaching a useful size.
const MIN_GROWTH: usize = 256;

/// A uniquely owned, writable window into a region.
///
/// This is the storage behind `vortex-buffer`'s `BufferMut<T>`. The window `ptr..ptr + cap` is
/// exclusively ours: no other handle may read or write it, even when the underlying region is
/// shared with the other half of a [`split_off`](Self::split_off).
///
/// Like [`SharedBytes`], a window that has never been split describes its region inline and
/// allocates no refcount.
pub struct UniqueBytes {
    /// The first byte of the window.
    ptr: NonNull<u8>,
    /// The number of initialised bytes at the front of the window.
    len: usize,
    /// The size of the window in bytes.
    cap: usize,
    /// The first byte of the region. The window starts at or after it: alignment padding, bytes
    /// given up by [`advance`](Self::advance), and the other half of a split all sit in between.
    base: NonNull<u8>,
    /// The ownership state. Plain rather than atomic: this handle is never shared by reference.
    state: State,
}

// SAFETY: `Shared` is `Send`/`Sync`, and the window is exclusively owned by this handle.
unsafe impl Send for UniqueBytes {}
// SAFETY: see above.
unsafe impl Sync for UniqueBytes {}

impl UniqueBytes {
    /// A window that owns nothing, aligned to [`Alignment::MAX`].
    #[inline]
    pub fn empty() -> Self {
        Self {
            ptr: dangling(),
            len: 0,
            cap: 0,
            base: dangling(),
            state: State::STATIC,
        }
    }

    /// Allocate an empty window with room for `capacity` bytes, aligned to `alignment`, from the
    /// global allocator.
    #[inline]
    pub fn with_capacity(capacity: usize, alignment: Alignment) -> Self {
        Self::with_capacity_in(
            capacity,
            alignment,
            BufferAllocatorRef::statically_allocated(),
        )
    }

    /// Allocate an empty window with room for `capacity` bytes, aligned to `alignment`.
    #[inline]
    pub fn with_capacity_in(
        capacity: usize,
        alignment: Alignment,
        allocator: BufferAllocatorRef,
    ) -> Self {
        Self::allocate(capacity, alignment, false, allocator)
    }

    /// Allocate a window of `len` zeroed bytes, aligned to `alignment`, from the global allocator.
    #[inline]
    pub fn zeroed(len: usize, alignment: Alignment) -> Self {
        Self::zeroed_in(len, alignment, BufferAllocatorRef::statically_allocated())
    }

    /// Allocate a window of `len` zeroed bytes, aligned to `alignment`.
    #[inline]
    pub fn zeroed_in(len: usize, alignment: Alignment, allocator: BufferAllocatorRef) -> Self {
        let mut this = Self::allocate(len, alignment, true, allocator);
        this.len = len;
        this
    }

    #[inline]
    fn allocate(
        capacity: usize,
        alignment: Alignment,
        zeroed: bool,
        allocator: BufferAllocatorRef,
    ) -> Self {
        if !allocator.is_statically_allocated() {
            return Self::allocate_in(capacity, alignment, zeroed, allocator);
        }
        if capacity == 0 {
            // Nothing to allocate: the dangling pointer satisfies every alignment.
            return Self::empty();
        }

        let (base, layout, offset) = allocate_shifted(capacity, alignment, zeroed, &allocator);
        let state = match State::owned(layout.size(), Alignment::of_layout(layout)) {
            Some(state) => state,
            // A region too large to describe inline.
            None => shared_state(base, layout, allocator, 1),
        };
        Self {
            // SAFETY: `allocate_shifted` guarantees the window `offset..offset + capacity` fits.
            ptr: unsafe { base.add(offset) },
            len: 0,
            // The capacity is what was asked for. Whatever alignment padding the shift did not
            // use stays behind the window until `reclaim` grows back over it.
            cap: capacity,
            base,
            state,
        }
    }

    /// Allocate from a custom allocator.
    ///
    /// The region has to carry the allocator's handle, which only a [`Shared`] has room for, so
    /// the `Shared` is written into the front of the block itself rather than boxed separately:
    /// one allocation, like the global-allocator path. Kept out of line so that path stays small.
    #[inline(never)]
    fn allocate_in(
        capacity: usize,
        alignment: Alignment,
        zeroed: bool,
        allocator: BufferAllocatorRef,
    ) -> Self {
        if capacity == 0 {
            // Nothing to allocate: the dangling pointer satisfies every alignment. The allocator
            // is still recorded, so that growth allocates from it.
            let state = shared_state(dangling(), Layout::new::<()>(), allocator, 1);
            return Self {
                ptr: dangling(),
                len: 0,
                cap: 0,
                base: dangling(),
                state,
            };
        }

        let layout = embedded_layout(capacity, alignment);
        let block = if zeroed {
            allocator.allocate_zeroed(layout)
        } else {
            allocator.allocate(layout)
        };
        let block = block
            .unwrap_or_else(|_| handle_alloc_error(layout))
            .cast::<u8>();
        // SAFETY: `embedded_layout` reserves `HEADER` bytes at the front of the block.
        let base = unsafe { block.add(HEADER) };
        let header = block.cast::<Shared>();
        // SAFETY: the block is aligned for a `Shared` and has room for one, and nothing else has
        // seen it yet.
        unsafe {
            header.write(Shared {
                refcount: AtomicUsize::new(1),
                base,
                size: layout.size() - HEADER,
                writable: true,
                release: Release::Embedded { layout, allocator },
            });
        }
        let offset = shift(base, alignment);
        debug_assert!(HEADER + offset + capacity <= layout.size());
        Self {
            // SAFETY: `embedded_layout` pads the region by the largest shift that could be
            // needed, so the window `offset..offset + capacity` fits behind the header.
            ptr: unsafe { base.add(offset) },
            len: 0,
            cap: capacity,
            base,
            // SAFETY: we just wrote `header` and take over its single reference.
            state: unsafe { State::shared(header.as_ptr()) },
        }
    }

    /// Take ownership of a `Vec<T>`'s allocation without copying it.
    ///
    /// The buffer treats the elements as plain bytes and never runs `T`'s destructor. Callers that
    /// need destructors must keep the `Vec` alive themselves, e.g. through
    /// [`SharedBytes::from_owner`].
    #[inline]
    pub fn from_vec<T>(vec: Vec<T>) -> Self {
        let mut vec = ManuallyDrop::new(vec);
        let capacity = vec.capacity();
        let len = vec.len();
        // A `Vec` with no capacity owns no allocation, so there is nothing to adopt. Zero-sized
        // elements have no byte representation at all.
        if capacity == 0 || size_of::<T>() == 0 {
            drop(ManuallyDrop::into_inner(vec));
            return Self::empty();
        }

        // SAFETY: `as_mut_ptr` is derived from a unique reference to the `Vec`'s buffer, giving
        // the pointer write provenance over the whole `capacity`.
        let base = unsafe { NonNull::new_unchecked(vec.as_mut_ptr().cast::<u8>()) };

        // `Vec<T>` allocates its buffer through the global allocator with exactly this layout, so
        // recording it as one of our own allocations is enough to free it correctly - and lets
        // `try_into_vec` hand it straight back out again.
        let layout = Layout::array::<T>(capacity)
            .unwrap_or_else(|_| bytes_panic!("a live Vec's layout is always representable"));

        let state = match State::owned(layout.size(), Alignment::of_layout(layout)) {
            Some(state) => state,
            // A `Vec` too large to describe inline.
            None => shared_state(base, layout, BufferAllocatorRef::statically_allocated(), 1),
        };
        Self {
            ptr: base,
            len: len * size_of::<T>(),
            cap: layout.size(),
            base,
            state,
        }
    }

    /// Adopt a writable region kept alive by `owner`, without copying it.
    ///
    /// Taking `owner` by value and going through [`AsMut`] is what makes this safe: it proves that
    /// nothing else can be observing the region while we hold it.
    pub fn from_owner<O, T>(owner: O) -> Self
    where
        O: AsMut<[T]> + Send + 'static,
    {
        // Leak the box before reading the slice out of it, so that the pointer we keep is derived
        // from a raw pointer that nothing reborrows again.
        let owner: *mut O = Box::into_raw(Box::new(owner));
        // SAFETY: we have just created `owner` and nothing else can free it or reach into it.
        let slice: &mut [T] = unsafe { &mut *owner }.as_mut();
        let size = size_of_val(slice);
        // An empty window never dereferences its pointer, so prefer the maximally aligned dangling
        // address over the owner's. The owner is kept alive either way: its `Drop` may release
        // resources the caller expects us to hold.
        let base = if size == 0 {
            dangling()
        } else {
            NonNull::from(slice).cast::<u8>()
        };

        let shared = Shared {
            refcount: AtomicUsize::new(1),
            base,
            size,
            // `base` is derived from a unique reference, so it may be written through.
            writable: true,
            release: Release::Owner {
                owner: owner.cast::<()>(),
                drop: drop_owner::<O>,
                type_id: TypeId::of::<O>(),
            },
        }
        .into_raw();

        Self {
            ptr: base,
            len: size,
            cap: size,
            base,
            // SAFETY: we just created `shared` and take over its single reference.
            state: unsafe { State::shared(shared) },
        }
    }

    /// Construct from a window into a region.
    ///
    /// ## Safety
    ///
    /// The caller must hold the only handle to `ptr..ptr + cap`, that range must lie within the
    /// region `state` describes, `base` must be that region's first byte, the region must be
    /// writable, the first `len` bytes must be initialised, and the caller must hand over its
    /// ownership.
    #[inline]
    pub(crate) unsafe fn from_parts(
        ptr: NonNull<u8>,
        len: usize,
        cap: usize,
        base: NonNull<u8>,
        state: State,
    ) -> Self {
        debug_assert!(len <= cap);
        Self {
            ptr,
            len,
            cap,
            base,
            state,
        }
    }

    /// The address of the first byte of the window.
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Whether the window holds no initialised bytes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The number of initialised bytes in the window.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// The number of bytes the window can hold before it has to grow.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// The allocator the region came from, and so the one to allocate any derived region with.
    ///
    /// Regions that were adopted rather than allocated report the global allocator.
    #[inline]
    pub fn allocator(&self) -> &BufferAllocatorRef {
        if self.state.is_shared() {
            // SAFETY: we hold a reference to the `Shared`, so it is live.
            unsafe { &*self.state.as_shared() }.allocator()
        } else {
            BufferAllocatorRef::static_ref()
        }
    }

    /// The window's initialised bytes.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: the first `len` bytes of the window are initialised and exclusively ours.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    /// The window's initialised bytes, mutably.
    ///
    /// Nothing else can see them: that is what [`UniqueBytes`] means.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: the first `len` bytes of the window are initialised and exclusively ours.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    /// The uninitialised tail of the window.
    #[inline]
    pub fn spare_capacity_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        // SAFETY: `len..cap` is within the window, which is exclusively ours.
        unsafe {
            std::slice::from_raw_parts_mut(
                self.ptr.as_ptr().add(self.len).cast::<MaybeUninit<u8>>(),
                self.cap - self.len,
            )
        }
    }

    /// Set the number of initialised bytes.
    ///
    /// ## Safety
    ///
    /// `len` must not exceed [`capacity`](Self::capacity), and the bytes up to `len` must be
    /// initialised.
    #[inline]
    pub unsafe fn set_len(&mut self, len: usize) {
        debug_assert!(len <= self.cap);
        self.len = len;
    }

    /// Advance the start of the window by `cnt` bytes, giving up the bytes skipped over.
    ///
    /// This does not preserve alignment: advancing by anything that is not a multiple of the
    /// buffer's alignment leaves the window unaligned. Keeping to a multiple is the caller's
    /// business - `vortex-buffer`'s `BufferMut::advance` rejects the rest - and a
    /// subsequent [`reserve`](Self::reserve) will re-align by reallocating rather than reclaiming
    /// in place.
    #[inline]
    pub fn advance(&mut self, cnt: usize) {
        if cnt > self.len {
            bytes_panic!(
                "cannot advance past the end of the buffer: {cnt} > {}",
                self.len
            );
        }
        // SAFETY: `cnt <= len <= cap`, so the new start stays inside the window. The region's
        // start is tracked separately, so this cannot lose it.
        self.ptr = unsafe { self.ptr.add(cnt) };
        self.len -= cnt;
        self.cap -= cnt;
    }

    /// The address one past the last byte of the region this window lies in.
    #[inline]
    fn region_end(&self) -> usize {
        if self.state.is_owned() {
            self.base.as_ptr().addr() + self.state.owned_size()
        } else if self.state.is_static() {
            self.ptr.as_ptr().addr() + self.cap
        } else {
            // SAFETY: we hold a reference to the `Shared`, so it is live.
            unsafe { &*self.state.as_shared() }.end_addr()
        }
    }

    /// Whether nothing else holds the region, so we are free to grow back over all of it.
    #[inline]
    fn owns_region(&self) -> bool {
        if self.state.is_owned() {
            return true;
        }
        if self.state.is_static() {
            return false;
        }
        // SAFETY: we hold a reference to the `Shared`, so it is live.
        unsafe { &*self.state.as_shared() }.is_unique()
    }

    /// The layout the region was allocated with, if we allocated it ourselves.
    #[inline]
    fn allocated_layout(&self) -> Option<Layout> {
        if self.state.is_owned() {
            Some(self.state.owned_layout())
        } else if self.state.is_static() {
            None
        } else {
            // SAFETY: we hold a reference to the `Shared`, so it is live.
            unsafe { &*self.state.as_shared() }.allocated_layout()
        }
    }

    /// Ensure the window has room for `additional` more bytes past its length.
    ///
    /// The resulting window is aligned to at least `alignment`.
    #[inline]
    pub fn reserve(&mut self, additional: usize, alignment: Alignment) {
        if additional <= self.cap - self.len {
            return;
        }
        self.reserve_slow(additional, alignment);
    }

    /// The slow path of [`reserve`](Self::reserve), kept out of line so the common case inlines.
    /// Not marked cold: a buffer built up from empty lands here on its very first append.
    #[inline(never)]
    fn reserve_slow(&mut self, additional: usize, alignment: Alignment) {
        let required = self
            .len
            .checked_add(additional)
            .unwrap_or_else(|| bytes_panic!("buffer capacity overflow"));

        if self.reclaim(required, alignment) {
            return;
        }

        // Amortise the cost of growing by at least doubling each time.
        let target = max(required, self.cap.saturating_mul(2)).max(MIN_GROWTH);
        if self.grow_in_place(target, alignment) {
            return;
        }

        // Fall back to a fresh region from the same allocator.
        let allocator = self.allocator().clone();
        let mut grown = Self::with_capacity_in(target, alignment, allocator);
        grown.extend_from_slice(self.as_slice(), alignment);
        *self = grown;
    }

    /// Reclaim capacity in our own region that a sibling window has since released.
    ///
    /// After `a.split_off(n)` the two halves share one region. Once the other half is dropped we
    /// are free to grow back over it without touching the allocator.
    ///
    /// A region described inline has never been split, so there is nothing to reclaim but the
    /// alignment padding the shift did not use. That is skipped on purpose: how much of it there
    /// is depends on the address the allocator happened to return, and capacity should not.
    fn reclaim(&mut self, required: usize, alignment: Alignment) -> bool {
        if self.state.is_owned()
            || !alignment.is_ptr_aligned(self.ptr.as_ptr())
            || !self.owns_region()
        {
            return false;
        }
        let available = self.region_end() - self.ptr.as_ptr().addr();
        if available < required {
            return false;
        }
        self.cap = available;
        true
    }

    /// Ask the allocator to grow our region in place.
    ///
    /// Only possible when we hold the region alone, we allocated it ourselves, and the window sits
    /// at its front - behind nothing but alignment padding. Growing keeps the region's base
    /// alignment, so the window may have to shift within the grown region to stay aligned; the
    /// padding accounts for that.
    fn grow_in_place(&mut self, target: usize, alignment: Alignment) -> bool {
        if !self.owns_region() {
            return false;
        }
        let Some(layout) = self.allocated_layout() else {
            return false;
        };
        // An empty region from a custom allocator was never allocated, so there is nothing to
        // grow; see `allocate_in`.
        if layout.size() == 0 {
            return false;
        }
        let old_offset = self.ptr.as_ptr().addr() - self.base.as_ptr().addr();
        // The window sits at the alignment shift unless `advance` moved it further in. When most
        // of the region has been given up that way, growing would carry it all along; a fresh
        // region copies only the live bytes.
        let advanced = old_offset.saturating_sub(shift(self.base, alignment));
        if advanced > layout.size() / 2 {
            return false;
        }

        if self.state.is_shared() {
            // SAFETY: we hold a reference to the `Shared`, so it is live.
            let shared = unsafe { &*self.state.as_shared() };
            if matches!(shared.release, Release::Embedded { .. }) {
                return self.grow_embedded(layout, target, alignment, old_offset);
            }
        }

        let new_layout = shifted_layout(target, alignment);
        if new_layout.size() <= layout.size() {
            return false;
        }
        let new_state = if self.state.is_owned() {
            match State::owned(new_layout.size(), Alignment::of_layout(new_layout)) {
                Some(state) => state,
                None => return false,
            }
        } else {
            self.state
        };

        // SAFETY: `base` is a live block from `allocator` with `layout`, and `new_layout` is no
        // smaller.
        let block = unsafe { self.allocator().grow(self.base, layout, new_layout) };
        let base = block
            .unwrap_or_else(|_| handle_alloc_error(new_layout))
            .cast::<u8>();
        let new_offset = shift(base, alignment);
        // SAFETY: `grow` preserved the first `layout.size()` bytes, so the initialised bytes still
        // sit at `old_offset`, and both windows lie within the new block.
        unsafe { self.move_window(base, old_offset, new_offset) };

        if new_state.is_shared() {
            // SAFETY: we hold the only reference, so nothing else can observe the update.
            let shared = unsafe { &mut *new_state.as_shared() };
            shared.base = base;
            shared.size = new_layout.size();
            let Release::Allocated { layout, .. } = &mut shared.release else {
                unreachable!("only allocated regions grow")
            };
            *layout = new_layout;
        }

        self.base = base;
        // SAFETY: the window `new_offset..new_offset + target` fits inside the new block.
        self.ptr = unsafe { base.add(new_offset) };
        self.cap = target;
        self.state = new_state;
        true
    }

    /// Grow a region whose `Shared` is embedded in its block. The header moves with the block.
    fn grow_embedded(
        &mut self,
        layout: Layout,
        target: usize,
        alignment: Alignment,
        old_offset: usize,
    ) -> bool {
        let new_layout = embedded_layout(target, alignment);
        if new_layout.size() <= layout.size() {
            return false;
        }
        // SAFETY: we hold a reference to the `Shared`, so it is live; a `SHARED` word is never
        // null.
        let header = unsafe { NonNull::new_unchecked(self.state.as_shared()) };
        // `grow` carries the header over byte for byte, allocator handle included, but the
        // handle we call through has to outlive the old block.
        // SAFETY: as above.
        let allocator = unsafe { header.as_ref() }.allocator().clone();
        // SAFETY: `header` is the start of a live block from `allocator` with `layout`, and
        // `new_layout` is no smaller.
        let block = unsafe { allocator.grow(header.cast::<u8>(), layout, new_layout) };
        let block = block
            .unwrap_or_else(|_| handle_alloc_error(new_layout))
            .cast::<u8>();
        let header = block.cast::<Shared>();
        // SAFETY: the header sits at the front of the block, followed by the region.
        let base = unsafe { block.add(HEADER) };
        let new_offset = shift(base, alignment);
        // SAFETY: `grow` preserved the old block's bytes, so the initialised bytes still sit at
        // `old_offset` behind the header, and both windows lie within the new block.
        unsafe { self.move_window(base, old_offset, new_offset) };

        // SAFETY: `grow` carried the header over, and we hold the only reference to it.
        let shared = unsafe { &mut *header.as_ptr() };
        shared.base = base;
        shared.size = new_layout.size() - HEADER;
        let Release::Embedded { layout, .. } = &mut shared.release else {
            unreachable!("the header was embedded before the block moved")
        };
        *layout = new_layout;

        self.base = base;
        // SAFETY: the window `new_offset..new_offset + target` fits behind the header.
        self.ptr = unsafe { base.add(new_offset) };
        self.cap = target;
        // SAFETY: `header` is the moved `Shared`, and we keep the one reference we held.
        self.state = unsafe { State::shared(header.as_ptr()) };
        true
    }

    /// Move the window's initialised bytes from `old_offset` to `new_offset` within the region at
    /// `base`, after the region has been grown in place.
    ///
    /// ## Safety
    ///
    /// Both `old_offset..old_offset + len` and `new_offset..new_offset + len` must lie within the
    /// region at `base`, and the initialised bytes must sit at `old_offset`.
    #[inline]
    unsafe fn move_window(&self, base: NonNull<u8>, old_offset: usize, new_offset: usize) {
        if new_offset != old_offset && self.len != 0 {
            // SAFETY: the caller guarantees both ranges lie within the region; `copy` allows them
            // to overlap.
            unsafe {
                std::ptr::copy(
                    base.as_ptr().add(old_offset),
                    base.as_ptr().add(new_offset),
                    self.len,
                );
            }
        }
    }

    /// Append `slice` to the window, growing it if needed.
    #[inline]
    pub fn extend_from_slice(&mut self, slice: &[u8], alignment: Alignment) {
        self.reserve(slice.len(), alignment);
        // `unsplit` is the one caller that can hand us a slice from our own region, so the
        // non-overlap argument is worth spelling out: live windows into a region are disjoint, so
        // `slice` starts at or after our window's end, while the copy below stays within
        // `len..len + slice.len() <= cap`. `reserve` cannot have widened our window over `slice`
        // either - the other half still holds a reference, so `owns_region` is false and we get a
        // fresh region instead.
        debug_assert!(
            slice.is_empty()
                || slice.as_ptr().addr() + slice.len() <= self.ptr.as_ptr().addr() + self.len
                || slice.as_ptr().addr() >= self.ptr.as_ptr().addr() + self.len + slice.len(),
            "extend_from_slice source overlaps the destination"
        );
        // SAFETY: we just reserved `slice.len()` bytes past `len`, and per the argument above
        // `slice` cannot overlap them.
        unsafe {
            std::ptr::copy_nonoverlapping(
                slice.as_ptr(),
                self.ptr.as_ptr().add(self.len),
                slice.len(),
            );
        }
        self.len += slice.len();
    }

    /// Promote an inline-described region to a refcounted one, so two windows can share it.
    ///
    /// Returns the `Shared` with `refcount` references already taken.
    #[cold]
    fn promote(&mut self, refcount: usize) -> *mut Shared {
        debug_assert!(self.state.is_owned());
        // One of the references is this handle's own.
        self.state = shared_state(
            self.base,
            self.state.owned_layout(),
            BufferAllocatorRef::statically_allocated(),
            refcount,
        );
        // SAFETY: `shared_state` returns a `SHARED` word.
        unsafe { self.state.as_shared() }
    }

    /// Split the window in two at `at`, keeping `..at` and returning `at..`.
    ///
    /// Both halves keep pointing into the same region; neither moves.
    #[inline]
    pub fn split_off(&mut self, at: usize) -> Self {
        if at > self.cap {
            bytes_panic!("cannot split buffer of capacity {} at {at}", self.cap);
        }

        let state = if self.state.is_static() {
            debug_assert_eq!(self.cap, 0);
            State::STATIC
        } else if self.state.is_owned() {
            // SAFETY: `promote` takes two references, one for each half.
            unsafe { State::shared(self.promote(2)) }
        } else {
            // SAFETY: we hold a reference to the `Shared`, and take one more for the new half.
            unsafe { Shared::retain(self.state.as_shared()) };
            self.state
        };

        let other = Self {
            // SAFETY: `at <= cap`, so the split point is inside (or at the end of) the window.
            ptr: unsafe { self.ptr.add(at) },
            len: self.len.saturating_sub(at),
            cap: self.cap - at,
            base: self.base,
            state,
        };
        self.cap = at;
        self.len = self.len.min(at);
        other
    }

    /// Absorb a window previously produced by [`split_off`](Self::split_off).
    ///
    /// `O(1)` when the two windows are still adjacent in the same region; otherwise this
    /// degenerates to a copy.
    pub fn unsplit(&mut self, other: Self, alignment: Alignment) {
        if self.cap == 0 {
            *self = other;
            return;
        }
        if other.cap == 0 {
            return;
        }
        // Only a `SHARED` state names a region; see [`State::is_shared`]. Two windows that both
        // own their region outright can never be halves of the same one, however they are laid
        // out in memory.
        if self.state.is_shared()
            && self.state == other.state
            && self.ptr.as_ptr().addr() + self.len == other.ptr.as_ptr().addr()
        {
            self.cap += other.cap;
            self.len += other.len;
            // `other` gives up its reference to the region we now cover in full.
            drop(other);
            return;
        }
        self.extend_from_slice(other.as_slice(), alignment);
    }

    /// Freeze the window into an immutable, shareable one.
    #[inline]
    pub fn freeze(self) -> SharedBytes {
        let this = ManuallyDrop::new(self);
        // SAFETY: the window lies within the region, and we hand its reference over.
        unsafe { SharedBytes::from_parts(this.ptr, this.len, this.base, this.state) }
    }

    /// Hand the region out as a `Vec<T>`, if it is exactly a `Vec<T>`'s allocation.
    ///
    /// This succeeds when the region came from the global allocator with
    /// `Layout::array::<T>(capacity)` - either because it came from a `Vec<T>` in the first
    /// place, or because it was allocated with exactly `align_of::<T>()` - and our window starts
    /// at the front of it. An over-aligned buffer cannot be given away, because `Vec` would free
    /// it with the wrong layout.
    #[inline]
    pub fn try_into_vec<T>(self) -> Result<Vec<T>, Self> {
        let elem = size_of::<T>();
        if elem == 0 || !self.len.is_multiple_of(elem) {
            return Err(self);
        }
        // Nothing to hand over, so an empty `Vec` is trivially a zero-copy answer. This also
        // covers windows that own no region at all.
        if self.len == 0 {
            return Ok(Vec::new());
        }

        let Some(layout) = self.sole_global_layout() else {
            return Err(self);
        };
        if self.ptr != self.base
            || layout.align() != align_of::<T>()
            || !layout.size().is_multiple_of(elem)
        {
            return Err(self);
        }

        let capacity = layout.size() / elem;
        let length = self.len / elem;
        let ptr = self.ptr.cast::<T>().as_ptr();

        // The `Vec` takes the region over, so neither this handle nor any `Shared` may free it.
        let this = ManuallyDrop::new(self);
        if this.state.is_shared() {
            // SAFETY: `sole_global_layout` reported a boxed header with a refcount of one, so we
            // free the box without ever running its `Release`.
            unsafe { free_header(this.state.as_shared()) };
        }

        // SAFETY: the region was allocated by the global allocator with exactly
        // `Layout::array::<T>(capacity)`, its first `length` elements are initialised, and we have
        // just given up our own claim to it.
        Ok(unsafe { Vec::from_raw_parts(ptr, length, capacity) })
    }

    /// The layout of the region, if this handle alone owns it and the global allocator produced
    /// it: the two conditions for handing it out as a `Vec`.
    #[inline]
    fn sole_global_layout(&self) -> Option<Layout> {
        if self.state.is_owned() {
            return Some(self.state.owned_layout());
        }
        if self.state.is_static() {
            return None;
        }
        // SAFETY: we hold a reference to the `Shared`, so it is live.
        unsafe { &*self.state.as_shared() }.sole_global_layout()
    }
}

impl Drop for UniqueBytes {
    #[inline]
    fn drop(&mut self) {
        if self.state.is_static() {
            return;
        }
        if self.state.is_owned() {
            // SAFETY: we hold the only handle to a global-allocator region we allocated with
            // exactly this layout.
            unsafe { std::alloc::dealloc(self.base.as_ptr(), self.state.owned_layout()) };
            return;
        }
        // SAFETY: we hold one reference to a live `Shared`, and give it up here.
        unsafe { Shared::release(self.state.as_shared()) };
    }
}

impl std::fmt::Debug for UniqueBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UniqueBytes")
            .field("ptr", &self.ptr)
            .field("len", &self.len)
            .field("cap", &self.cap)
            .field("owned", &self.state.is_owned())
            .finish()
    }
}

impl AsRef<[u8]> for UniqueBytes {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl PartialEq for UniqueBytes {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for UniqueBytes {}
