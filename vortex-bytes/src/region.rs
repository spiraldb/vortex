// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The region primitives the handle types are built on: the tagged ownership word, the lazily
//! allocated refcount, and the allocation policy. See the crate docs for the encoding.

use std::alloc::Layout;
use std::any::TypeId;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::atomic::fence;

use allocator_api2::alloc::Allocator;
use allocator_api2::alloc::handle_alloc_error;

use crate::Alignment;
use crate::BufferAllocatorRef;
use crate::panic::bytes_panic;

/// A dangling but maximally aligned address used by buffers that own no allocation.
///
/// A zero-length slice never dereferences its pointer, so any non-null, sufficiently aligned
/// address is valid for it. The greatest power of two representable in a `usize` satisfies every
/// alignment up to [`Alignment::MAX`].
const DANGLING_ADDR: usize = 1usize << (usize::BITS - 1);

const _: () = assert!(Alignment::MAX.is_offset_aligned(DANGLING_ADDR));

/// A non-null, [`Alignment::MAX`]-aligned pointer to zero readable bytes.
#[inline]
pub(crate) fn dangling() -> NonNull<u8> {
    // SAFETY: `DANGLING_ADDR` is non-zero.
    unsafe { NonNull::new_unchecked(std::ptr::without_provenance_mut(DANGLING_ADDR)) }
}

// -------------------------------------------------------------------------------------------
// State word
// -------------------------------------------------------------------------------------------

const KIND_MASK: usize = 0b11;
/// The state word is a `*mut Shared`. A `Box` is at least 8-aligned, so its low bits are zero.
const KIND_SHARED: usize = 0b00;
/// The handle owns a global-allocator region outright, described inline by this word and `base`.
const KIND_OWNED: usize = 0b01;
/// The handle owns nothing: `'static` memory, or an empty window over no region at all.
const KIND_STATIC: usize = 0b10;

const ALIGN_SHIFT: u32 = 2;
const ALIGN_BITS: u32 = 6;
const SIZE_SHIFT: u32 = ALIGN_SHIFT + ALIGN_BITS;

/// The largest region an `OWNED` state word can describe. Anything larger is held through a
/// [`Shared`] instead, which stores the size in full.
const MAX_OWNED_SIZE: usize = usize::MAX >> SIZE_SHIFT;

/// The largest alignment exponent is that of [`Alignment::MAX`], and it has to fit in the field.
const _: () = assert!((1usize << ALIGN_BITS) > (usize::BITS - 1) as usize);

/// The ownership state of a buffer handle. See the crate docs for the encoding.
///
/// This wraps a *pointer* rather than a `usize` so that the `SHARED` case keeps its provenance:
/// rebuilding the `Shared` pointer from an integer address would make it undereferenceable. The
/// `OWNED` and `STATIC` cases are pure bit patterns that are never dereferenced, so they carry no
/// provenance and do not need any.
///
/// `OWNED` always means the global allocator. A region from any other allocator has to carry the
/// allocator's handle, which only a [`Shared`] has room for.
#[derive(Clone, Copy)]
pub(crate) struct State(pub(crate) *mut ());

impl State {
    /// The state of a handle that owns nothing.
    pub(crate) const STATIC: Self = Self(std::ptr::without_provenance_mut(KIND_STATIC));

    /// Describe a global allocation inline, if it is small enough to fit in the word.
    ///
    /// Callers must pass the size and alignment of a `Layout` that is known to be valid, so that
    /// [`owned_layout`](Self::owned_layout) can rebuild it without re-checking.
    #[inline]
    pub(crate) fn owned(size: usize, alignment: Alignment) -> Option<Self> {
        (size <= MAX_OWNED_SIZE).then(|| {
            Self(std::ptr::without_provenance_mut(
                KIND_OWNED
                    | (usize::from(alignment.exponent()) << ALIGN_SHIFT)
                    | (size << SIZE_SHIFT),
            ))
        })
    }

    /// Describe a region held through a [`Shared`].
    ///
    /// ## Safety
    ///
    /// `shared` must be a live pointer from [`Shared::into_raw`], and this state takes over one
    /// of its references.
    #[inline]
    pub(crate) unsafe fn shared(shared: *mut Shared) -> Self {
        debug_assert_eq!(shared.addr() & KIND_MASK, KIND_SHARED, "Shared is aligned");
        Self(shared.cast())
    }

    #[inline]
    fn addr(self) -> usize {
        self.0.addr()
    }

    #[inline]
    fn kind(self) -> usize {
        self.addr() & KIND_MASK
    }

    #[inline]
    pub(crate) fn is_owned(self) -> bool {
        self.kind() == KIND_OWNED
    }

    #[inline]
    pub(crate) fn is_static(self) -> bool {
        self.kind() == KIND_STATIC
    }

    /// Whether the region is held through a [`Shared`], and so has an identity two handles can be
    /// compared on. `OWNED` words describe a region rather than naming one: two handles that
    /// allocated the same layout independently carry the same word.
    #[inline]
    pub(crate) fn is_shared(self) -> bool {
        self.kind() == KIND_SHARED
    }

    /// The size of the inline-described region.
    #[inline]
    pub(crate) fn owned_size(self) -> usize {
        debug_assert!(self.is_owned());
        self.addr() >> SIZE_SHIFT
    }

    /// The alignment of the inline-described region.
    #[inline]
    fn owned_alignment(self) -> Alignment {
        debug_assert!(self.is_owned());
        #[expect(
            clippy::cast_possible_truncation,
            reason = "the exponent occupies ALIGN_BITS bits, so it fits in a u8"
        )]
        Alignment::from_exponent(((self.addr() >> ALIGN_SHIFT) & ((1 << ALIGN_BITS) - 1)) as u8)
    }

    /// The layout the inline-described region was allocated with.
    #[inline]
    pub(crate) fn owned_layout(self) -> Layout {
        // SAFETY: every `State::owned` caller passes the parts of a valid `Layout`, and the size
        // round-trips exactly because `owned` rejects anything wider than `MAX_OWNED_SIZE`.
        unsafe {
            Layout::from_size_align_unchecked(self.owned_size(), self.owned_alignment().as_usize())
        }
    }

    /// The [`Shared`] this state points at.
    ///
    /// ## Safety
    ///
    /// The state must be `SHARED`, and the pointer must still be live.
    #[inline]
    pub(crate) unsafe fn as_shared(self) -> *mut Shared {
        debug_assert_eq!(self.kind(), KIND_SHARED);
        self.0.cast::<Shared>()
    }
}

impl PartialEq for State {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.addr() == other.addr()
    }
}

impl Eq for State {}

// -------------------------------------------------------------------------------------------
// Shared state
// -------------------------------------------------------------------------------------------

/// How the memory behind a region is released.
pub(crate) enum Release {
    /// Allocated through `allocator` with exactly `layout`, and returned to it the same way. The
    /// `Shared` describing it is a separate box.
    Allocated {
        layout: Layout,
        allocator: BufferAllocatorRef,
    },
    /// Allocated through `allocator` with exactly `layout`, with the `Shared` describing it
    /// embedded at the front of the block and the region starting [`HEADER`] bytes in. Releasing
    /// the last handle returns the whole block, header included, in one call.
    ///
    /// This is how a region from a custom allocator costs a single allocation even though it has
    /// to carry the allocator's handle from the start.
    Embedded {
        layout: Layout,
        allocator: BufferAllocatorRef,
    },
    /// Kept alive by an owner value; dropping the owner releases the memory.
    ///
    /// The owner is held as a leaked `Box<O>` rather than a `Box<dyn Any>` so that no reborrow of
    /// it ever happens after we have derived the region's pointer from it: moving a `Box` asserts
    /// unique access to its contents, which would invalidate that pointer. This mirrors what
    /// `bytes::Bytes::from_owner` does. The `TypeId` is what lets the owner be handed back out to
    /// a caller that knows what it is.
    ///
    /// We never hand out a reference to the owner of a writable region, we only drop it, so `Send`
    /// alone is enough for the region to be shared across threads.
    Owner {
        owner: *mut (),
        drop: unsafe fn(*mut ()),
        type_id: TypeId,
    },
}

/// Drop a leaked `Box<O>` that was erased to a `*mut ()`.
///
/// ## Safety
///
/// `ptr` must be the result of `Box::into_raw(Box::<O>::new(..))`, and must not have been dropped.
pub(crate) unsafe fn drop_owner<O>(ptr: *mut ()) {
    // SAFETY: the caller guarantees `ptr` came from `Box::<O>::into_raw` and is still live.
    drop(unsafe { Box::from_raw(ptr.cast::<O>()) })
}

/// The refcounted description of a region shared by more than one handle.
///
/// This is allocated lazily: a handle that has never been shared describes its region inline in
/// its [`State`] instead. The exception is a region from a custom allocator, which needs somewhere
/// to keep the allocator's handle and so is refcounted from the start.
pub(crate) struct Shared {
    /// Number of live handles.
    pub(crate) refcount: AtomicUsize,
    /// The first byte of the region.
    pub(crate) base: NonNull<u8>,
    /// The size of the region in bytes.
    pub(crate) size: usize,
    /// Whether the region may be written through.
    ///
    /// This is `false` for regions we only ever obtained a shared reference to. Writing through a
    /// pointer derived from a shared reference is undefined behaviour, and the memory itself may
    /// genuinely be read-only (a `PROT_READ` mapping, a `.rodata` static).
    pub(crate) writable: bool,
    pub(crate) release: Release,
}

// SAFETY: `Shared` owns its region exclusively and hands out access only through the handles in
// this crate, which enforce that at most one of them may write to any given byte. The bytes
// themselves have no interior mutability, `Release::Owner` is `Send`, and allocators are
// `Send + Sync`, so moving the deallocation to another thread is sound.
unsafe impl Send for Shared {}
// SAFETY: see above. `&Shared` exposes nothing but the region's extent, its refcount, and a
// `Sync` allocator handle.
unsafe impl Sync for Shared {}

impl Shared {
    /// Move this description onto the heap, where handles can point at it.
    #[inline]
    pub(crate) fn into_raw(self) -> *mut Shared {
        Box::into_raw(Box::new(self))
    }

    /// Take another reference.
    ///
    /// ## Safety
    ///
    /// `shared` must be live, and the caller must already hold a reference to it.
    #[inline]
    pub(crate) unsafe fn retain(shared: *mut Shared) {
        // SAFETY: the caller guarantees the pointer is live.
        let old = unsafe { &*shared }.refcount.fetch_add(1, Ordering::Relaxed);
        // The count can only overflow if handles are leaked in a loop; abort rather than wrap
        // into a premature free. `bytes` and `Arc` take the same precaution.
        if old > usize::MAX / 2 {
            std::process::abort();
        }
    }

    /// Give up a reference, releasing the region if it was the last one.
    ///
    /// ## Safety
    ///
    /// `shared` must be live, and the caller must hold the reference being given up.
    #[inline]
    pub(crate) unsafe fn release(shared: *mut Shared) {
        // SAFETY: the caller guarantees the pointer is live.
        if unsafe { &*shared }.refcount.fetch_sub(1, Ordering::Release) != 1 {
            return;
        }
        // Synchronise with every other handle's release before running the destructor.
        fence(Ordering::Acquire);
        // SAFETY: the refcount reached zero, so we hold the only reference.
        unsafe { Self::destroy(shared) }
    }

    /// Free a `Shared` whose last reference has been given up, along with its region.
    ///
    /// ## Safety
    ///
    /// `shared` must be live with a refcount of zero, and nothing may use it afterwards.
    #[inline(never)]
    unsafe fn destroy(shared: *mut Shared) {
        // SAFETY: the caller guarantees the pointer is live and unreferenced.
        let (layout, allocator) = match unsafe { &(*shared).release } {
            // An embedded header lives inside the block it describes, so read out what freeing
            // the block needs first. Nothing else in `Shared` has a destructor.
            // SAFETY: the header is never touched again, so the handle is moved out, not copied.
            Release::Embedded { layout, allocator } => {
                (*layout, unsafe { std::ptr::read(allocator) })
            }
            // SAFETY: a boxed header is freed along with its region by `Drop`.
            _ => return unsafe { drop(Box::from_raw(shared)) },
        };
        // SAFETY: `shared` is the start of a live block from `allocator` with `layout`, and the
        // refcount reached zero, so no handle survives.
        unsafe { allocator.deallocate(NonNull::new_unchecked(shared.cast::<u8>()), layout) }
    }

    /// Whether this is the only handle to the region.
    #[inline]
    pub(crate) fn is_unique(&self) -> bool {
        self.refcount.load(Ordering::Acquire) == 1
    }

    /// The layout this region was allocated with, if we allocated it ourselves. For an embedded
    /// header this is the layout of the whole block, header included.
    #[inline]
    pub(crate) fn allocated_layout(&self) -> Option<Layout> {
        match &self.release {
            Release::Allocated { layout, .. } | Release::Embedded { layout, .. } => Some(*layout),
            Release::Owner { .. } => None,
        }
    }

    /// The layout of the region, if the global allocator produced it and this is the only handle
    /// to it: the two conditions for handing it out as a `Vec`.
    #[inline(never)]
    pub(crate) fn sole_global_layout(&self) -> Option<Layout> {
        match &self.release {
            Release::Allocated { layout, allocator }
                if allocator.is_statically_allocated() && self.is_unique() =>
            {
                Some(*layout)
            }
            _ => None,
        }
    }

    /// The allocator this region came from. Adopted regions report the global allocator, which is
    /// what any buffer derived from them should allocate with.
    #[inline]
    pub(crate) fn allocator(&self) -> &BufferAllocatorRef {
        match &self.release {
            Release::Allocated { allocator, .. } | Release::Embedded { allocator, .. } => allocator,
            Release::Owner { .. } => BufferAllocatorRef::static_ref(),
        }
    }

    /// The address one past the last byte of the region.
    #[inline]
    pub(crate) fn end_addr(&self) -> usize {
        self.base.as_ptr().addr() + self.size
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        match &self.release {
            Release::Allocated { layout, allocator } => {
                // An empty region was never allocated; see `UniqueBytes::allocate`.
                if layout.size() == 0 {
                    return;
                }
                // SAFETY: `base` and `layout` are always kept in step with the allocator call that
                // produced them, so this frees the region with exactly the layout it was
                // allocated with. The refcount reached zero, so no handle survives.
                unsafe { allocator.deallocate(self.base, *layout) }
            }
            Release::Embedded { layout, allocator } => {
                // A header in its own block is freed by `destroy`, which never runs this. It only
                // runs for a header that was moved out of its block, whose region still starts
                // `HEADER` bytes into it.
                // SAFETY: `base` was derived as `block.add(HEADER)`, so this is the block, which
                // was allocated from `allocator` with `layout` and is no longer referenced.
                unsafe { allocator.deallocate(self.base.sub(HEADER), *layout) }
            }
            Release::Owner { owner, drop, .. } => {
                // SAFETY: the owner is a live leaked box that only we may drop, and the refcount
                // reached zero.
                unsafe { drop(*owner) }
            }
        }
    }
}

// -------------------------------------------------------------------------------------------
// Allocation policy
// -------------------------------------------------------------------------------------------

/// The largest alignment the global allocator provides without taking an aligned-allocation path.
///
/// This mirrors the `MIN_ALIGN` table in `std`'s `System` allocator: requests at or below it (and
/// no larger than the size) go straight to `malloc`; anything else goes through `posix_memalign`
/// or its equivalent, which is markedly slower.
pub(crate) const FREE_ALIGN: usize = if usize::BITS >= 64 { 16 } else { 8 };

/// The raw layout to request so that an `alignment`-aligned window of `size` bytes fits inside it.
///
/// Rather than ask the allocator for `alignment` directly, we ask for what it provides for free
/// and pad the size by the largest shift that could then be needed to reach `alignment`. The
/// window starts at [`shift`] bytes into the region.
///
/// `size` must be non-zero.
#[inline]
pub(crate) fn shifted_layout(size: usize, alignment: Alignment) -> Layout {
    debug_assert!(size != 0);
    let alignment = alignment.as_usize();
    // `std` only takes the `malloc` path when the requested alignment does not exceed the size
    // either, so a tiny region requests less and pads a little more. Requesting the alignment
    // itself whenever that is free, rather than always 1, is what lets a `Buffer<T>` that asked
    // for no more than `align_of::<T>()` be handed out as a `Vec<T>` later.
    let free = if size >= FREE_ALIGN {
        FREE_ALIGN
    } else {
        1 << (usize::BITS - 1 - size.leading_zeros())
    };
    let requested = alignment.min(free);
    let padding = alignment - requested;
    let Some(total) = size.checked_add(padding) else {
        bytes_panic!("buffer of {size} bytes aligned to {alignment} exceeds the maximum layout");
    };
    Layout::from_size_align(total, requested).unwrap_or_else(|_| {
        bytes_panic!("buffer of {size} bytes aligned to {alignment} exceeds the maximum layout")
    })
}

/// How far into a region the first `alignment`-aligned byte lies.
///
/// This is computed from the address rather than with `align_offset`, which is permitted to give
/// up and return `usize::MAX`.
#[inline]
pub(crate) fn shift(base: NonNull<u8>, alignment: Alignment) -> usize {
    base.as_ptr().addr().wrapping_neg() & (alignment.as_usize() - 1)
}

/// Allocate a region able to hold an `alignment`-aligned window of `size` non-zero bytes.
///
/// Returns the region's base, the layout it was allocated with, and the shift to the window.
#[inline]
pub(crate) fn allocate_shifted(
    size: usize,
    alignment: Alignment,
    zeroed: bool,
    allocator: &BufferAllocatorRef,
) -> (NonNull<u8>, Layout, usize) {
    let layout = shifted_layout(size, alignment);
    let block = if zeroed {
        allocator.allocate_zeroed(layout)
    } else {
        allocator.allocate(layout)
    };
    let base = block
        .unwrap_or_else(|_| handle_alloc_error(layout))
        .cast::<u8>();
    let shift = shift(base, alignment);
    debug_assert!(shift + size <= layout.size());
    (base, layout, shift)
}

/// The bytes reserved at the front of a block for an embedded [`Shared`].
///
/// Rounded up to [`FREE_ALIGN`] so that the region behind the header is as aligned as the block
/// itself, which is what [`shifted_layout`]'s padding assumes.
pub(crate) const HEADER: usize = size_of::<Shared>().next_multiple_of(FREE_ALIGN);

/// The layout of a block holding an embedded [`Shared`] followed by a region able to hold an
/// `alignment`-aligned window of `size` non-zero bytes.
#[inline]
pub(crate) fn embedded_layout(size: usize, alignment: Alignment) -> Layout {
    let region = shifted_layout(size, alignment);
    let Some(total) = HEADER.checked_add(region.size()) else {
        bytes_panic!("buffer of {size} bytes aligned to {alignment} exceeds the maximum layout");
    };
    Layout::from_size_align(total, region.align().max(align_of::<Shared>())).unwrap_or_else(|_| {
        bytes_panic!("buffer of {size} bytes aligned to {alignment} exceeds the maximum layout")
    })
}

/// Box up the description of a region we allocated from the global allocator, or one too large to
/// describe inline, and return the state holding its `refcount` references.
///
/// Kept out of line so that the constructors it backs stay small enough to inline.
#[inline(never)]
pub(crate) fn shared_state(
    base: NonNull<u8>,
    layout: Layout,
    allocator: BufferAllocatorRef,
    refcount: usize,
) -> State {
    let shared = Shared {
        refcount: AtomicUsize::new(refcount),
        base,
        size: layout.size(),
        writable: true,
        release: Release::Allocated { layout, allocator },
    }
    .into_raw();
    // SAFETY: we just created `shared`, and the caller takes over its references.
    unsafe { State::shared(shared) }
}

/// Free the box behind a `Shared` without running its `Release`, once its region or owner has been
/// handed elsewhere.
///
/// ## Safety
///
/// `shared` must be a boxed `Shared` (never an embedded one) that nothing else references, and
/// nothing may use it afterwards.
#[inline(never)]
pub(crate) unsafe fn free_header(shared: *mut Shared) {
    // SAFETY: the caller guarantees the box is live and unreferenced.
    unsafe { drop(Box::from_raw(shared.cast::<ManuallyDrop<Shared>>())) }
}
