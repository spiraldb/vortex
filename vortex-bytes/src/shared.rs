// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::any::TypeId;
use std::cmp::Ordering;
use std::hash::Hash;
use std::hash::Hasher;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering as AtomicOrdering;

use crate::BufferAllocatorRef;
use crate::Release;
use crate::Shared;
use crate::State;
use crate::UniqueBytes;
use crate::dangling;
use crate::drop_owner;
use crate::free_header;
use crate::panic::bytes_panic;
use crate::shared_state;

/// An immutable, reference-counted window into a region.
///
/// This is the storage behind `vortex-buffer`'s `Buffer<T>`. Cloning and slicing are `O(1)`.
///
/// A handle that has never been shared describes its region inline (see the crate docs for the
/// state encoding) and allocates no refcount; the first [`clone`](Clone::clone) promotes it.
pub struct SharedBytes {
    /// The first byte of the window.
    ptr: NonNull<u8>,
    /// The length of the window in bytes.
    len: usize,
    /// The first byte of the region. Promotion never rewrites this, which is what lets it happen
    /// behind a shared reference.
    base: NonNull<u8>,
    /// The ownership state. Written only by promotion, which is why it is atomic. Holding a
    /// pointer rather than a `usize` is what keeps the `SHARED` case's provenance.
    state: AtomicPtr<()>,
}

// SAFETY: `Shared` is `Send`/`Sync`, and a `SharedBytes` only ever hands out `&[u8]` into a region
// that no live handle may write to (`try_into_unique` requires unique ownership).
unsafe impl Send for SharedBytes {}
// SAFETY: see above. The state word is only ever mutated through atomics.
unsafe impl Sync for SharedBytes {}

impl SharedBytes {
    #[inline]
    fn state(&self) -> State {
        State(self.state.load(AtomicOrdering::Acquire))
    }

    /// An empty window that owns nothing, aligned to [`Alignment::MAX`](crate::Alignment::MAX).
    #[inline]
    pub fn empty() -> Self {
        Self {
            ptr: dangling(),
            len: 0,
            base: dangling(),
            state: AtomicPtr::new(State::STATIC.0),
        }
    }

    /// Borrow a `'static` slice without copying it.
    #[inline]
    pub fn from_static(slice: &'static [u8]) -> Self {
        if slice.is_empty() {
            return Self::empty();
        }
        let ptr = NonNull::from(slice).cast();
        Self {
            ptr,
            len: slice.len(),
            base: ptr,
            state: AtomicPtr::new(State::STATIC.0),
        }
    }

    /// Adopt a region kept alive by `owner`, without copying it.
    ///
    /// The window covers exactly the bytes `owner` currently references. The region is recorded as
    /// read-only, so [`try_into_unique`](Self::try_into_unique) will refuse it: the pointer is
    /// derived from a shared reference, and writing through such a pointer is undefined behaviour
    /// even when the memory itself is writable.
    ///
    /// The owner can be had back through [`owner`](Self::owner) and
    /// [`try_into_owner`](Self::try_into_owner), which is what keeps round trips through foreign
    /// buffer types zero-copy.
    ///
    /// Foreign memory is always refcounted: adoption already allocates a box for the owner, so
    /// there is nothing to be gained by describing it inline.
    pub fn from_owner<O, T>(owner: O) -> Self
    where
        O: AsRef<[T]> + Send + 'static,
    {
        // Leak the box before reading the slice out of it, so that the pointer we keep is derived
        // from a raw pointer that nothing reborrows again.
        let owner: *mut O = Box::into_raw(Box::new(owner));
        // SAFETY: we have just created `owner` and nothing else can free it.
        let slice: &[T] = unsafe { &*owner }.as_ref();
        let size = size_of_val(slice);
        // An empty window never dereferences its pointer, so prefer the maximally aligned dangling
        // address over the owner's, which may be aligned to nothing in particular. The owner is
        // kept alive either way: its `Drop` may release resources the caller expects us to hold.
        let base = if size == 0 {
            dangling()
        } else {
            NonNull::from(slice).cast::<u8>()
        };

        let shared = Shared {
            refcount: AtomicUsize::new(1),
            base,
            size,
            // Read-only: `base` is derived from a shared reference.
            writable: false,
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
            base,
            // SAFETY: we just created `shared` and take over its single reference.
            state: AtomicPtr::new(unsafe { State::shared(shared) }.0),
        }
    }

    /// Construct from a window into a region.
    ///
    /// ## Safety
    ///
    /// `ptr..ptr + len` must lie within the region `state` describes, `base` must be its first
    /// byte, and the caller must hand over one reference to it.
    #[inline]
    pub(crate) unsafe fn from_parts(
        ptr: NonNull<u8>,
        len: usize,
        base: NonNull<u8>,
        state: State,
    ) -> Self {
        Self {
            ptr,
            len,
            base,
            state: AtomicPtr::new(state.0),
        }
    }

    /// The address of the first byte of the window.
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Whether the window covers no bytes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The number of bytes in the window.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// The window's bytes.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: the window is a valid, initialised, immutable region of `len` bytes. When `len`
        // is zero `ptr` may dangle, which `from_raw_parts` permits so long as it is aligned and
        // non-null.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    /// How far into its region the window starts.
    #[inline]
    pub fn offset_in_region(&self) -> usize {
        self.ptr.as_ptr().addr() - self.base.as_ptr().addr()
    }

    /// The allocator the region came from, and so the one to allocate any derived region with.
    ///
    /// Regions that were adopted or borrowed rather than allocated report the global allocator.
    #[inline]
    pub fn allocator(&self) -> &BufferAllocatorRef {
        let state = self.state();
        if state.is_shared() {
            // SAFETY: we hold a reference to the `Shared`, so it is live.
            unsafe { &*state.as_shared() }.allocator()
        } else {
            BufferAllocatorRef::static_ref()
        }
    }

    /// The value this window's region was adopted from, if it was adopted from an `O`.
    ///
    /// Only read-only adoptions ([`from_owner`](Self::from_owner)) are visible this way: handing
    /// out a reference to the owner of a writable region would alias its bytes.
    pub fn owner<O: 'static>(&self) -> Option<&O> {
        let state = self.state();
        if !state.is_shared() {
            return None;
        }
        // SAFETY: we hold a reference to the `Shared`, so it is live.
        let shared = unsafe { &*state.as_shared() };
        match &shared.release {
            Release::Owner { owner, type_id, .. }
                if !shared.writable && *type_id == TypeId::of::<O>() =>
            {
                // SAFETY: the owner is a live leaked `Box<O>` - the `TypeId` says so - that lives
                // as long as the `Shared`, which `self` keeps alive. The region was derived from
                // a shared reference to it, so another shared reference is fine.
                Some(unsafe { &*owner.cast::<O>() })
            }
            _ => None,
        }
    }

    /// Take the value this window's region was adopted from back out, if this is the only handle
    /// to it and it is an `O`.
    ///
    /// The window itself is not consulted: a caller that needs the whole owner should check that
    /// the window covers it first, and one that only needs part of it can slice the owner instead.
    pub fn try_into_owner<O: 'static>(self) -> Result<O, Self> {
        let state = self.state();
        if !state.is_shared() {
            return Err(self);
        }
        // SAFETY: we hold a reference to the `Shared`, so it is live.
        let shared = unsafe { &*state.as_shared() };
        let Release::Owner { owner, type_id, .. } = &shared.release else {
            return Err(self);
        };
        if *type_id != TypeId::of::<O>() || !shared.is_unique() {
            return Err(self);
        }
        let owner = owner.cast::<O>();

        // This handle must not release the region: the owner is being handed out, not dropped.
        let _this = ManuallyDrop::new(self);
        // SAFETY: the refcount is one, so this handle holds the only reference to the box, which
        // is never embedded for an owner, and we free it without running its `Release`.
        unsafe { free_header(state.as_shared()) };

        // SAFETY: `owner` is the leaked `Box<O>` from `from_owner`, and nothing else can reach it
        // now that the `Shared` is gone.
        Ok(*unsafe { Box::from_raw(owner) })
    }

    /// Promote an inline-described region to a refcounted one, so a second handle can exist.
    ///
    /// Returns the `Shared` with an extra reference already taken for the caller's new handle.
    #[cold]
    fn promote(&self, state: State) -> *mut Shared {
        debug_assert!(state.is_owned());
        // Two references: this handle, and the one the caller is about to create.
        let new = shared_state(
            self.base,
            state.owned_layout(),
            BufferAllocatorRef::statically_allocated(),
            2,
        );
        // SAFETY: `shared_state` returns a `SHARED` word.
        let shared = unsafe { new.as_shared() };
        match self.state.compare_exchange(
            state.0,
            new.0,
            AtomicOrdering::AcqRel,
            AtomicOrdering::Acquire,
        ) {
            Ok(_) => shared,
            Err(actual) => {
                // Another thread promoted this handle first. Throw ours away without releasing
                // the region - the winner owns it now - and take a reference to theirs instead.
                // SAFETY: nothing ever saw this boxed `Shared`, and its `Release` has not run.
                unsafe { free_header(shared) };

                let actual = State(actual);
                debug_assert!(!actual.is_owned(), "promotion only ever happens once");
                // SAFETY: the winner published a live `Shared` and holds a reference to it, so it
                // cannot have been freed while we hold a handle of our own.
                let shared = unsafe { actual.as_shared() };
                // SAFETY: as above; we take the reference for the caller's new handle.
                unsafe { Shared::retain(shared) };
                shared
            }
        }
    }

    /// Returns a new handle to `self[begin..end]`.
    ///
    /// ## Panics
    ///
    /// Panics if the range is out of bounds.
    #[inline]
    pub fn slice(&self, begin: usize, end: usize) -> Self {
        if begin > end {
            bytes_panic!("range start must not be greater than end: {begin} <= {end}");
        }
        if end > self.len {
            bytes_panic!("range end out of bounds: {end} > {}", self.len);
        }
        if begin == end {
            return Self::empty();
        }
        // SAFETY: `begin < len`, so the offset stays inside the window.
        let ptr = unsafe { self.ptr.add(begin) };
        let mut sliced = self.clone();
        sliced.ptr = ptr;
        sliced.len = end - begin;
        sliced
    }

    /// Returns a new handle to `subset`, which must be contained within this window.
    ///
    /// ## Panics
    ///
    /// Panics if `subset` is not contained within this window.
    #[inline]
    pub fn slice_ref(&self, subset: &[u8]) -> Self {
        // An empty subset carries no address we can meaningfully check against.
        if subset.is_empty() {
            return Self::empty();
        }

        let self_start = self.ptr.as_ptr().addr();
        let sub_start = subset.as_ptr().addr();

        if sub_start < self_start {
            bytes_panic!("subset pointer starts before the buffer");
        }
        if sub_start + subset.len() > self_start + self.len {
            bytes_panic!("subset pointer ends past the end of the buffer");
        }

        let offset = sub_start - self_start;
        self.slice(offset, offset + subset.len())
    }

    /// Advance the start of the window by `cnt` bytes.
    ///
    /// This does not preserve alignment; see [`UniqueBytes::advance`].
    ///
    /// ## Panics
    ///
    /// Panics if `cnt > len`.
    #[inline]
    pub fn advance(&mut self, cnt: usize) {
        if cnt > self.len {
            bytes_panic!(
                "cannot advance past the end of the buffer: {cnt} > {}",
                self.len
            );
        }
        // SAFETY: `cnt <= len`, so the new start stays inside (or exactly at the end of) the
        // window. The region's start is tracked separately, so this cannot lose it.
        self.ptr = unsafe { self.ptr.add(cnt) };
        self.len -= cnt;
    }

    /// Shorten the window to zero bytes, keeping the start.
    #[inline]
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Whether this is the only handle to the underlying region.
    #[inline]
    pub fn is_unique(&self) -> bool {
        let state = self.state();
        if state.is_owned() {
            // Never shared, so there is nothing else to hold it.
            return true;
        }
        if state.is_static() {
            // A zero-length window is trivially exclusive, but a borrowed `'static` slice carries
            // no refcount, so we cannot claim to be its only handle.
            return self.len == 0;
        }
        // SAFETY: we hold a reference to the `Shared`, so it is live.
        unsafe { &*state.as_shared() }.is_unique()
    }

    /// Take exclusive ownership of the region, if this is the only handle to writable memory.
    ///
    /// The returned buffer's capacity runs from the start of this window to the end of the region,
    /// so a handle onto the front of a partially filled allocation regains its spare capacity.
    #[inline]
    pub fn try_into_unique(self) -> Result<UniqueBytes, Self> {
        let state = self.state();

        if state.is_static() {
            return if self.len == 0 {
                Ok(UniqueBytes::empty())
            } else {
                Err(self)
            };
        }

        if state.is_owned() {
            // Never shared: hand the inline description straight over, no atomics involved.
            let capacity =
                self.base.as_ptr().addr() + state.owned_size() - self.ptr.as_ptr().addr();
            let this = ManuallyDrop::new(self);
            // SAFETY: we hold the only handle, the window lies in the region, and a region we
            // allocated ourselves is always writable. `this` will not release it.
            return Ok(unsafe {
                UniqueBytes::from_parts(this.ptr, this.len, capacity, this.base, state)
            });
        }

        // SAFETY: we hold a reference to the `Shared`, so it is live.
        let shared = unsafe { &*state.as_shared() };
        if !shared.writable || !shared.is_unique() {
            return Err(self);
        }
        let capacity = shared.end_addr() - self.ptr.as_ptr().addr();
        let base = shared.base;
        let this = ManuallyDrop::new(self);
        // SAFETY: the refcount is one, so we hold the only handle and take over its reference.
        Ok(unsafe { UniqueBytes::from_parts(this.ptr, this.len, capacity, base, state) })
    }

    /// Hand the region out as a `Vec<T>`, if this is the only handle to it and it is exactly a
    /// `Vec<T>`'s allocation. See [`UniqueBytes::try_into_vec`] for what that takes.
    #[inline]
    pub fn try_into_vec<T>(self) -> Result<Vec<T>, Self> {
        self.try_into_unique()?
            .try_into_vec::<T>()
            .map_err(UniqueBytes::freeze)
    }
}

impl Clone for SharedBytes {
    #[inline]
    fn clone(&self) -> Self {
        let state = self.state();
        let state = if state.is_static() {
            state
        } else if state.is_owned() {
            // SAFETY: `promote` returns a `Shared` with a reference already taken for us.
            unsafe { State::shared(self.promote(state)) }
        } else {
            // SAFETY: we hold a reference to the `Shared`, so it is live.
            unsafe { Shared::retain(state.as_shared()) };
            state
        };

        Self {
            ptr: self.ptr,
            len: self.len,
            base: self.base,
            state: AtomicPtr::new(state.0),
        }
    }
}

impl Drop for SharedBytes {
    #[inline]
    fn drop(&mut self) {
        let state = State(*self.state.get_mut());
        if state.is_static() {
            return;
        }
        if state.is_owned() {
            // SAFETY: we hold the only handle to a global-allocator region we allocated with
            // exactly this layout.
            unsafe { std::alloc::dealloc(self.base.as_ptr(), state.owned_layout()) };
            return;
        }
        // SAFETY: we hold one reference to a live `Shared`, and give it up here.
        unsafe { Shared::release(state.as_shared()) };
    }
}

impl std::fmt::Debug for SharedBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state();
        f.debug_struct("SharedBytes")
            .field("ptr", &self.ptr)
            .field("len", &self.len)
            .field(
                "state",
                &if state.is_static() {
                    "static"
                } else if state.is_owned() {
                    "owned"
                } else {
                    "shared"
                },
            )
            .finish()
    }
}

impl AsRef<[u8]> for SharedBytes {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl PartialEq for SharedBytes {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for SharedBytes {}

impl PartialOrd for SharedBytes {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SharedBytes {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

impl Hash for SharedBytes {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state)
    }
}
