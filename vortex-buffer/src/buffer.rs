// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::any::type_name;
use std::cmp::Ordering;
use std::collections::Bound;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::Buf;
use bytes::Bytes;
use vortex_bytes::SharedBytes;
use vortex_error::VortexExpect;
use vortex_error::vortex_panic;

use crate::Alignment;
use crate::BufferAllocatorRef;
use crate::BufferMut;
use crate::ByteBuffer;
use crate::debug::TruncatedDebug;
use crate::trusted_len::TrustedLen;

/// An immutable buffer of items of `T`.
///
/// A `Buffer<T>` is a typed view over a [`SharedBytes`] window: the bytes, the alignment promise,
/// and the reference-counted region all live there, and this type only reinterprets them as
/// `T`s. Cloning it shares the region and slicing it is pointer arithmetic. The buffer promises
/// that its start is aligned to [`alignment`](Self::alignment), which is at least
/// `align_of::<T>()`.
///
/// Zero-sized element types are rejected at compile time when constructing a buffer.
///
/// ```compile_fail
/// use vortex_buffer::Buffer;
/// let _ = Buffer::<()>::empty();
/// ```
///
/// A `Buffer<T>` is `Send` only when `T` is `Send + Sync`. It is a *shared* handle - every clone
/// hands out `&[T]` - so sending one to another thread shares the elements exactly as an `Arc<T>`
/// would. With `T: Send` alone, two threads could write through the same `&[Cell<u8>]`:
///
/// ```compile_fail,E0277
/// use std::cell::Cell;
/// use vortex_buffer::Buffer;
///
/// let a: Buffer<Cell<u8>> = Buffer::copy_from(vec![Cell::new(0u8); 8]);
/// let b = a.clone();
/// std::thread::spawn(move || b[0].set(1));
/// a[0].set(2);
/// ```
pub struct Buffer<T> {
    /// The bytes of the elements, the region they live in, and the alignment promised for the
    /// first of them.
    pub(crate) bytes: SharedBytes,
    /// Carries `T`'s variance and auto traits. See the type docs for why this is `Arc<T>`.
    pub(crate) _marker: PhantomData<Arc<T>>,
}

impl<T> Clone for Buffer<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            bytes: self.bytes.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T> Default for Buffer<T> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<T: PartialEq> PartialEq for Buffer<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T: Eq> Eq for Buffer<T> {}

impl<T: Ord> Ord for Buffer<T> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

impl<T: PartialOrd> PartialOrd for Buffer<T> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_slice().partial_cmp(other.as_slice())
    }
}

impl<T: Hash> Hash for Buffer<T> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state)
    }
}

/// Copy a buffer's contents into a fresh `Vec`.
///
/// Kept out of line and marked cold so that the zero-copy paths in [`Buffer::into_vec`] and
/// [`BufferMut::into_vec`] do not have to reserve registers for a `memcpy` they will not run.
#[cold]
#[inline(never)]
pub(crate) fn copy_to_vec<T: Copy>(buffer: &[T]) -> Vec<T> {
    buffer.to_vec()
}

/// Copy a buffer's contents into a fresh, equally aligned [`BufferMut`] from the same allocator.
///
/// Cold for the same reason as [`copy_to_vec`]: it is the fallback [`Buffer::into_mut`] takes
/// only when the buffer is shared or read-only.
#[cold]
#[inline(never)]
fn copy_to_mut<T>(buffer: &Buffer<T>) -> BufferMut<T> {
    BufferMut::<T>::copy_from_aligned_in(buffer, buffer.alignment(), buffer.allocator().clone())
}

/// The number of `T`s that `bytes` bytes hold.
///
/// ## Panics
///
/// Panics if `bytes` is not a multiple of `size_of::<T>()`.
pub(crate) fn elements_in<T>(bytes: usize) -> usize {
    if !bytes.is_multiple_of(size_of::<T>()) {
        vortex_panic!(
            "Buffer length {} must be a multiple of the scalar type's size {}",
            bytes,
            size_of::<T>()
        );
    }
    bytes / size_of::<T>()
}

impl<T> Buffer<T> {
    /// View `bytes` as `T`s.
    ///
    /// `bytes` must hold a whole number of `T`s and promise at least `T`'s alignment; every
    /// constructor arranges for both.
    #[inline]
    pub(crate) fn from_shared(bytes: SharedBytes) -> Self {
        const { assert!(size_of::<T>() != 0, "zero-sized types are not supported") };
        debug_assert!(bytes.len().is_multiple_of(size_of::<T>()));
        debug_assert!(bytes.alignment().is_aligned_to(Alignment::of::<T>()));
        Self {
            bytes,
            _marker: PhantomData,
        }
    }

    /// Reject an alignment that `T` itself could not be stored at.
    #[inline]
    fn check_alignment(alignment: Alignment) {
        if !alignment.is_aligned_to(Alignment::of::<T>()) {
            vortex_panic!(
                "Alignment {} must align to the scalar type's alignment {}",
                alignment,
                Alignment::of::<T>(),
            );
        }
    }

    /// Returns a new `Buffer<T>` copied from the provided `Vec<T>`, `&[T]`, etc.
    ///
    /// To adopt a `Vec<T>` without copying, use [`from_vec`](Self::from_vec) or
    /// `Buffer::from(vec)`.
    pub fn copy_from(values: impl AsRef<[T]>) -> Self {
        BufferMut::copy_from(values).freeze()
    }

    /// Returns a new `Buffer<T>` copied with the provided allocator.
    pub fn copy_from_in(values: impl AsRef<[T]>, allocator: BufferAllocatorRef) -> Self {
        BufferMut::copy_from_in(values, allocator).freeze()
    }

    /// Returns a new `Buffer<T>` copied from the provided slice and with the requested alignment.
    ///
    /// The allocation is over-aligned to [`Alignment::DEFAULT_ALIGNMENT`] when that is larger than
    /// `alignment`. Use [`copy_from_preferred_aligned`] to control the over-alignment.
    ///
    /// [`copy_from_preferred_aligned`]: Self::copy_from_preferred_aligned
    pub fn copy_from_aligned(values: impl AsRef<[T]>, alignment: Alignment) -> Self {
        Self::copy_from_preferred_aligned(values, alignment, Some(Alignment::DEFAULT_ALIGNMENT))
    }

    /// Returns a new `Buffer<T>` copied from the provided slice and with the requested alignment.
    ///
    /// The buffer reports `alignment`, but the underlying allocation is over-aligned to the larger
    /// of `alignment` and `preferred_alignment`.
    pub fn copy_from_preferred_aligned(
        values: impl AsRef<[T]>,
        alignment: Alignment,
        preferred_alignment: Option<Alignment>,
    ) -> Self {
        BufferMut::copy_from_preferred_aligned(values, alignment, preferred_alignment).freeze()
    }

    /// Create a new zeroed `Buffer` with the given value.
    pub fn zeroed(len: usize) -> Self {
        Self::zeroed_aligned(len, Alignment::of::<T>())
    }

    /// Create a new zeroed `Buffer` with the provided allocator.
    pub fn zeroed_in(len: usize, allocator: BufferAllocatorRef) -> Self {
        BufferMut::zeroed_in(len, allocator).freeze()
    }

    /// Create a new zeroed `Buffer` with the requested alignment.
    ///
    /// The allocation is over-aligned to [`Alignment::DEFAULT_ALIGNMENT`] when that is larger than
    /// `alignment`. Use [`zeroed_preferred_aligned`] to control the over-alignment.
    ///
    /// [`zeroed_preferred_aligned`]: Self::zeroed_preferred_aligned
    pub fn zeroed_aligned(len: usize, alignment: Alignment) -> Self {
        Self::zeroed_preferred_aligned(len, alignment, Some(Alignment::DEFAULT_ALIGNMENT))
    }

    /// Create a new zeroed `Buffer` with the requested alignment.
    ///
    /// The buffer reports `alignment`, but the underlying allocation is over-aligned to the larger
    /// of `alignment` and `preferred_alignment`.
    pub fn zeroed_preferred_aligned(
        len: usize,
        alignment: Alignment,
        preferred_alignment: Option<Alignment>,
    ) -> Self {
        BufferMut::zeroed_preferred_aligned(len, alignment, preferred_alignment).freeze()
    }

    /// Create a new empty `Buffer` aligned to `T`.
    pub fn empty() -> Self {
        Self::empty_aligned(Alignment::of::<T>())
    }

    /// Create a new empty `Buffer` with the provided alignment.
    ///
    /// This does not allocate. Empty buffers use an aligned dangling pointer.
    pub fn empty_aligned(alignment: Alignment) -> Self {
        Self::check_alignment(alignment);
        Self::from_shared(SharedBytes::empty_aligned(alignment))
    }

    /// Create a new full `Buffer` with the given value.
    pub fn full(item: T, len: usize) -> Self
    where
        T: Copy,
    {
        BufferMut::full(item, len).freeze()
    }

    /// Create a full `Buffer` with the given value and allocator.
    pub fn full_in(item: T, len: usize, allocator: BufferAllocatorRef) -> Self
    where
        T: Copy,
    {
        BufferMut::full_in(item, len, allocator).freeze()
    }

    /// Take zero-copy ownership of a `Vec<T>`.
    ///
    /// For a `T` without a destructor the `Vec`'s allocation becomes the buffer's, and
    /// [`into_vec`](Self::into_vec) can hand it straight back out. A `T` with a destructor keeps
    /// the `Vec` alive as the buffer's owner instead, so that its elements are still dropped.
    pub fn from_vec(vec: Vec<T>) -> Self
    where
        T: Send + Sync + 'static,
    {
        if std::mem::needs_drop::<T>() {
            return Self::from_owner(vec);
        }
        BufferMut::from_vec(vec).freeze()
    }

    /// Take zero-copy ownership of memory kept alive by `owner`.
    ///
    /// The buffer's contents are whatever `owner` currently references, and `owner` is dropped
    /// once the last handle to the buffer goes away. This is how foreign allocations - an Arrow
    /// buffer, a memory map, a slab handed over an FFI boundary - enter Vortex without a copy.
    ///
    /// The memory is treated as read-only: [`try_into_mut`](Self::try_into_mut) will copy rather
    /// than write through a pointer we only ever had shared access to. Use
    /// [`BufferMut::from_owner`] when the owner can hand over exclusive, writable access.
    ///
    /// ## Example
    ///
    /// ```
    /// use std::sync::Arc;
    /// use vortex_buffer::Buffer;
    ///
    /// let shared: Arc<[i32]> = Arc::from(vec![1, 2, 3]);
    /// let buffer = Buffer::from_owner(shared.clone());
    /// assert_eq!(buffer.as_ptr(), shared.as_ptr(), "adoption is zero-copy");
    /// assert_eq!(buffer.as_slice(), &[1, 2, 3]);
    /// ```
    pub fn from_owner<O>(owner: O) -> Self
    where
        O: AsRef<[T]> + Send + 'static,
    {
        Self::from_shared(SharedBytes::from_owner::<O, T>(owner))
    }

    /// Borrow a `'static` slice without copying it.
    pub fn from_static(values: &'static [T]) -> Self {
        // SAFETY: any `[T]` is a valid `[u8]` of `size_of_val` bytes for the purposes of reading.
        let bytes = unsafe {
            std::slice::from_raw_parts(values.as_ptr().cast::<u8>(), size_of_val(values))
        };
        let mut bytes = SharedBytes::from_static(bytes);
        // A `[T]` is always aligned to `T`.
        bytes.ensure_aligned(Alignment::of::<T>());
        Self::from_shared(bytes)
    }

    /// Create a `Buffer<T>` zero-copy from a `ByteBuffer`.
    ///
    /// ## Panics
    ///
    /// Panics if the buffer is not aligned to the size of `T`, or the length is not a multiple of
    /// the size of `T`.
    pub fn from_byte_buffer(buffer: ByteBuffer) -> Self {
        // TODO(ngates): should this preserve the current alignment of the buffer?
        Self::from_byte_buffer_aligned(buffer, Alignment::of::<T>())
    }

    /// Create a `Buffer<T>` zero-copy from a `ByteBuffer`.
    ///
    /// ## Panics
    ///
    /// Panics if the buffer is not aligned to the given alignment, if the length is not a multiple
    /// of the size of `T`, or if the given alignment is not aligned to that of `T`.
    pub fn from_byte_buffer_aligned(buffer: ByteBuffer, alignment: Alignment) -> Self {
        Self::check_alignment(alignment);
        if !buffer.is_aligned(alignment) {
            vortex_panic!("Buffer must align to the requested alignment {}", alignment);
        }
        let mut bytes = buffer.bytes;
        elements_in::<T>(bytes.len());
        bytes.ensure_aligned(alignment);
        Self::from_shared(bytes)
    }

    /// Create a `Buffer<T>` zero-copy from a `Bytes`.
    ///
    /// ## Panics
    ///
    /// Panics if the buffer is not aligned to the size of `T`, or the length is not a multiple of
    /// the size of `T`.
    pub fn from_bytes_aligned(bytes: Bytes, alignment: Alignment) -> Self {
        Self::check_alignment(alignment);
        if !alignment.is_ptr_aligned(bytes.as_ptr()) {
            vortex_panic!(
                "Bytes alignment must align to the requested alignment {}",
                alignment,
            );
        }
        if bytes.is_empty() {
            return Self::empty_aligned(alignment);
        }
        let mut bytes = SharedBytes::from_owner::<Bytes, u8>(bytes);
        elements_in::<T>(bytes.len());
        bytes.ensure_aligned(alignment);
        Self::from_shared(bytes)
    }

    /// Create a buffer with values from the TrustedLen iterator.
    /// Should be preferred over `from_iter` when the iterator is known to be `TrustedLen`.
    pub fn from_trusted_len_iter<I: TrustedLen<Item = T>>(iter: I) -> Self {
        BufferMut::from_trusted_len_iter(iter).freeze()
    }

    /// Map each element of the buffer with a closure, reusing the buffer's allocation when this
    /// is its only handle.
    ///
    /// ## Panics
    ///
    /// Panics if `R` does not have the same size and alignment as `T`.
    pub fn map_each_in_place<R, F>(self, mut f: F) -> BufferMut<R>
    where
        T: Copy,
        R: Copy,
        F: FnMut(T) -> R,
    {
        // Assert here as well as in `BufferMut::map_each_in_place`, so that the contract does not
        // depend on which arm we take: only the in-place arm reuses `T`'s allocation, so without
        // this a mismatched `R` would panic or not depending on the buffer's refcount.
        assert_eq!(
            size_of::<T>(),
            size_of::<R>(),
            "Size of T and R do not match"
        );
        assert_eq!(
            align_of::<T>(),
            align_of::<R>(),
            "Alignment of T and R do not match"
        );
        match self.try_into_mut() {
            Ok(mut_buf) => mut_buf.map_each_in_place(f),
            Err(buf) => {
                let len = buf.len();
                let allocator = buf.allocator().clone();
                let mut out_buf = BufferMut::with_capacity_in(len, allocator);
                out_buf
                    .spare_capacity_mut()
                    .iter_mut()
                    .zip(buf)
                    .for_each(|(out, in_)| {
                        out.write(f(in_));
                    });
                // SAFETY: every one of the `len` slots was just written.
                unsafe { out_buf.set_len(len) }
                out_buf
            }
        }
    }

    /// Clear the buffer.
    pub fn clear(&mut self) {
        self.bytes.clear();
    }

    /// Returns the length of the buffer in elements of type T.
    #[inline]
    pub fn len(&self) -> usize {
        self.bytes.len() / size_of::<T>()
    }

    /// Returns whether the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Returns the alignment of the buffer.
    #[inline]
    pub fn alignment(&self) -> Alignment {
        self.bytes.alignment()
    }

    /// Returns the allocator to use for derived buffers.
    ///
    /// Adopted and borrowed buffers report the static allocator.
    #[inline]
    pub fn allocator(&self) -> &BufferAllocatorRef {
        self.bytes.allocator()
    }

    /// Returns a raw pointer to the buffer's data.
    #[inline]
    pub fn as_ptr(&self) -> *const T {
        self.bytes.as_ptr().cast::<T>()
    }

    /// Returns a slice over the buffer of elements of type T.
    #[inline]
    pub fn as_slice(&self) -> &[T] {
        // SAFETY: the bytes hold `len()` initialised `T`s, and the pointer is aligned for `T` by
        // construction.
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len()) }
    }

    /// Return a view over the buffer as an opaque byte slice.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    /// Returns an iterator over the buffer of elements of type T.
    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            inner: self.as_slice().iter(),
        }
    }

    /// Returns a slice of self for the provided range.
    ///
    /// # Panics
    ///
    /// Requires that `begin <= end` and `end <= self.len()`.
    /// Also requires that both `begin` and `end` are aligned to the buffer's required alignment.
    #[inline]
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        self.slice_with_alignment(range, self.alignment())
    }

    /// Returns a slice of self for the provided range, with no guarantees about the resulting
    /// alignment.
    ///
    /// # Panics
    ///
    /// Requires that `begin <= end` and `end <= self.len()`.
    #[inline]
    pub fn slice_unaligned(&self, range: impl RangeBounds<usize>) -> Self {
        self.slice_with_alignment(range, Alignment::of::<u8>())
    }

    /// Returns a slice of self for the provided range, ensuring the resulting slice has the
    /// given alignment.
    ///
    /// # Panics
    ///
    /// Requires that `begin <= end` and `end <= self.len()`.
    /// Also requires that both `begin` and `end` are aligned to the given alignment.
    pub fn slice_with_alignment(
        &self,
        range: impl RangeBounds<usize>,
        alignment: Alignment,
    ) -> Self {
        let len = self.len();
        let begin = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n.checked_add(1).vortex_expect("out of range"),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&n) => n.checked_add(1).vortex_expect("out of range"),
            Bound::Excluded(&n) => n,
            Bound::Unbounded => len,
        };

        if begin > end {
            vortex_panic!(
                "range start must not be greater than end: {:?} <= {:?}",
                begin,
                end
            );
        }
        if end > len {
            vortex_panic!("range end out of bounds: {:?} > {:?}", end, len);
        }
        if !alignment.is_aligned_to(Alignment::of::<T>()) {
            vortex_panic!("Slice alignment must at least align to type T")
        }

        if end == begin {
            // We prefer to return a new empty buffer instead of sharing this one and creating a
            // strong reference just to hold an empty slice.
            return Self::empty_aligned(alignment);
        }

        let begin_byte = begin * size_of::<T>();
        let end_byte = end * size_of::<T>();
        if !alignment.is_offset_aligned(begin_byte) {
            vortex_panic!(
                "range start must be aligned to {alignment:?}, byte {}",
                begin_byte
            );
        }
        Self::from_shared(self.bytes.slice_aligned(begin_byte, end_byte, alignment))
    }

    /// Returns a slice of self that is equivalent to the given subset.
    ///
    /// When processing the buffer you will often end up with `&[T]` that is a subset
    /// of the underlying buffer. This function turns the slice into a slice of the buffer
    /// it has been taken from.
    ///
    /// # Panics:
    /// Requires that the given sub slice is in fact contained within the Bytes buffer; otherwise this function will panic.
    #[inline]
    pub fn slice_ref(&self, subset: &[T]) -> Self {
        self.slice_ref_with_alignment(subset, Alignment::of::<T>())
    }

    /// Returns a slice of self that is equivalent to the given subset.
    ///
    /// When processing the buffer you will often end up with `&[T]` that is a subset
    /// of the underlying buffer. This function turns the slice into a slice of the buffer
    /// it has been taken from.
    ///
    /// # Panics:
    /// Requires that the given sub slice is in fact contained within the Bytes buffer; otherwise this function will panic.
    /// Also requires that the given alignment aligns to the type of slice and is smaller or equal to the buffers alignment
    pub fn slice_ref_with_alignment(&self, subset: &[T], alignment: Alignment) -> Self {
        if !alignment.is_aligned_to(Alignment::of::<T>()) {
            vortex_panic!("slice_ref alignment must at least align to type T")
        }

        if !self.alignment().is_aligned_to(alignment) {
            vortex_panic!("slice_ref subset alignment must at least align to the buffer alignment")
        }

        if !alignment.is_ptr_aligned(subset.as_ptr()) {
            vortex_panic!("slice_ref subset must be aligned to {:?}", alignment);
        }

        // SAFETY: any `[T]` is a valid `[u8]` of `size_of_val` bytes for the purposes of reading.
        let subset_bytes = unsafe {
            std::slice::from_raw_parts(subset.as_ptr().cast::<u8>(), size_of_val(subset))
        };
        Self::from_shared(self.bytes.slice_ref_aligned(subset_bytes, alignment))
    }

    /// Returns the underlying bytes without copying.
    ///
    /// A buffer that was adopted from a `Bytes` hands the original back, sliced to this window if
    /// need be. Anything else is wrapped in a `Bytes` that keeps the buffer's region alive.
    pub fn into_bytes(self) -> Bytes {
        let offset = self.bytes.offset_in_region();
        let length = self.bytes.len();
        let bytes = match self.bytes.try_into_owner::<Bytes>() {
            Ok(owner) => {
                return if offset == 0 && length == owner.len() {
                    owner
                } else {
                    owner.slice(offset..offset + length)
                };
            }
            Err(bytes) => bytes,
        };
        if let Some(owner) = bytes.owner::<Bytes>() {
            return owner.slice(offset..offset + length);
        }
        if bytes.is_empty() {
            return Bytes::new();
        }
        Bytes::from_owner(bytes)
    }

    /// Return the ByteBuffer for this `Buffer<T>`.
    ///
    /// The byte buffer keeps this buffer's alignment.
    pub fn into_byte_buffer(self) -> ByteBuffer {
        ByteBuffer::from_shared(self.bytes)
    }

    /// Try to convert self into `BufferMut<T>` if there is only a single strong reference.
    ///
    /// This succeeds for buffers built over foreign memory too - a `Vec<T>` adopted with
    /// [`from_vec`](Self::from_vec), or any owner handed to [`BufferMut::from_owner`] - as long
    /// as nothing else holds a reference to it. Memory adopted read-only through
    /// [`from_owner`](Self::from_owner) is never made mutable.
    ///
    /// The recovered capacity runs from the start of this buffer to the end of its allocation, so
    /// a buffer that is a slice of a larger region regains the rest of it.
    pub fn try_into_mut(self) -> Result<BufferMut<T>, Self> {
        self.bytes
            .try_into_unique()
            .map(BufferMut::from_unique)
            .map_err(Self::from_shared)
    }

    /// Convert self into `BufferMut<T>`, cloning the data if there are multiple strong references.
    pub fn into_mut(self) -> BufferMut<T> {
        self.try_into_mut()
            .unwrap_or_else(|buffer| copy_to_mut(&buffer))
    }

    /// Convert the buffer into a `Vec<T>`, without copying where possible.
    ///
    /// See [`BufferMut::into_vec`] for when this is zero-copy; in addition, this buffer must be
    /// the only handle to its allocation.
    pub fn into_vec(self) -> Vec<T>
    where
        T: Copy,
    {
        match self.try_into_vec() {
            Ok(vec) => vec,
            Err(buffer) => copy_to_vec(buffer.as_slice()),
        }
    }

    /// Convert the buffer into a `Vec<T>` without copying, or give it back.
    ///
    /// See [`into_vec`](Self::into_vec).
    pub fn try_into_vec(self) -> Result<Vec<T>, Self> {
        self.bytes.try_into_vec::<T>().map_err(Self::from_shared)
    }

    /// Returns whether this is the only handle to the buffer's allocation.
    ///
    /// When this is true, [`try_into_mut`](Self::try_into_mut) succeeds for any buffer over
    /// writable memory, and [`try_into_vec`](Self::try_into_vec) succeeds for any buffer that
    /// starts at the front of a global allocation made with *exactly* `align_of::<T>()`.
    pub fn is_unique(&self) -> bool {
        self.bytes.is_unique()
    }

    /// Returns whether a `Buffer<T>` is aligned to the given alignment.
    pub fn is_aligned(&self, alignment: Alignment) -> bool {
        self.bytes.is_aligned(alignment)
    }

    /// Return a `Buffer<T>` with the given alignment. Where possible, this will be zero-copy.
    ///
    /// ## Panics
    ///
    /// Panics when the requested alignment isn't itself aligned to type T.
    pub fn aligned(mut self, alignment: Alignment) -> Self {
        Self::check_alignment(alignment);
        if self.bytes.is_aligned(alignment) {
            self.bytes.ensure_aligned(alignment);
            self
        } else {
            #[cfg(feature = "warn-copy")]
            {
                let bt = std::backtrace::Backtrace::capture();
                tracing::warn!(
                    "Buffer is not aligned to requested alignment {alignment}, copying: {bt}"
                )
            }
            let allocator = self.allocator().clone();
            BufferMut::copy_from_aligned_in(self, alignment, allocator).freeze()
        }
    }

    /// Return a `Buffer<T>` with the given alignment. Panics if the buffer is not aligned.
    pub fn ensure_aligned(mut self, alignment: Alignment) -> Self {
        Self::check_alignment(alignment);
        if !self.bytes.is_aligned(alignment) {
            vortex_panic!("Buffer is not aligned to requested alignment {}", alignment)
        }
        self.bytes.ensure_aligned(alignment);
        self
    }

    /// Transmute a `Buffer<T>` into a `Buffer<U>`.
    ///
    /// # Safety
    ///
    /// The caller must ensure that all possible bit representations of type `T` are valid when
    /// interpreted as type `U`.
    /// See [`std::mem::transmute`] for more details.
    ///
    /// # Panics
    ///
    /// Panics if the type `U` does not have the same size and alignment as `T`.
    pub unsafe fn transmute<U>(self) -> Buffer<U> {
        assert_eq!(size_of::<T>(), size_of::<U>(), "Buffer type size mismatch");
        assert_eq!(
            align_of::<T>(),
            align_of::<U>(),
            "Buffer type alignment mismatch"
        );
        Buffer::<U>::from_shared(self.bytes)
    }
}

/// An iterator over Buffer elements.
///
/// This is an analog to the `std::slice::Iter` type.
pub struct Iter<'a, T> {
    inner: std::slice::Iter<'a, T>,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }

    #[inline]
    fn count(self) -> usize {
        self.inner.count()
    }

    #[inline]
    fn last(self) -> Option<Self::Item> {
        self.inner.last()
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.inner.nth(n)
    }
}

impl<T> ExactSizeIterator for Iter<'_, T> {
    #[inline]
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<T: Debug> Debug for Buffer<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(&format!("Buffer<{}>", type_name::<T>()))
            .field("length", &self.len())
            .field("alignment", &self.alignment())
            .field("as_slice", &TruncatedDebug(self.as_slice()))
            .finish()
    }
}

impl<T> Deref for Buffer<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T> AsRef<[T]> for Buffer<T> {
    #[inline]
    fn as_ref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T> FromIterator<T> for Buffer<T> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        BufferMut::from_iter(iter).freeze()
    }
}

impl<T> From<Vec<T>> for Buffer<T>
where
    T: Send + Sync + 'static,
{
    fn from(value: Vec<T>) -> Self {
        Self::from_vec(value)
    }
}

impl From<Bytes> for ByteBuffer {
    fn from(bytes: Bytes) -> Self {
        Self::from_bytes_aligned(bytes, Alignment::of::<u8>())
    }
}

impl From<SharedBytes> for ByteBuffer {
    /// Bytes need no reinterpretation, so this is free and keeps the window's alignment.
    #[inline]
    fn from(bytes: SharedBytes) -> Self {
        Self::from_shared(bytes)
    }
}

impl From<ByteBuffer> for SharedBytes {
    #[inline]
    fn from(buffer: ByteBuffer) -> Self {
        buffer.bytes
    }
}

impl Buf for ByteBuffer {
    #[inline]
    fn remaining(&self) -> usize {
        self.len()
    }

    #[inline]
    fn chunk(&self) -> &[u8] {
        self.as_slice()
    }

    #[inline]
    fn advance(&mut self, cnt: usize) {
        self.bytes.advance(cnt);
    }
}

/// Owned iterator over a [`Buffer`].
pub struct BufferIterator<T: Copy> {
    // Keep the buffer alive for the duration of the iteration.
    _buffer: Buffer<T>,
    ptr: *const T,
    end: *const T,
}

// SAFETY: `BufferIterator` is a `Buffer<T>` plus two cursors into it, so it can be sent or shared
// exactly when `Buffer<T>` can.
unsafe impl<T: Copy + Send + Sync> Send for BufferIterator<T> {}
// SAFETY: see above.
unsafe impl<T: Copy + Send + Sync> Sync for BufferIterator<T> {}

impl<T: Copy> Iterator for BufferIterator<T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.ptr == self.end {
            return None;
        }
        // SAFETY: `ptr..end` are initialised elements kept alive by `_buffer`.
        let value = unsafe { self.ptr.read() };
        self.ptr = unsafe { self.ptr.add(1) };
        Some(value)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        // SAFETY: both pointers lie within the same buffer, with `ptr <= end`.
        let remaining = unsafe { self.end.offset_from_unsigned(self.ptr) };
        (remaining, Some(remaining))
    }
}

impl<T: Copy> ExactSizeIterator for BufferIterator<T> {}

impl<T: Copy> IntoIterator for Buffer<T> {
    type Item = T;
    type IntoIter = BufferIterator<T>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        let ptr = self.as_ptr();
        // SAFETY: `len()` elements start at `ptr`, so one past the last is still in bounds.
        let end = unsafe { ptr.add(self.len()) };
        BufferIterator {
            _buffer: self,
            ptr,
            end,
        }
    }
}

impl<T> From<BufferMut<T>> for Buffer<T> {
    #[inline]
    fn from(value: BufferMut<T>) -> Self {
        value.freeze()
    }
}

#[cfg(test)]
mod tests;
