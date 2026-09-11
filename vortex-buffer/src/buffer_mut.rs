// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use core::mem::MaybeUninit;
use std::any::type_name;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ops::DerefMut;

use itertools::Itertools;
use vortex_bytes::UniqueBytes;
use vortex_error::VortexExpect;
use vortex_error::vortex_panic;

use crate::Alignment;
use crate::Buffer;
use crate::BufferAllocatorRef;
use crate::ByteBufferMut;
use crate::buffer::copy_to_vec;
use crate::debug::TruncatedDebug;
use crate::trusted_len::TrustedLen;

/// A mutable buffer that maintains a runtime-defined alignment through resizing operations.
///
/// This is a typed view over a [`UniqueBytes`] window: the bytes, the alignment promise, and the
/// allocation all live there, and this type only reinterprets them as `T`s. Elements are treated
/// as plain data: the buffer never runs `T`'s destructor, and never will.
///
/// Zero-sized element types are rejected at compile time when constructing a buffer.
///
/// ```compile_fail
/// use vortex_buffer::BufferMut;
/// let _ = BufferMut::<()>::empty();
/// ```
///
/// ```compile_fail
/// use vortex_buffer::BufferMut;
/// let _ = BufferMut::<()>::from_vec(vec![(); 3]);
/// ```
pub struct BufferMut<T> {
    /// The bytes of the initialised elements, the spare capacity after them, the region they live
    /// in, and the alignment promised for the first of them.
    pub(crate) bytes: UniqueBytes,
    /// Marks the buffer as logically owning values of `T` despite storing erased bytes.
    pub(crate) _marker: PhantomData<T>,
}

impl<T> BufferMut<T> {
    /// View `bytes` as `T`s.
    ///
    /// `bytes` must hold a whole number of `T`s and promise at least `T`'s alignment; every
    /// constructor arranges for both.
    #[inline]
    pub(crate) fn from_unique(bytes: UniqueBytes) -> Self {
        const { assert!(size_of::<T>() != 0, "zero-sized types are not supported") };
        debug_assert!(bytes.len().is_multiple_of(size_of::<T>()));
        debug_assert!(bytes.alignment().is_aligned_to(Alignment::of::<T>()));
        Self {
            bytes,
            _marker: PhantomData,
        }
    }

    /// The number of bytes `n` elements occupy.
    #[inline]
    fn bytes_for(n: usize) -> usize {
        n.checked_mul(size_of::<T>())
            .vortex_expect("buffer capacity overflow")
    }

    /// Reject an alignment that `T` itself could not be stored at.
    #[inline]
    fn check_alignment(alignment: Alignment) {
        if !alignment.is_aligned_to(Alignment::of::<T>()) {
            vortex_panic!(
                "Alignment {} must align to the scalar type's alignment {}",
                alignment,
                align_of::<T>()
            );
        }
    }

    /// Create a new `BufferMut` with the requested alignment and capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_in(capacity, BufferAllocatorRef::statically_allocated())
    }

    /// Create a new `BufferMut` with the requested capacity and allocator.
    pub fn with_capacity_in(capacity: usize, allocator: BufferAllocatorRef) -> Self {
        Self::with_capacity_aligned_in(capacity, Alignment::of::<T>(), allocator)
    }

    /// Create a new `BufferMut` with the requested alignment and capacity.
    ///
    /// The allocation is over-aligned to [`Alignment::DEFAULT_ALIGNMENT`] when that is larger than
    /// `alignment`. Use [`with_capacity_preferred_aligned`] to control the over-alignment.
    ///
    /// [`with_capacity_preferred_aligned`]: Self::with_capacity_preferred_aligned
    pub fn with_capacity_aligned(capacity: usize, alignment: Alignment) -> Self {
        Self::with_capacity_aligned_in(
            capacity,
            alignment,
            BufferAllocatorRef::statically_allocated(),
        )
    }

    /// Create a new `BufferMut` with the requested alignment, capacity, and allocator.
    pub fn with_capacity_aligned_in(
        capacity: usize,
        alignment: Alignment,
        allocator: BufferAllocatorRef,
    ) -> Self {
        Self::with_capacity_preferred_aligned_in(
            capacity,
            alignment,
            Some(Alignment::DEFAULT_ALIGNMENT),
            allocator,
        )
    }

    /// Create a new `BufferMut` with the requested alignment and capacity.
    ///
    /// The buffer reports `alignment`, but the underlying allocation is over-aligned to the larger
    /// of `alignment` and `preferred_alignment`.
    pub fn with_capacity_preferred_aligned(
        capacity: usize,
        alignment: Alignment,
        preferred_alignment: Option<Alignment>,
    ) -> Self {
        Self::with_capacity_preferred_aligned_in(
            capacity,
            alignment,
            preferred_alignment,
            BufferAllocatorRef::statically_allocated(),
        )
    }

    /// Create a new allocator-backed `BufferMut` with a requested and preferred alignment.
    pub fn with_capacity_preferred_aligned_in(
        capacity: usize,
        alignment: Alignment,
        preferred_alignment: Option<Alignment>,
        allocator: BufferAllocatorRef,
    ) -> Self {
        Self::check_alignment(alignment);
        Self::from_unique(UniqueBytes::with_capacity_preferred_in(
            Self::bytes_for(capacity),
            alignment,
            preferred_alignment.unwrap_or(alignment),
            allocator,
        ))
    }

    /// Create a new zeroed `BufferMut`.
    pub fn zeroed(len: usize) -> Self {
        Self::zeroed_in(len, BufferAllocatorRef::statically_allocated())
    }

    /// Create a new zeroed `BufferMut` with the requested allocator.
    pub fn zeroed_in(len: usize, allocator: BufferAllocatorRef) -> Self {
        Self::zeroed_aligned_in(len, Alignment::of::<T>(), allocator)
    }

    /// Create a new zeroed `BufferMut` with the requested alignment.
    ///
    /// The allocation is over-aligned to [`Alignment::DEFAULT_ALIGNMENT`] when that is larger than
    /// `alignment`. Use [`zeroed_preferred_aligned`] to control the over-alignment.
    ///
    /// [`zeroed_preferred_aligned`]: Self::zeroed_preferred_aligned
    pub fn zeroed_aligned(len: usize, alignment: Alignment) -> Self {
        Self::zeroed_aligned_in(len, alignment, BufferAllocatorRef::statically_allocated())
    }

    /// Create a zeroed `BufferMut` with an alignment and allocator.
    pub fn zeroed_aligned_in(
        len: usize,
        alignment: Alignment,
        allocator: BufferAllocatorRef,
    ) -> Self {
        Self::zeroed_preferred_aligned_in(
            len,
            alignment,
            Some(Alignment::DEFAULT_ALIGNMENT),
            allocator,
        )
    }

    /// Create a new zeroed `BufferMut` with the requested alignment.
    ///
    /// The buffer reports `alignment`, but the underlying allocation is over-aligned to the larger
    /// of `alignment` and `preferred_alignment`.
    pub fn zeroed_preferred_aligned(
        len: usize,
        alignment: Alignment,
        preferred_alignment: Option<Alignment>,
    ) -> Self {
        Self::zeroed_preferred_aligned_in(
            len,
            alignment,
            preferred_alignment,
            BufferAllocatorRef::statically_allocated(),
        )
    }

    /// Create a zeroed allocator-backed buffer with a requested and preferred alignment.
    pub fn zeroed_preferred_aligned_in(
        len: usize,
        alignment: Alignment,
        preferred_alignment: Option<Alignment>,
        allocator: BufferAllocatorRef,
    ) -> Self {
        Self::check_alignment(alignment);
        Self::from_unique(UniqueBytes::zeroed_preferred_in(
            Self::bytes_for(len),
            alignment,
            preferred_alignment.unwrap_or(alignment),
            allocator,
        ))
    }

    /// Create a new empty `BufferMut` aligned to `T`.
    pub fn empty() -> Self {
        Self::empty_aligned(Alignment::of::<T>())
    }

    /// Create a new empty `BufferMut` with the provided alignment.
    ///
    /// The allocation is over-aligned to [`Alignment::DEFAULT_ALIGNMENT`] when that is larger than
    /// `alignment`. Use [`empty_preferred_aligned`] to control the over-alignment.
    ///
    /// [`empty_preferred_aligned`]: Self::empty_preferred_aligned
    pub fn empty_aligned(alignment: Alignment) -> Self {
        Self::empty_aligned_in(alignment, BufferAllocatorRef::statically_allocated())
    }

    /// Create an empty `BufferMut` with an alignment and allocator.
    pub fn empty_aligned_in(alignment: Alignment, allocator: BufferAllocatorRef) -> Self {
        Self::with_capacity_aligned_in(0, alignment, allocator)
    }

    /// Create a new empty `BufferMut` with the provided alignment.
    ///
    /// The buffer reports `alignment`, but the underlying allocation is over-aligned to the larger
    /// of `alignment` and `preferred_alignment`.
    pub fn empty_preferred_aligned(
        alignment: Alignment,
        preferred_alignment: Option<Alignment>,
    ) -> Self {
        Self::with_capacity_preferred_aligned(0, alignment, preferred_alignment)
    }

    /// Create a new full `BufferMut` with the given value.
    pub fn full(item: T, len: usize) -> Self
    where
        T: Copy,
    {
        Self::full_in(item, len, BufferAllocatorRef::statically_allocated())
    }

    /// Create a full `BufferMut` with the given value and allocator.
    pub fn full_in(item: T, len: usize, allocator: BufferAllocatorRef) -> Self
    where
        T: Copy,
    {
        let mut buffer = BufferMut::<T>::with_capacity_in(len, allocator);
        buffer.push_n(item, len);
        buffer
    }

    /// Create a mutable scalar buffer by copying the contents of the slice.
    pub fn copy_from(other: impl AsRef<[T]>) -> Self {
        Self::copy_from_in(other, BufferAllocatorRef::statically_allocated())
    }

    /// Create a mutable scalar buffer by copying with the given allocator.
    pub fn copy_from_in(other: impl AsRef<[T]>, allocator: BufferAllocatorRef) -> Self {
        Self::copy_from_aligned_in(other, Alignment::of::<T>(), allocator)
    }

    /// Create a mutable scalar buffer with the alignment by copying the contents of the slice.
    ///
    /// The allocation is over-aligned to [`Alignment::DEFAULT_ALIGNMENT`] when that is larger than
    /// `alignment`. Use [`copy_from_preferred_aligned`] to control the over-alignment.
    ///
    /// [`copy_from_preferred_aligned`]: Self::copy_from_preferred_aligned
    ///
    /// ## Panics
    ///
    /// Panics when the requested alignment isn't itself aligned to type T.
    pub fn copy_from_aligned(other: impl AsRef<[T]>, alignment: Alignment) -> Self {
        Self::copy_from_aligned_in(other, alignment, BufferAllocatorRef::statically_allocated())
    }

    /// Copy values into a mutable buffer with the given alignment and allocator.
    pub fn copy_from_aligned_in(
        other: impl AsRef<[T]>,
        alignment: Alignment,
        allocator: BufferAllocatorRef,
    ) -> Self {
        Self::copy_from_preferred_aligned_in(
            other,
            alignment,
            Some(Alignment::DEFAULT_ALIGNMENT),
            allocator,
        )
    }

    /// Create a mutable scalar buffer with the alignment by copying the contents of the slice.
    ///
    /// The buffer reports `alignment`, but the underlying allocation is over-aligned to the larger
    /// of `alignment` and `preferred_alignment`.
    ///
    /// ## Panics
    ///
    /// Panics when the requested alignment isn't itself aligned to type T.
    pub fn copy_from_preferred_aligned(
        other: impl AsRef<[T]>,
        alignment: Alignment,
        preferred_alignment: Option<Alignment>,
    ) -> Self {
        Self::copy_from_preferred_aligned_in(
            other,
            alignment,
            preferred_alignment,
            BufferAllocatorRef::statically_allocated(),
        )
    }

    /// Copy values with the given allocator, requested alignment, and preferred alignment.
    pub fn copy_from_preferred_aligned_in(
        other: impl AsRef<[T]>,
        alignment: Alignment,
        preferred_alignment: Option<Alignment>,
        allocator: BufferAllocatorRef,
    ) -> Self {
        let other = other.as_ref();
        let mut buffer = Self::with_capacity_preferred_aligned_in(
            other.len(),
            alignment,
            preferred_alignment,
            allocator,
        );
        buffer.extend_from_slice(other);
        debug_assert_eq!(buffer.alignment(), alignment);
        buffer
    }

    /// Take zero-copy ownership of a `Vec<T>`.
    ///
    /// The buffer treats the elements as plain data and never runs `T`'s destructor. Prefer
    /// [`Buffer::from_vec`] for a `T` with a destructor: it keeps the `Vec` alive instead.
    pub fn from_vec(vec: Vec<T>) -> Self {
        Self::from_unique(UniqueBytes::from_vec(vec))
    }

    /// Take zero-copy, *writable* ownership of memory kept alive by `owner`.
    ///
    /// Where [`Buffer::from_owner`] only asks the owner for shared access, this asks for
    /// exclusive access through [`AsMut`], and taking `owner` by value proves nothing else can be
    /// looking at the memory in the meantime. That is what lets the buffer write straight into a
    /// writable memory map or an Arrow `MutableBuffer` without copying, and it is why freezing and
    /// thawing such a buffer never copies either.
    ///
    /// ## Example
    ///
    /// ```
    /// use vortex_buffer::BufferMut;
    ///
    /// let mut buffer = BufferMut::from_owner(vec![1u32, 2, 3]);
    /// buffer[0] = 10;
    /// let frozen = buffer.freeze();
    /// let thawed = frozen.try_into_mut().expect("still the only handle");
    /// assert_eq!(thawed.as_slice(), &[10, 2, 3]);
    /// ```
    pub fn from_owner<O>(owner: O) -> Self
    where
        O: AsMut<[T]> + Send + 'static,
    {
        Self::from_unique(UniqueBytes::from_owner::<O, T>(owner))
    }

    /// Get the alignment of the buffer.
    #[inline]
    pub fn alignment(&self) -> Alignment {
        self.bytes.alignment()
    }

    /// Returns the allocator that owns this buffer.
    #[inline]
    pub fn allocator(&self) -> &BufferAllocatorRef {
        self.bytes.allocator()
    }

    /// Returns the length of the buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.bytes.len() / size_of::<T>()
    }

    /// Returns whether the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Returns the capacity of the buffer, in elements.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.bytes.capacity() / size_of::<T>()
    }

    /// Returns a raw pointer to the buffer's data.
    #[inline]
    pub fn as_ptr(&self) -> *const T {
        self.bytes.as_ptr().cast::<T>()
    }

    /// Returns a mutable raw pointer to the buffer's data.
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.bytes.as_ptr().cast_mut().cast::<T>()
    }

    /// Returns a slice over the buffer of elements of type T.
    #[inline]
    pub fn as_slice(&self) -> &[T] {
        // SAFETY: the bytes hold `len()` initialised `T`s, and the pointer is aligned for `T` by
        // construction.
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len()) }
    }

    /// Returns a mutable slice over the buffer of elements of type T.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        let length = self.len();
        // SAFETY: as for `as_slice`, and the window is exclusively ours.
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), length) }
    }

    /// Clear the buffer, retaining any existing capacity.
    #[inline]
    pub fn clear(&mut self) {
        // SAFETY: shrinking to zero cannot expose uninitialised bytes.
        unsafe { self.bytes.set_len(0) }
    }

    /// Shortens the buffer, keeping the first `len` elements and dropping the rest.
    ///
    /// If `len` is greater than the buffer's current length, this has no effect. Existing
    /// underlying capacity is preserved.
    #[inline]
    pub fn truncate(&mut self, len: usize) {
        if len <= self.len() {
            // SAFETY: shrinking the buffer cannot expose uninitialised bytes.
            unsafe { self.set_len(len) };
        }
    }

    /// Reserves capacity for at least `additional` more elements to be inserted in the buffer.
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.bytes.reserve(Self::bytes_for(additional));
    }

    /// Returns the spare capacity of the buffer as a slice of `MaybeUninit<T>`.
    /// Has identical semantics to [`Vec::spare_capacity_mut`].
    ///
    /// The returned slice can be used to fill the buffer with data (e.g. by
    /// reading from a file) before marking the data as initialized using the
    /// [`set_len`] method.
    ///
    /// Note that the returned slice may be larger than the capacity requested at
    /// construction, since the underlying allocation can be rounded up (e.g. to
    /// satisfy alignment requirements).
    ///
    /// [`set_len`]: BufferMut::set_len
    /// [`Vec::spare_capacity_mut`]: Vec::spare_capacity_mut
    ///
    /// # Examples
    ///
    /// ```
    /// use vortex_buffer::BufferMut;
    ///
    /// // Allocate vector big enough for 10 elements.
    /// let mut b = BufferMut::<u64>::with_capacity(10);
    ///
    /// // Fill in the first 3 elements.
    /// let uninit = b.spare_capacity_mut();
    /// uninit[0].write(0);
    /// uninit[1].write(1);
    /// uninit[2].write(2);
    ///
    /// // Mark the first 3 elements of the vector as being initialized.
    /// unsafe {
    ///     b.set_len(3);
    /// }
    ///
    /// assert_eq!(b.as_slice(), &[0u64, 1, 2]);
    /// ```
    #[inline]
    pub fn spare_capacity_mut(&mut self) -> &mut [MaybeUninit<T>] {
        let length = self.len();
        let spare = self.capacity() - length;
        // SAFETY: `length..capacity` is within the window, which is exclusively ours.
        let dst = unsafe { self.as_mut_ptr().add(length) }.cast::<MaybeUninit<T>>();
        unsafe { std::slice::from_raw_parts_mut(dst, spare) }
    }

    /// Sets the length of the buffer.
    ///
    /// # Safety
    ///
    /// - `new_len` must be less than or equal to [`capacity()`].
    /// - The elements at `old_len..new_len` must be initialized.
    ///
    /// [`capacity()`]: Self::capacity
    #[inline]
    pub unsafe fn set_len(&mut self, len: usize) {
        debug_assert!(len <= self.capacity());
        // SAFETY: the caller guarantees the elements, and so their bytes, are initialised.
        unsafe { self.bytes.set_len(Self::bytes_for(len)) }
    }

    /// Appends a scalar to the buffer.
    #[inline]
    pub fn push(&mut self, value: T) {
        self.reserve(1);
        // SAFETY: we just reserved room for one more element.
        unsafe { self.push_unchecked(value) }
    }

    /// Appends a scalar to the buffer without checking for sufficient capacity.
    ///
    /// ## Safety
    ///
    /// The caller must ensure there is sufficient capacity in the array.
    #[inline]
    pub unsafe fn push_unchecked(&mut self, item: T) {
        let length = self.len();
        // SAFETY: the caller ensures we have sufficient capacity.
        unsafe {
            let dst = self.as_mut_ptr().add(length);
            dst.write(item);
            self.set_len(length + 1);
        }
    }

    /// Appends n scalars to the buffer.
    ///
    /// This function is slightly more optimized than `extend(iter::repeat_n(item, b))`.
    #[inline]
    pub fn push_n(&mut self, item: T, n: usize)
    where
        T: Copy,
    {
        self.reserve(n);
        // SAFETY: we just reserved room for `n` more elements.
        unsafe { self.push_n_unchecked(item, n) }
    }

    /// Appends n scalars to the buffer.
    ///
    /// ## Safety
    ///
    /// The caller must ensure there is sufficient capacity in the array.
    #[inline]
    pub unsafe fn push_n_unchecked(&mut self, item: T, n: usize)
    where
        T: Copy,
    {
        let length = self.len();
        // SAFETY: the caller guarantees enough spare capacity.
        let mut dst = unsafe { self.as_mut_ptr().add(length) };
        // SAFETY: we checked the capacity in the reserve call
        unsafe {
            let end = dst.add(n);
            while dst < end {
                dst.write(item);
                dst = dst.add(1);
            }
            self.set_len(length + n);
        }
    }

    /// Appends a slice of type `T`, growing the internal buffer as needed.
    ///
    /// # Example:
    ///
    /// ```
    /// # use vortex_buffer::BufferMut;
    ///
    /// let mut builder = BufferMut::<u16>::with_capacity(10);
    /// builder.extend_from_slice(&[42, 44, 46]);
    ///
    /// assert_eq!(builder.len(), 3);
    /// ```
    #[inline]
    pub fn extend_from_slice(&mut self, slice: &[T]) {
        self.reserve(slice.len());
        let length = self.len();
        // SAFETY: reserve made the destination valid and non-overlapping for slice.len() values.
        unsafe {
            std::ptr::copy_nonoverlapping(
                slice.as_ptr(),
                self.as_mut_ptr().add(length),
                slice.len(),
            );
            self.set_len(length + slice.len());
        }
    }

    /// Return the [`ByteBufferMut`] for this [`BufferMut`].
    ///
    /// The byte buffer keeps this buffer's alignment.
    pub fn into_byte_buffer(self) -> ByteBufferMut {
        ByteBufferMut::from_unique(self.bytes)
    }

    /// Freeze the `BufferMut` into a `Buffer`.
    ///
    /// This never allocates: the region simply changes hands.
    #[inline]
    pub fn freeze(self) -> Buffer<T> {
        Buffer::from_shared(self.bytes.freeze())
    }

    /// Convert the buffer into a `Vec<T>`, without copying where possible.
    ///
    /// This is zero-copy when the buffer's region came from the global allocator with exactly a
    /// `Vec<T>`'s layout - because it was adopted from a `Vec<T>`, or because it was allocated
    /// with `align_of::<T>()` and no preferred over-alignment - and the buffer starts at the front
    /// of it. Anything else, an over-aligned buffer in particular, is copied.
    pub fn into_vec(self) -> Vec<T>
    where
        T: Copy,
    {
        self.try_into_vec()
            .unwrap_or_else(|buffer| copy_to_vec(buffer.as_slice()))
    }

    /// Convert the buffer into a `Vec<T>` without copying, or give it back.
    ///
    /// See [`into_vec`](Self::into_vec) for when this succeeds.
    pub fn try_into_vec(self) -> Result<Vec<T>, Self> {
        self.bytes.try_into_vec::<T>().map_err(Self::from_unique)
    }

    /// Split the buffer in two at `at`, keeping `..at` and returning `at..`.
    ///
    /// Both halves keep pointing into the same allocation, so neither moves; the split point may
    /// lie past the current length, in which case the returned half is empty but has capacity.
    ///
    /// The half we keep reports this buffer's alignment; the half we hand back starts at `at` and
    /// reports the strongest alignment that offset still satisfies.
    ///
    /// ## Panics
    ///
    /// Panics if `at` exceeds the capacity.
    pub fn split_off(&mut self, at: usize) -> Self {
        if at > self.capacity() {
            vortex_panic!(
                "Cannot split buffer of capacity {} at {}",
                self.capacity(),
                at
            );
        }
        Self::from_unique(self.bytes.split_off(Self::bytes_for(at)))
    }

    /// Absorb a buffer previously produced by [`split_off`](Self::split_off).
    ///
    /// `O(1)` when the two are still adjacent in the same allocation; otherwise this copies.
    ///
    /// The result starts where this buffer started, so it keeps this buffer's alignment whatever
    /// `other` reported.
    pub fn unsplit(&mut self, other: Self) {
        self.bytes.unsplit(other.bytes);
    }

    /// Map each element of the buffer with a closure, reusing the buffer's allocation.
    ///
    /// ## Panics
    ///
    /// Panics if `R` does not have the same size and alignment as `T`. Both are required: the
    /// mapped buffer keeps `T`'s pointer, so a wider `R` would be read and written through a
    /// pointer that is not aligned for it.
    pub fn map_each_in_place<R, F>(self, mut f: F) -> BufferMut<R>
    where
        T: Copy,
        // `R: Copy` is what makes the in-place write below sound. Assigning through the
        // reinterpreted slice drops the old value, which is `T`'s bits viewed as an `R` - for an
        // `R` with a destructor that means running it over bits that were never a valid `R`.
        R: Copy,
        F: FnMut(T) -> R,
    {
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
        // `T` and `R` have the same size and alignment, so the bytes, length and capacity are all
        // equally valid for `R`.
        let mut buf = BufferMut::<R>::from_unique(self.bytes);
        buf.iter_mut().for_each(|item| {
            // SAFETY: the element still holds a `T`, and `T` and `R` have the same size.
            let value = unsafe { std::mem::transmute_copy::<R, T>(item) };
            *item = f(value);
        });
        buf
    }

    /// Return a `BufferMut<T>` with the same data as this one with the given alignment.
    ///
    /// If the data is already properly aligned, this is a metadata-only operation.
    ///
    /// If the data is not aligned, we copy it into a new allocation.
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
            let capacity = self.capacity();
            let allocator = self.allocator().clone();
            let mut aligned = Self::with_capacity_aligned_in(capacity, alignment, allocator);
            aligned.extend_from_slice(&self);
            aligned
        }
    }

    /// Transmute a `BufferMut<T>` into a `BufferMut<U>`.
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
    pub unsafe fn transmute<U>(self) -> BufferMut<U> {
        assert_eq!(size_of::<T>(), size_of::<U>(), "Buffer type size mismatch");
        assert_eq!(
            align_of::<T>(),
            align_of::<U>(),
            "Buffer type alignment mismatch"
        );
        BufferMut::<U>::from_unique(self.bytes)
    }
}

impl<T> Clone for BufferMut<T> {
    fn clone(&self) -> Self {
        let mut buffer = BufferMut::<T>::with_capacity_aligned_in(
            self.capacity(),
            self.alignment(),
            self.allocator().clone(),
        );
        buffer.extend_from_slice(self.as_slice());
        buffer
    }
}

impl<T: PartialEq> PartialEq for BufferMut<T> {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T: Eq> Eq for BufferMut<T> {}

impl<T: Debug> Debug for BufferMut<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(&format!("BufferMut<{}>", type_name::<T>()))
            .field("length", &self.len())
            .field("alignment", &self.alignment())
            .field("as_slice", &TruncatedDebug(self.as_slice()))
            .finish()
    }
}

impl<T> Default for BufferMut<T> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<T> Deref for BufferMut<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T> DerefMut for BufferMut<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl<T> AsRef<[T]> for BufferMut<T> {
    #[inline]
    fn as_ref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T> AsMut<[T]> for BufferMut<T> {
    #[inline]
    fn as_mut(&mut self) -> &mut [T] {
        self.as_mut_slice()
    }
}

impl From<UniqueBytes> for ByteBufferMut {
    /// Bytes need no reinterpretation, so this is free and keeps the window's alignment.
    #[inline]
    fn from(bytes: UniqueBytes) -> Self {
        Self::from_unique(bytes)
    }
}

impl From<ByteBufferMut> for UniqueBytes {
    #[inline]
    fn from(buffer: ByteBufferMut) -> Self {
        buffer.bytes
    }
}

impl<T> BufferMut<T> {
    /// A helper method for the two [`Extend`] implementations.
    ///
    /// We use the lower bound hint on the iterator to manually write data, and then we continue to
    /// push items normally past the lower bound.
    fn extend_iter(&mut self, mut iter: impl Iterator<Item = T>) {
        // Since we do not know the length of the iterator, we can only guess how much memory we
        // need to reserve. Note that these hints may be inaccurate.
        let (lower_bound, _) = iter.size_hint();

        // We choose not to use the optional upper bound size hint to match the standard library.

        self.reserve(lower_bound);
        let unwritten = self.capacity() - self.len();

        // We store `begin` in the case that the lower bound hint is incorrect.
        let begin: *const T = self.spare_capacity_mut().as_mut_ptr().cast();
        let mut dst: *mut T = begin.cast_mut();
        let mut items_written = 0;

        // As a first step, we manually iterate the iterator up to the known capacity.
        for _ in 0..unwritten {
            let Some(item) = iter.next() else {
                // The lower bound hint may be incorrect.
                break;
            };

            // SAFETY: We have reserved enough capacity to hold this item, and `dst` is a pointer
            // derived from a valid reference to byte data.
            unsafe { dst.write(item) };

            // Note: We used to have `dst.add(iteration).write(item)`, here. However this was much
            // slower than just incrementing `dst`.
            // SAFETY: The offsets fits in `isize`, and because we were able to reserve the memory
            // we know that `add` will not overflow.
            unsafe { dst = dst.add(1) };
            items_written += 1;
        }

        let length = self.len() + items_written;

        // SAFETY: We have written valid items between the old length and the new length.
        unsafe { self.set_len(length) };

        // Finally, since the iterator will have arbitrarily more items to yield, we push the
        // remaining items normally.
        iter.for_each(|item| self.push(item));
    }

    /// Extends the `BufferMut` with an iterator with `TrustedLen`.
    ///
    /// The caller guarantees that the iterator will have a trusted upper bound, which allows the
    /// implementation to reserve all of the memory needed up front.
    pub fn extend_trusted<I: TrustedLen<Item = T>>(&mut self, iter: I) {
        let (_, upper_bound) = iter.size_hint();
        let upper_bound = upper_bound
            .vortex_expect("`TrustedLen` iterator somehow didn't have valid upper bound");
        self.reserve(upper_bound);

        let begin: *const T = self.spare_capacity_mut().as_mut_ptr().cast();
        let mut dst: *mut T = begin.cast_mut();
        let mut items_written = 0;

        iter.for_each(|item| {
            // SAFETY: We have reserved enough capacity to hold this item, and `dst` is a pointer
            // derived from a valid reference to byte data.
            unsafe { dst.write(item) };

            // Note: We used to have `dst.add(iteration).write(item)`, here. However this was much
            // slower than just incrementing `dst`.
            // SAFETY: The offset fits in `isize`, and because we were able to reserve the memory
            // we know that `add` will not overflow.
            unsafe { dst = dst.add(1) };
            items_written += 1;
        });
        debug_assert!(
            items_written <= upper_bound,
            "TrustedLen upper bound was wrong"
        );

        let length = self.len() + items_written;

        // SAFETY: We have written valid items between the old length and the new length.
        unsafe { self.set_len(length) };
    }

    /// Creates a `BufferMut` from an iterator with a trusted length.
    ///
    /// Internally, this calls [`extend_trusted()`](Self::extend_trusted).
    pub fn from_trusted_len_iter<I>(iter: I) -> Self
    where
        I: TrustedLen<Item = T>,
    {
        let (_, upper_bound) = iter.size_hint();
        let mut buffer = Self::with_capacity(
            upper_bound
                .vortex_expect("`TrustedLen` iterator somehow didn't have valid upper bound"),
        );

        buffer.extend_trusted(iter);
        buffer
    }

    /// Like [`extend_trusted()`](Self::extend_trusted), but the iterator yields `Result<T, E>`
    /// and the extension short-circuits on the first `Err`.
    ///
    /// On error, items written before the failure remain in the buffer.
    pub fn try_extend_trusted<E, I>(&mut self, iter: I) -> Result<(), E>
    where
        I: TrustedLen<Item = Result<T, E>>,
    {
        iter.process_results(|values| self.extend_trusted(values))
    }

    /// Like [`from_trusted_len_iter()`](Self::from_trusted_len_iter), but the iterator yields
    /// `Result<T, E>` and construction short-circuits on the first `Err`.
    pub fn try_from_trusted_len_iter<E, I>(iter: I) -> Result<Self, E>
    where
        I: TrustedLen<Item = Result<T, E>>,
    {
        iter.process_results(|values| Self::from_trusted_len_iter(values))
    }
}

impl<T> Extend<T> for BufferMut<T> {
    #[inline]
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        self.extend_iter(iter.into_iter())
    }
}

impl<'a, T> Extend<&'a T> for BufferMut<T>
where
    T: Copy + 'a,
{
    #[inline]
    fn extend<I: IntoIterator<Item = &'a T>>(&mut self, iter: I) {
        self.extend_iter(iter.into_iter().copied())
    }
}

impl<T> FromIterator<T> for BufferMut<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut buffer = Self::with_capacity(iter.size_hint().0);
        buffer.extend(iter);
        buffer
    }
}

#[cfg(test)]
mod tests;
