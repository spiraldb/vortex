// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#![deny(missing_docs)]

//! Aligned, reference-counted byte regions.
//!
//! This is the untyped layer beneath [`vortex-buffer`]'s `Buffer<T>` and `BufferMut<T>`: it owns
//! memory, and the alignment promised for it, and nothing else. Element types belong to the layer
//! above, which is a `PhantomData` away from the types here; everything here is bytes.
//!
//! Two handle types divide the world by exclusivity, the way `Vec` and `RawVec` divide it by
//! responsibility:
//!
//! * [`SharedBytes`] is a window that may be aliased. It is `Clone`, and it only ever hands out
//!   `&[u8]`.
//! * [`UniqueBytes`] is a window nothing else can see. It is the only one that hands out
//!   `&mut [u8]`, and it is not `Clone`.
//!
//! [`UniqueBytes::freeze`] moves a window from the second world into the first without allocating,
//! and [`SharedBytes::try_into_unique`] moves it back whenever the region turns out to have only
//! one handle.
//!
//! Keeping this layer non-generic is deliberate. All of the `unsafe` that manages regions -
//! provenance, refcount discipline, promotion, `realloc` - compiles exactly once, rather than once
//! per element type, and it is small enough to audit and to test exhaustively under Miri.
//!
//! The crate has no dependencies at all, Vortex or otherwise. Failures are reported as
//! [`InvalidAlignment`] and through panics, rather than through a shared error type, so nothing
//! here has to agree with the rest of Vortex about how errors are represented. `vortex-error`
//! converts [`InvalidAlignment`] into a `VortexError` behind its `vortex-bytes` feature, which is
//! how `?` keeps working for callers that want it.
//!
//! [`vortex-buffer`]: https://docs.rs/vortex-buffer
//!
//! # Model
//!
//! These types mirror the model of `bytes::Bytes` and `bytes::BytesMut`: a handle is a window
//! `(ptr, len)` into a region, cloning is a refcount bump, and slicing is pointer arithmetic. They
//! differ in that the region is managed directly through [`std::alloc`], which buys three things
//! `bytes` cannot give us:
//!
//! * **Native alignment.** A region records the alignment it was allocated with, so an
//!   over-aligned buffer no longer has to over-allocate by `alignment` bytes and offset into the
//!   middle of a `Vec<u8>`.
//! * **Mutable foreign buffers.** A region adopted from foreign memory records whether it may be
//!   written through. `bytes::Bytes::try_into_mut` only succeeds for bytes that came out of
//!   `BytesMut::freeze`, so an adopted `Vec<T>`, Arrow buffer, or mmap could never be made
//!   mutable again without a copy.
//! * **`Vec<T>` round-trips.** A region allocated with exactly `Layout::array::<T>(cap)` is
//!   indistinguishable from a `Vec<T>`'s allocation, so it can be handed back out as one.
//!
//! # Allocation
//!
//! Regions come from a [`BufferAllocatorRef`]: the global allocator by default, or any
//! `allocator_api2::alloc::Allocator` a caller installs. Alignment is never requested from the
//! allocator directly. Asking for more than it provides natively sends every allocation down its
//! aligned-allocation path (`posix_memalign` and friends), which is markedly slower than `malloc`.
//! Instead a region is allocated with the allocator's free alignment, padded by the largest shift
//! that could be needed, and the window starts at the first suitably aligned byte inside it. A
//! region adopted from a `Vec` keeps the `Vec`'s exact layout, so it can be handed back out again.
//!
//! # Deferred sharing
//!
//! A handle that has never been shared owns its region outright, and describes it inline: no
//! refcount is allocated until a second handle actually exists. This is the same trick `bytes`
//! plays with its "promotable" vtables, and it is what keeps the common
//! build-freeze-read-drop path down to a single allocation.
//!
//! The ownership state is one word:
//!
//! ```text
//! bit  63                              8   7  2   1 0
//!     ┌─────────────────────────────────┬───────┬─────┐
//!     │ size                            │ align │ 0 1 │  OWNED  - inline description
//!     └─────────────────────────────────┴───────┴─────┘
//!     ┌───────────────────────────────────────────┬───┐
//!     │ *mut Shared                               │ 0 │  SHARED - refcounted, 8-aligned
//!     └───────────────────────────────────────────┴───┘
//!                                               0b10    STATIC - owns nothing
//! ```
//!
//! `OWNED` keeps the region's start in the handle's own `base` field, so advancing or truncating a
//! handle never has to allocate. Promotion to `SHARED` only ever rewrites the state word, never
//! `base`, which is what lets it happen through a shared reference with a single compare-exchange.
//! `OWNED` always means the global allocator. A region from a custom allocator has to carry the
//! allocator's handle, so it is `SHARED` from the start; its `Shared` is embedded at the front of
//! the block it describes rather than boxed separately, so it still costs a single allocation.

pub use alignment::*;
pub use allocator::*;
pub use shared::*;
pub use unique::*;

mod alignment;
mod allocator;
mod panic;
mod region;
mod shared;
mod unique;

pub(crate) use region::HEADER;
pub(crate) use region::Release;
pub(crate) use region::Shared;
pub(crate) use region::State;
pub(crate) use region::allocate_shifted;
pub(crate) use region::dangling;
pub(crate) use region::drop_owner;
pub(crate) use region::embedded_layout;
pub(crate) use region::free_header;
pub(crate) use region::shared_state;
pub(crate) use region::shift;
pub(crate) use region::shifted_layout;

#[cfg(test)]
mod property_tests;
#[cfg(test)]
mod tests;
