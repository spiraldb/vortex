// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Host-owned output slots and direct Boolean packing.

use std::mem::MaybeUninit;
use std::ops::BitOrAssign;

use rowfn_kernels::lane_kernels::IndexedSource;
use rowfn_kernels::lane_kernels::IndexedSourceExt;

use crate::Host;
use crate::HostResult;

/// Storage that permits safe abandonment after any partial initialization.
///
/// # Safety
/// Slot count and contents must remain stable across views and moves. Dropping the buffer with any
/// subset initialized must be safe. Publication must preserve initialized slot order and contents.
pub unsafe trait OutputBuffer<T>: Sized {
    /// The host-native finished result.
    type Finished;
    /// Writable slots whose initialized prefix can be published.
    fn slots(&mut self) -> &mut [MaybeUninit<T>];
    /// Publish the initialized prefix, reusing its allocation where possible.
    ///
    /// # Safety
    /// The first `len` slots must exist and remain initialized. No other slots can be read.
    unsafe fn finish(self, len: usize) -> Self::Finished;
}

/// Output storage and native metadata for an owned value.
pub trait OutputBinding<T: Default + 'static>: Host {
    /// Storage allocated using host resources.
    type Buffer: OutputBuffer<T, Finished = Self::Column> + 'static;
    /// Non-nullable native storage type.
    fn output_type() -> Self::NativeType;
    /// Reserve at least `rows` writable slots using the invocation allocator.
    fn allocate(rows: usize, ctx: &mut Self::Context) -> HostResult<Self, Self::Buffer>;

    /// Collect an infallible source directly into host storage.
    ///
    /// An override must preserve values and call `apply` once per source row in increasing order.
    /// It must use invocation resources and cannot introduce semantic errors or panics. Allocation
    /// and other infrastructure errors remain terminal. Values that need drop are unsupported.
    fn build_from<S: IndexedSource>(
        source: S,
        apply: impl Fn(S::Item) -> T,
        ctx: &mut Self::Context,
    ) -> HostResult<Self, Self::Column> {
        const { assert!(!std::mem::needs_drop::<T>()); }
        let rows = source.len();
        let mut output = Self::allocate(rows, ctx)?;
        source.map_into(&mut output.slots()[..rows], apply);
        // SAFETY: the collector initialized the entire published prefix.
        Ok(unsafe { output.finish(rows) })
    }
}

/// A host that packs Boolean values directly into its own output storage.
///
/// # Safety
/// The collector may invoke `apply` only with indices below `rows`. No callback can run when
/// `rows` is zero. Executors can perform unchecked reads inside that callback.
pub unsafe trait BooleanOutput: OutputBinding<bool> {
    /// Evaluate every index once in order and pack it using the shared kernels.
    fn collect_bool<const MULTIVERSIONED: bool>(
        rows: usize,
        apply: impl FnMut(usize) -> bool,
        ctx: &mut Self::Context,
    ) -> HostResult<Self, Self::Column>;
}

/// Compact row failure evidence. Default must represent success, including for zero rows.
pub trait FailureEvidence: Copy + Default + BitOrAssign {}
impl<T: Copy + Default + BitOrAssign> FailureEvidence for T {}
