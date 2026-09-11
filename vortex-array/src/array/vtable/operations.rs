// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_error::VortexResult;
use vortex_error::vortex_bail;

use crate::ExecutionCtx;
use crate::ProbeCtx;
use crate::array::ArrayView;
use crate::array::VTable;
use crate::scalar::Scalar;
use crate::vtable::NotSupported;

/// Element-level operations for an array encoding.
///
/// This trait is separated from [`VTable`] so encodings can organize scalar
/// access independently from traversal, serialization, and execution. The erased
/// [`ArrayRef`](crate::ArrayRef)
/// methods perform common checks before dispatching here.
pub trait OperationsVTable<V: VTable> {
    /// Encoding-specific state retained by repeated scalar access.
    ///
    /// Default construction should be cheap and avoid allocation or execution. Preparation
    /// belongs in [`Self::probe_scalar`]. `'a` is the borrow of the root array, so state may
    /// retain views into its source tree. Request retained child probes through [`ProbeCtx`].
    /// Use `()` when no local state is needed.
    type ProbeState<'a>: Default + 'a;

    /// Read a scalar, handling nullness and optionally retaining state for subsequent reads.
    ///
    /// Bounds have been checked, but the row may be null. `None` requests one-off access and
    /// never initializes a context; `Some` reuses local state and child probes for this source.
    /// The scalar must retain the source's logical dtype, including nullability.
    ///
    /// The default preserves the existing scalar path without adding caching.
    fn probe_scalar<'a>(
        array: ArrayView<'a, V>,
        index: usize,
        _probe: Option<&mut ProbeCtx<'a, Self::ProbeState<'a>>>,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        array.array().execute_scalar(index, ctx)
    }

    /// Fetch the scalar at the given index.
    ///
    /// ## Preconditions
    ///
    /// Bounds-checking has already been performed by the time this function is called,
    /// and the index is guaranteed to be non-null. Implementations may assume `index < len`.
    ///
    /// ## Postconditions
    ///
    /// The returned [`Scalar`] must have the same logical dtype as the array's element dtype.
    fn scalar_at(
        array: ArrayView<'_, V>,
        index: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar>;
}

impl<V: VTable> OperationsVTable<V> for NotSupported {
    type ProbeState<'a> = ();

    fn scalar_at(
        array: ArrayView<'_, V>,
        _index: usize,
        _ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        vortex_bail!(
            "Legacy scalar_at operation is not supported for {} arrays",
            array.encoding_id()
        )
    }
}
