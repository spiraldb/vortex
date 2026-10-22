// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The [`Interleave`] encoding: a lazy, random-access gather of `N` value arrays into one array,
//! routed by a per-row `(array_index, row_index)` pair.
//!
//! # Specification
//!
//! An [`Interleave`] array has `N + 2` children: an `array_indices` selector and a `row_indices`
//! selector followed by `N` *values*. The output has `array_indices.len()` rows, and output
//! row `i` comes from `values[array_indices[i]][row_indices[i]]`.
//!
//! Unlike a `Merge`, which consumes each branch in order under a cursor, an [`Interleave`] is
//! **random-access**: `row_indices` names an explicit position within the selected value array, so
//! rows may be reordered, skipped, or repeated. A `Merge` is the special case where each value
//! array is consumed front-to-back exactly once.
//!
//! Like a `Merge`, the value arrays are independent: each holds only its own rows, and the
//! selectors stitch them back together. This distinguishes [`Interleave`] from an element-wise
//! select such as `zip`, whose arguments are all full-length.
//!
//! ## Invariants
//!
//! - Both selectors are **non-nullable** and equal in length, which is the output length. They
//!   record *where* each output row comes from, which is always a definite decision. Predicate
//!   nullability must be resolved into definite indices by the caller *before* the interleave is
//!   built.
//! - `array_indices[i] < values.len()` and `row_indices[i] < values[array_indices[i]].len()` for
//!   every `i`. These per-row bounds depend on the selector *values* and so are a runtime
//!   precondition of the caller, checked in the execution kernels rather than at construction.
//! - All values share a logical type up to nullability. The output type is that shared type with
//!   the union of the values' nullabilities. This is orthogonal to the selectors: a row's *value*
//!   may be null even though its `(array_index, row_index)` is definite.
//! - The output length equals `array_indices.len()` (`== row_indices.len()`).
//!
//! ## Selector types
//!
//! `array_indices` encodes the value array per row as a non-nullable **unsigned integer**
//! (`array_indices[i]` is the index into `values`). `row_indices` is likewise a non-nullable
//! **unsigned integer** naming the position within the selected value array.

mod execute;
mod slots;

use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hasher;

use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_panic;
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

use self::slots::InterleaveSlots;
use crate::ArrayEq;
use crate::ArrayHash;
use crate::ArrayRef;
use crate::EqMode;
use crate::ExecutionCtx;
use crate::IntoArray;
use crate::array::Array;
use crate::array::ArrayId;
use crate::array::ArrayParts;
use crate::array::ArraySlots;
use crate::array::ArrayView;
use crate::array::OperationsVTable;
use crate::array::TypedArrayRef;
use crate::array::VTable;
use crate::array::ValidityVTable;
use crate::array::with_empty_buffers;
use crate::arrays::ConstantArray;
use crate::buffer::BufferHandle;
use crate::dtype::DType;
use crate::dtype::Nullability;
use crate::executor::ExecutionResult;
use crate::scalar::Scalar;
use crate::serde::ArrayChildren;
use crate::validity::Validity;

/// An [`Interleave`]-encoded Vortex array. See the [module docs](self) for the specification.
pub type InterleaveArray = Array<Interleave>;

/// The [`Interleave`] encoding. See the [module docs](self).
#[derive(Clone, Debug)]
pub struct Interleave;

/// Per-array metadata for an [`InterleaveArray`].
///
/// The selectors occupy the leading slots, followed by the `num_values` value arrays.
#[derive(Clone, Debug)]
pub struct InterleaveData {
    pub(crate) num_values: usize,
}

impl Display for InterleaveData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "num_values: {}", self.num_values)
    }
}

impl ArrayHash for InterleaveData {
    fn array_hash<H: Hasher>(&self, state: &mut H, _accuracy: EqMode) {
        state.write_usize(self.num_values);
    }
}

impl ArrayEq for InterleaveData {
    fn array_eq(&self, other: &Self, _accuracy: EqMode) -> bool {
        self.num_values == other.num_values
    }
}

/// Accessors for the values and selectors of an [`InterleaveArray`].
pub trait InterleaveArrayExt: TypedArrayRef<Interleave> {
    /// The number of value arrays (two fewer than the number of children).
    fn num_values(&self) -> usize {
        self.num_values
    }

    /// The `idx`-th value array (holding the rows that `array_indices` routes to it).
    fn value(&self, idx: usize) -> &ArrayRef {
        self.slots()[InterleaveSlots::VALUES_OFFSET + idx]
            .as_ref()
            .vortex_expect("validated interleave value slot")
    }

    /// The selector routing each output row to a value array.
    fn array_indices(&self) -> &ArrayRef {
        self.slots()[InterleaveSlots::ARRAY_INDICES]
            .as_ref()
            .vortex_expect("validated interleave array_indices slot")
    }

    /// The selector naming each output row's position within its value array.
    fn row_indices(&self) -> &ArrayRef {
        self.slots()[InterleaveSlots::ROW_INDICES]
            .as_ref()
            .vortex_expect("validated interleave row_indices slot")
    }
}
impl<T: TypedArrayRef<Interleave>> InterleaveArrayExt for T {}

impl Interleave {
    /// The single source of truth for [`InterleaveArray`] invariants.
    ///
    /// Validates `values`, `array_indices`, and `row_indices` against the [specification](self) and
    /// returns the output [`DType`] (the shared value type with the union of value nullabilities).
    /// Both the public constructor and the [`VTable::validate`] hook funnel through here.
    fn check(
        values: &[ArrayRef],
        array_indices: &ArrayRef,
        row_indices: &ArrayRef,
    ) -> VortexResult<DType> {
        vortex_ensure!(
            values.len() >= 2,
            "interleave requires at least 2 values, got {}",
            values.len()
        );

        // Both selectors are non-nullable unsigned integers: `array_indices` indexes the values and
        // `row_indices` names a position within the selected value.
        for (name, selector) in [
            ("array_indices", array_indices),
            ("row_indices", row_indices),
        ] {
            match selector.dtype() {
                DType::Primitive(ptype, nullability) if ptype.is_unsigned_int() => {
                    vortex_ensure!(
                        !nullability.is_nullable(),
                        "interleave {name} must be non-nullable, got {}",
                        selector.dtype()
                    );
                }
                other => vortex_bail!(
                    "interleave {name} must be a non-nullable unsigned integer, got {other}"
                ),
            }
        }

        vortex_ensure_eq!(
            array_indices.len(),
            row_indices.len(),
            "interleave selectors must have equal length",
        );

        let base_dtype = values[0].dtype();
        let mut nullability = Nullability::NonNullable;
        for value in values {
            vortex_ensure!(
                value.dtype().eq_ignore_nullability(base_dtype),
                "interleave values must share a dtype up to nullability: {} vs {}",
                base_dtype,
                value.dtype()
            );
            nullability |= value.dtype().nullability();
        }

        Ok(base_dtype.with_nullability(nullability))
    }
}

impl Array<Interleave> {
    /// Constructs a new [`InterleaveArray`] from `values` and the `array_indices` / `row_indices`
    /// selectors.
    ///
    /// See the [module docs](self) for the full specification and invariants. The selectors must be
    /// non-nullable: they record a definite `(array_index, row_index)` per row, so null-predicate
    /// handling is the caller's responsibility, resolved before the interleave is constructed. The
    /// per-row bounds on the selector values are a runtime precondition checked during execution.
    pub fn try_new(
        values: Vec<ArrayRef>,
        array_indices: ArrayRef,
        row_indices: ArrayRef,
    ) -> VortexResult<Self> {
        let dtype = Interleave::check(&values, &array_indices, &row_indices)?;
        // SAFETY: `check` just validated every invariant and computed the matching `dtype`.
        Ok(unsafe { Self::new_unchecked(values, array_indices, row_indices, dtype) })
    }

    /// Constructs an [`InterleaveArray`] without re-validating the spec invariants.
    ///
    /// This is the assembly half of [`try_new`](Self::try_new): it lays out the selectors followed
    /// by the value arrays and stamps `dtype`, skipping the `Interleave::check` pass for callers
    /// that already uphold the invariants.
    ///
    /// # Safety
    ///
    /// The caller must uphold every [module invariant](self): at least two `values` sharing a dtype
    /// up to nullability, both selectors non-nullable unsigned integers of equal length, and `dtype`
    /// equal to the value returned by `Interleave::check` for these arguments (the shared value
    /// type with the union of the values' nullabilities).
    pub unsafe fn new_unchecked(
        values: Vec<ArrayRef>,
        array_indices: ArrayRef,
        row_indices: ArrayRef,
        dtype: DType,
    ) -> Self {
        let slots = InterleaveSlots {
            array_indices,
            row_indices,
            values,
        }
        .into_slots();

        // SAFETY: the caller of `new_unchecked` upholds every invariant; here we only assemble the
        // canonical slot layout (`array_indices`, `row_indices`, then values) that follows.
        unsafe { Self::new_unchecked_slots(slots, dtype) }
    }

    /// Constructs an [`InterleaveArray`] from a pre-assembled `slots` buffer, skipping both the
    /// spec re-check and the slot copy that [`new_unchecked`](Self::new_unchecked) performs.
    ///
    /// This is the lowest-level assembly path: it stamps `dtype` onto `slots` as-is. Callers that
    /// already own a correctly laid-out [`ArraySlots`] — for example reusing a validated parent's
    /// selectors while swapping its values — avoid materializing an intermediate `Vec<ArrayRef>`
    /// and re-pushing it into a fresh `ArraySlots`.
    ///
    /// # Safety
    ///
    /// The caller must uphold every [module invariant](self) *and* the slot layout: the
    /// `array_indices` selector comes first, followed by the `row_indices` selector and then the
    /// value arrays. Every slot must be `Some`, there must be at least two values, and `dtype`
    /// must equal the value returned by `Interleave::check` for these arguments.
    pub unsafe fn new_unchecked_slots(slots: ArraySlots, dtype: DType) -> Self {
        let num_values = slots.len() - InterleaveSlots::VALUES_OFFSET;
        let len = slots[InterleaveSlots::ARRAY_INDICES]
            .as_ref()
            .vortex_expect("interleave array_indices slot present")
            .len();

        unsafe {
            Array::from_parts_unchecked(ArrayParts::new(
                Interleave,
                dtype,
                len,
                InterleaveData { num_values },
                slots,
            ))
        }
    }
}

impl VTable for Interleave {
    type TypedArrayData = InterleaveData;
    type OperationsVTable = Self;
    type ValidityVTable = Self;

    fn id(&self) -> ArrayId {
        static ID: CachedId = CachedId::new("vortex.interleave");
        *ID
    }

    fn validate(
        &self,
        data: &Self::TypedArrayData,
        dtype: &DType,
        len: usize,
        slots: &[Option<ArrayRef>],
    ) -> VortexResult<()> {
        vortex_ensure_eq!(
            slots.len(),
            data.num_values + InterleaveSlots::FIXED_COUNT,
            "InterleaveArray expects values + array_indices + row_indices slots",
        );
        vortex_ensure!(
            slots.iter().all(|s| s.is_some()),
            "InterleaveArray slots must all be present"
        );

        let array_indices = slots[InterleaveSlots::ARRAY_INDICES]
            .clone()
            .vortex_expect("validated array_indices slot");
        let row_indices = slots[InterleaveSlots::ROW_INDICES]
            .clone()
            .vortex_expect("validated row_indices slot");
        let values: Vec<ArrayRef> = slots[InterleaveSlots::VALUES_OFFSET..]
            .iter()
            .map(|s| s.clone().vortex_expect("validated value slot"))
            .collect();

        // All semantic invariants live in `check`; here we only confirm the array's cached `dtype`
        // and `len` agree with what the children imply.
        let expected_dtype = Interleave::check(&values, &array_indices, &row_indices)?;
        vortex_ensure_eq!(
            dtype,
            &expected_dtype,
            "InterleaveArray dtype does not match the dtype implied by its children",
        );
        vortex_ensure_eq!(
            len,
            array_indices.len(),
            "InterleaveArray length does not match array_indices length",
        );
        Ok(())
    }

    fn nbuffers(_array: ArrayView<'_, Self>) -> usize {
        0
    }

    fn buffer(_array: ArrayView<'_, Self>, _idx: usize) -> BufferHandle {
        vortex_panic!("InterleaveArray has no buffers")
    }

    fn buffer_name(_array: ArrayView<'_, Self>, _idx: usize) -> Option<String> {
        None
    }

    fn with_buffers(
        &self,
        array: ArrayView<'_, Self>,
        buffers: &[BufferHandle],
    ) -> VortexResult<ArrayParts<Self>> {
        with_empty_buffers(self, array, buffers)
    }

    fn slot_name(_array: ArrayView<'_, Self>, idx: usize) -> String {
        InterleaveSlots::slot_name(idx)
    }

    fn serialize(
        _array: ArrayView<'_, Self>,
        _session: &VortexSession,
    ) -> VortexResult<Option<Vec<u8>>> {
        vortex_bail!("Interleave array is not serializable")
    }

    fn deserialize(
        &self,
        _dtype: &DType,
        _len: usize,
        _metadata: &[u8],
        _buffers: &[BufferHandle],
        _children: &dyn ArrayChildren,
        _session: &VortexSession,
    ) -> VortexResult<ArrayParts<Self>> {
        vortex_bail!("Interleave array is not serializable")
    }

    fn execute(array: Array<Self>, ctx: &mut ExecutionCtx) -> VortexResult<ExecutionResult> {
        execute::execute(array, ctx)
    }
}

impl OperationsVTable<Interleave> for Interleave {
    type ProbeState = ();

    fn scalar_at(
        array: ArrayView<'_, Interleave>,
        index: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        // Random-access gather: read the routing pair for `index` directly, then pull that row from
        // the selected value array. No cursor walk is required.
        let branch_idx = array
            .array_indices()
            .execute_scalar(index, ctx)?
            .as_primitive()
            .as_::<usize>()
            .vortex_expect("interleave array_indices is non-nullable");
        let row = array
            .row_indices()
            .execute_scalar(index, ctx)?
            .as_primitive()
            .as_::<usize>()
            .vortex_expect("interleave row_indices is non-nullable");

        let scalar = array.value(branch_idx).execute_scalar(row, ctx)?;
        // The value may be non-nullable while the interleaved output is nullable; align the dtype.
        Ok(if array.as_ref().dtype().is_nullable() {
            scalar.into_nullable()
        } else {
            scalar
        })
    }
}

impl ValidityVTable<Interleave> for Interleave {
    fn validity(array: ArrayView<'_, Interleave>) -> VortexResult<Validity> {
        if !array.as_ref().dtype().is_nullable() {
            return Ok(Validity::NonNullable);
        }
        let num_values = array.num_values();
        let mut slots: ArraySlots =
            ArraySlots::with_capacity(num_values + InterleaveSlots::FIXED_COUNT);
        slots.push(Some(array.array_indices().clone()));
        slots.push(Some(array.row_indices().clone()));
        for i in 0..num_values {
            slots.push(Some(value_validity_array(array.value(i))?));
        }
        // SAFETY: `value_validity_array` yields a non-nullable boolean array per value,
        // the selectors already validated.
        let interleaved = unsafe {
            InterleaveArray::new_unchecked_slots(slots, DType::Bool(Nullability::NonNullable))
        };
        Ok(Validity::Array(interleaved.into_array()))
    }
}

/// Materializes a value's validity as a non-nullable boolean array of the value's length, where
/// `true` marks a valid (non-null) row.
fn value_validity_array(value: &ArrayRef) -> VortexResult<ArrayRef> {
    Ok(match value.validity()? {
        Validity::NonNullable | Validity::AllValid => {
            ConstantArray::new(true, value.len()).into_array()
        }
        Validity::AllInvalid => ConstantArray::new(false, value.len()).into_array(),
        Validity::Array(array) => array,
    })
}

#[cfg(test)]
mod tests;
