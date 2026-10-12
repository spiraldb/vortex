// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;

use vortex_array::AnyCanonical;
use vortex_array::Array;
use vortex_array::ArrayEq;
use vortex_array::ArrayHash;
use vortex_array::ArrayId;
use vortex_array::ArrayParts;
use vortex_array::ArrayRef;
use vortex_array::ArrayView;
use vortex_array::EqMode;
use vortex_array::ExecutionCtx;
use vortex_array::ExecutionResult;
use vortex_array::IntoArray;
use vortex_array::TypedArrayRef;
use vortex_array::VortexSessionExecute;
use vortex_array::array_slots;
use vortex_array::arrays::Bool;
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::DecimalArray;
use vortex_array::arrays::ListViewArray;
use vortex_array::arrays::Primitive;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::arrays::bool::BoolArrayExt;
use vortex_array::arrays::listview::ListViewArraySlotsExt;
use vortex_array::buffer::BufferHandle;
use vortex_array::dtype::DType;
use vortex_array::dtype::PType;
use vortex_array::legacy_session;
use vortex_array::match_each_unsigned_integer_ptype;
use vortex_array::require_child;
use vortex_array::serde::ArrayChildren;
use vortex_array::validity::Validity;
use vortex_array::vtable::VTable;
use vortex_array::vtable::ValidityVTable;
use vortex_buffer::BitBuffer;
use vortex_error::VortexExpect as _;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_panic;
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

use crate::compress::BoolRuns;
use crate::compress::runend_decode_decimal;
use crate::compress::runend_decode_primitive;
use crate::compress::runend_decode_varbinview;
use crate::compress::runend_encode;
use crate::compress::runend_encode_bool;
use crate::decompress_bool::runend_decode_bools;
use crate::iter::trimmed_ends_iter;
use crate::ops::find_physical_index;
use crate::ops::find_slice_end_index;
use crate::rules::RULES;

/// A [`RunEnd`]-encoded Vortex array.
pub type RunEndArray = Array<RunEnd>;

#[derive(Clone, prost::Message)]
pub struct RunEndMetadata {
    #[prost(enumeration = "PType", tag = "1")]
    pub ends_ptype: i32,
    #[prost(uint64, tag = "2")]
    pub num_runs: u64,
    #[prost(uint64, tag = "3")]
    pub offset: u64,
}

impl ArrayHash for RunEndData {
    fn array_hash<H: Hasher>(&self, state: &mut H, _accuracy: EqMode) {
        self.offset.hash(state);
        self.alternating_start.hash(state);
    }
}

impl ArrayEq for RunEndData {
    fn array_eq(&self, other: &Self, _accuracy: EqMode) -> bool {
        self.offset == other.offset && self.alternating_start == other.alternating_start
    }
}

impl VTable for RunEnd {
    type TypedArrayData = RunEndData;

    type OperationsVTable = Self;
    type ValidityVTable = Self;

    fn id(&self) -> ArrayId {
        static ID: CachedId = CachedId::new("vortex.runend");
        *ID
    }

    #[allow(clippy::disallowed_methods)]
    fn validate(
        &self,
        data: &Self::TypedArrayData,
        dtype: &DType,
        len: usize,
        slots: &[Option<ArrayRef>],
    ) -> VortexResult<()> {
        let run_end_slots = RunEndSlotsView::from_slots(slots);
        let ends = run_end_slots.ends;
        let values = run_end_slots.values;
        // TODO(ctx): trait fixes - VTable::validate has a fixed signature.
        let mut ctx = legacy_session().create_execution_ctx();
        RunEndData::validate_parts(ends, values, data.offset, len, &mut ctx)?;
        vortex_ensure_eq!(values.dtype(), dtype);
        if data.alternating_start.is_some() {
            vortex_ensure!(
                dtype.is_boolean(),
                "Only boolean RunEnd values can alternate, got {dtype}"
            );
        }
        Ok(())
    }

    fn nbuffers(_array: ArrayView<'_, Self>) -> usize {
        0
    }

    fn buffer(_array: ArrayView<'_, Self>, idx: usize) -> BufferHandle {
        vortex_panic!("RunEndArray buffer index {idx} out of bounds")
    }

    fn buffer_name(_array: ArrayView<'_, Self>, idx: usize) -> Option<String> {
        vortex_panic!("RunEndArray buffer_name index {idx} out of bounds")
    }

    fn with_buffers(
        &self,
        array: ArrayView<'_, Self>,
        buffers: &[BufferHandle],
    ) -> VortexResult<ArrayParts<Self>> {
        vortex_array::vtable::with_empty_buffers(self, array, buffers)
    }

    fn serialize(
        _array: ArrayView<'_, Self>,
        _session: &VortexSession,
    ) -> VortexResult<Option<Vec<u8>>> {
        vortex_bail!("RunEnd serialization requires RunEndPlugin")
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
        vortex_bail!("RunEnd deserialization requires RunEndPlugin")
    }

    fn slot_name(_array: ArrayView<'_, Self>, idx: usize) -> String {
        RunEndSlots::NAMES[idx].to_string()
    }

    fn reduce_parent(
        array: ArrayView<'_, Self>,
        parent: &ArrayRef,
        child_idx: usize,
    ) -> VortexResult<Option<ArrayRef>> {
        RULES.evaluate(array, parent, child_idx)
    }

    fn execute(array: Array<Self>, ctx: &mut ExecutionCtx) -> VortexResult<ExecutionResult> {
        let array = require_child!(array, array.ends(), RunEndSlots::ENDS => Primitive);
        let array = require_child!(array, array.values(), RunEndSlots::VALUES => AnyCanonical);
        run_end_canonicalize(&array, ctx).map(ExecutionResult::done)
    }
}

#[array_slots(RunEnd)]
pub struct RunEndSlots {
    /// The run-end positions marking where each run terminates.
    #[slot(0)]
    pub ends: ArrayRef,
    /// The values for each run.
    #[slot(1)]
    pub values: ArrayRef,
}

#[derive(Clone, Debug)]
pub struct RunEndData {
    offset: usize,
    /// When set, the values are booleans that alternate from run to run, beginning with this
    /// value for the first physical run.
    alternating_start: Option<bool>,
}

impl Display for RunEndData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "offset: {}", self.offset)?;
        if let Some(start) = self.alternating_start {
            write!(f, ", alternating_start: {start}")?;
        }
        Ok(())
    }
}

pub struct RunEndDataParts {
    pub ends: ArrayRef,
    pub values: ArrayRef,
    pub offset: usize,
    pub alternating_start: Option<bool>,
}

pub trait RunEndArrayExt: RunEndArraySlotsExt {
    fn offset(&self) -> usize {
        self.offset
    }

    /// The value of the first physical run when the boolean values alternate between runs.
    ///
    /// Run `i` then holds `start ^ (i % 2 == 1)`, or null. Such arrays serialize without their
    /// values as `vortex.runend.v2`. Boolean arrays with explicit values, which may repeat a
    /// value in adjacent runs such as `[false, false, true]`, return `None`.
    fn alternating_start(&self) -> Option<bool> {
        self.alternating_start
    }

    fn dtype(&self) -> &DType {
        self.values().dtype()
    }

    fn find_physical_index(&self, index: usize, ctx: &mut ExecutionCtx) -> VortexResult<usize> {
        find_physical_index(self.ends(), index + self.offset(), ctx)
    }

    fn find_slice_end_index(&self, index: usize, ctx: &mut ExecutionCtx) -> VortexResult<usize> {
        find_slice_end_index(self.ends(), index + self.offset(), ctx)
    }
}

impl<T: TypedArrayRef<RunEnd>> RunEndArrayExt for T {}

#[derive(Clone, Debug)]
pub struct RunEnd;

impl RunEnd {
    /// Build a new [`RunEndArray`] without validation.
    ///
    /// # Safety
    /// See [`RunEndData::new_unchecked`] for preconditions.
    pub unsafe fn new_unchecked(
        ends: ArrayRef,
        values: ArrayRef,
        offset: usize,
        length: usize,
    ) -> RunEndArray {
        let dtype = values.dtype().clone();
        let slots = RunEndSlots { ends, values }.into_slots();
        let data = unsafe { RunEndData::new_unchecked(offset) };
        unsafe { Array::from_parts_unchecked(ArrayParts::new(RunEnd, dtype, length, data, slots)) }
    }

    /// Build a new [`RunEndArray`] from ends and values.
    pub fn try_new(
        ends: ArrayRef,
        values: ArrayRef,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<RunEndArray> {
        let len = RunEndData::logical_len_from_ends(&ends, ctx)?;
        RunEndData::validate_parts(&ends, &values, 0, len, ctx)?;
        let dtype = values.dtype().clone();
        let slots = RunEndSlots { ends, values }.into_slots();
        let data = RunEndData::new(0);
        Array::try_from_parts(ArrayParts::new(RunEnd, dtype, len, data, slots))
    }

    /// Build a new [`RunEndArray`] from ends, values, offset, and length.
    pub fn try_new_offset_length(
        ends: ArrayRef,
        values: ArrayRef,
        offset: usize,
        length: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<RunEndArray> {
        RunEndData::validate_parts(&ends, &values, offset, length, ctx)?;
        let dtype = values.dtype().clone();
        let slots = RunEndSlots { ends, values }.into_slots();
        let data = RunEndData::new(offset);
        Array::try_from_parts(ArrayParts::new(RunEnd, dtype, length, data, slots))
    }

    /// Build a new [`RunEndArray`] from ends and values (panics on invalid input).
    pub fn new(ends: ArrayRef, values: ArrayRef, ctx: &mut ExecutionCtx) -> RunEndArray {
        Self::try_new(ends, values, ctx).vortex_expect("RunEndData is always valid")
    }

    /// Build a new boolean [`RunEndArray`] whose run values alternate, beginning with `start`.
    ///
    /// `validity` holds one entry per run. The values of null runs still take part in the
    /// alternation, so a valid run `i` holds `start ^ (i % 2 == 1)`.
    pub fn try_new_alternating(
        ends: ArrayRef,
        start: bool,
        validity: Validity,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<RunEndArray> {
        let len = RunEndData::logical_len_from_ends(&ends, ctx)?;
        Self::try_new_alternating_offset_length(ends, start, validity, 0, len, ctx)
    }

    /// Build a new boolean [`RunEndArray`] whose run values alternate, beginning with `start`,
    /// covering `offset..offset + length` of the runs.
    pub fn try_new_alternating_offset_length(
        ends: ArrayRef,
        start: bool,
        validity: Validity,
        offset: usize,
        length: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<RunEndArray> {
        let values = alternating_values(start, ends.len(), validity)?.into_array();
        RunEndData::validate_parts(&ends, &values, offset, length, ctx)?;
        let dtype = values.dtype().clone();
        let slots = RunEndSlots { ends, values }.into_slots();
        let data = RunEndData {
            offset,
            alternating_start: Some(start),
        };
        Array::try_from_parts(ArrayParts::new(RunEnd, dtype, length, data, slots))
    }

    /// Build a new alternating boolean [`RunEndArray`] without validation.
    ///
    /// # Safety
    /// See [`RunEndData::new_unchecked`] for preconditions. In addition, `values` must be booleans
    /// whose valid entries alternate from run to run, with physical run 0 holding `start`.
    pub(crate) unsafe fn new_alternating_unchecked(
        ends: ArrayRef,
        values: ArrayRef,
        start: bool,
        offset: usize,
        length: usize,
    ) -> RunEndArray {
        let dtype = values.dtype().clone();
        let slots = RunEndSlots { ends, values }.into_slots();
        let data = RunEndData {
            offset,
            alternating_start: Some(start),
        };
        unsafe { Array::from_parts_unchecked(ArrayParts::new(RunEnd, dtype, length, data, slots)) }
    }

    /// Run the array through run-end encoding.
    ///
    /// Boolean arrays are encoded with explicit run values, which every reader of
    /// `vortex.runend` understands. Pass the result to [`RunEnd::normalize_bool`] to drop the
    /// values from the serialized form.
    pub fn encode(array: ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<RunEndArray> {
        let len = array.len();
        let (ends, values) = if let Some(parray) = array.as_opt::<Primitive>() {
            runend_encode(parray, ctx)
        } else if let Some(barray) = array.as_opt::<Bool>() {
            let (ends, values) = runend_encode_bool(barray, ctx)?;
            (ends, values.into_array())
        } else {
            vortex_bail!("REE can only encode primitive and boolean arrays")
        };
        let ends = ends.into_array();
        let dtype = values.dtype().clone();
        let slots = RunEndSlots { ends, values }.into_slots();
        let data = unsafe { RunEndData::new_unchecked(0) };
        Array::try_from_parts(ArrayParts::new(RunEnd, dtype, len, data, slots))
    }

    /// Normalize a boolean [`RunEndArray`] so that its values alternate between runs.
    ///
    /// Adjacent runs with the same value, such as the first two runs of `[false, false, true]`,
    /// are merged, along with adjacent null runs, and the offset is folded into the ends. The
    /// result is alternating, see [`RunEndArrayExt::alternating_start`], unless a null run sits
    /// between two runs of the same value, as in `[true, null, true]`; those runs keep explicit
    /// values.
    pub fn normalize_bool(
        array: &RunEndArray,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<RunEndArray> {
        vortex_ensure!(
            array.dtype().is_boolean(),
            "Only boolean RunEnd arrays can be normalized, got {}",
            array.dtype()
        );
        if array.alternating_start().is_some() {
            return Ok(array.clone());
        }

        let nullability = array.dtype().nullability();
        let runs = if array.is_empty() {
            BoolRuns::default()
        } else {
            let ends = array.ends().clone().execute::<PrimitiveArray>(ctx)?;
            let values = array.values().clone().execute::<BoolArray>(ctx)?;
            let validity = values
                .as_ref()
                .validity()?
                .execute_mask(values.len(), ctx)?;
            let bits = values.to_bit_buffer();
            let mut runs = BoolRuns::default();
            match_each_unsigned_integer_ptype!(ends.ptype(), |E| {
                for (run, end) in
                    trimmed_ends_iter(ends.as_slice::<E>(), array.offset(), array.len()).enumerate()
                {
                    runs.push(end as u64, validity.value(run).then(|| bits.value(run)));
                }
            });
            runs
        };

        let start = runs.alternating_start();
        let (ends, values) = runs.into_arrays(nullability, start, ctx)?;
        let data = RunEndData {
            offset: 0,
            alternating_start: start,
        };
        let slots = RunEndSlots {
            ends: ends.into_array(),
            values: values.into_array(),
        }
        .into_slots();
        Array::try_from_parts(ArrayParts::new(
            RunEnd,
            array.dtype().clone(),
            array.len(),
            data,
            slots,
        ))
    }
}

/// `num_runs` booleans that alternate, beginning with `start`.
fn alternating_values(
    start: bool,
    num_runs: usize,
    validity: Validity,
) -> VortexResult<BoolArray> {
    let bits = BitBuffer::from_iter((0..num_runs).map(|run| start ^ (run % 2 == 1)));
    BoolArray::try_new(bits, validity)
}

impl RunEndData {
    fn logical_len_from_ends(ends: &ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<usize> {
        if ends.is_empty() {
            Ok(0)
        } else {
            usize::try_from(&ends.execute_scalar(ends.len() - 1, ctx)?)
        }
    }

    /// Validate that `ends` and `values` form a well-formed run-end array covering
    /// `offset..offset + length`.
    pub fn validate_parts(
        ends: &ArrayRef,
        values: &ArrayRef,
        offset: usize,
        length: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<()> {
        // DType validation
        vortex_ensure!(
            ends.dtype().is_unsigned_int(),
            "run ends must be unsigned integers, was {}",
            ends.dtype(),
        );
        vortex_ensure_eq!(ends.len(), values.len());

        // Handle empty run-ends
        if ends.is_empty() {
            vortex_ensure_eq!(offset, 0, "non-zero offset provided for empty RunEndArray");
            return Ok(());
        }

        // Zero-length logical slices may retain run metadata from the source array.
        if length == 0 {
            return Ok(());
        }

        #[cfg(debug_assertions)]
        {
            // Run ends must be strictly sorted for binary search to work correctly.
            let pre_validation = ends.statistics().to_owned();

            let is_sorted = ends
                .statistics()
                .compute_is_strict_sorted(ctx)
                .unwrap_or(false);

            // Preserve the original statistics since compute_is_strict_sorted may have mutated them.
            // We don't want to run with different stats in debug mode and outside.
            ends.statistics().inherit(pre_validation.iter());
            debug_assert!(is_sorted);
        }

        // Skip host-only validation when ends are not host-resident.
        if !ends.is_host() {
            return Ok(());
        }

        // Validate the offset and length are valid for the given ends and values
        if offset != 0 && length != 0 {
            let first_run_end = usize::try_from(&ends.execute_scalar(0, ctx)?)?;
            if first_run_end < offset {
                vortex_bail!("First run end {first_run_end} must be >= offset {offset}");
            }
        }

        let last_run_end = usize::try_from(&ends.execute_scalar(ends.len() - 1, ctx)?)?;
        let min_required_end = offset + length;
        if last_run_end < min_required_end {
            vortex_bail!("Last run end {last_run_end} must be >= offset+length {min_required_end}");
        }

        Ok(())
    }
}

impl RunEndData {
    /// Build a new `RunEndArray` from an array of run `ends` and an array of `values`.
    ///
    /// Panics if any of the validation conditions described in [`RunEnd::try_new`] is
    /// not satisfied.
    ///
    /// # Examples
    ///
    /// ```
    /// # use vortex_array::arrays::BoolArray;
    /// # use vortex_array::IntoArray;
    /// # use vortex_array::VortexSessionExecute;
    /// # use vortex_buffer::buffer;
    /// # use vortex_error::VortexResult;
    /// # use vortex_runend::RunEnd;
    /// # fn main() -> VortexResult<()> {
    /// let session = vortex_array::array_session();
    /// vortex_runend::initialize(&session);
    /// let mut ctx = session.create_execution_ctx();
    /// let ends = buffer![2u8, 3u8].into_array();
    /// let values = BoolArray::from_iter([false, true]).into_array();
    /// let run_end = RunEnd::new(ends, values, &mut ctx);
    ///
    /// // Array encodes
    /// assert_eq!(run_end.execute_scalar(0, &mut ctx)?, false.into());
    /// assert_eq!(run_end.execute_scalar(1, &mut ctx)?, false.into());
    /// assert_eq!(run_end.execute_scalar(2, &mut ctx)?, true.into());
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(offset: usize) -> Self {
        Self {
            offset,
            alternating_start: None,
        }
    }

    /// Build a new `RunEndArray` without validation.
    ///
    /// # Safety
    ///
    /// The caller must ensure that all the validation performed in
    /// [`RunEnd::try_new_offset_length`] is
    /// satisfied before calling this function.
    ///
    /// See [`RunEnd::try_new_offset_length`] for the preconditions needed to build a new array.
    pub unsafe fn new_unchecked(offset: usize) -> Self {
        Self {
            offset,
            alternating_start: None,
        }
    }

    /// Run the array through run-end encoding.
    pub fn encode(array: ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<Self> {
        if let Some(parray) = array.as_opt::<Primitive>() {
            let (_ends, _values) = runend_encode(parray, ctx);
            // SAFETY: runend_encode handles this
            unsafe { Ok(Self::new_unchecked(0)) }
        } else {
            vortex_bail!("REE can only encode primitive arrays")
        }
    }

    pub fn into_parts(self, ends: ArrayRef, values: ArrayRef) -> RunEndDataParts {
        RunEndDataParts {
            ends,
            values,
            offset: self.offset,
            alternating_start: self.alternating_start,
        }
    }
}

impl ValidityVTable<RunEnd> for RunEnd {
    fn validity(array: ArrayView<'_, RunEnd>) -> VortexResult<Validity> {
        Ok(match array.values().validity()? {
            Validity::NonNullable | Validity::AllValid => Validity::AllValid,
            Validity::AllInvalid => Validity::AllInvalid,
            Validity::Array(values_validity) => Validity::Array(unsafe {
                RunEnd::new_unchecked(
                    array.ends().clone(),
                    values_validity,
                    array.offset(),
                    array.len(),
                )
                .into_array()
            }),
        })
    }
}

pub(super) fn run_end_canonicalize(
    array: &RunEndArray,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ArrayRef> {
    let pends = array.ends().clone().execute_as("ends", ctx)?;

    Ok(match array.dtype() {
        DType::Bool(_) => {
            let bools = array.values().clone().execute_as("values", ctx)?;
            runend_decode_bools(pends, bools, array.offset(), array.len(), ctx)?
        }
        DType::Primitive(..) => {
            let pvalues = array.values().clone().execute_as("values", ctx)?;
            runend_decode_primitive(pends, pvalues, array.offset(), array.len(), ctx)?
        }
        DType::Decimal(..) => {
            let values = array
                .values()
                .clone()
                .execute_as::<DecimalArray>("values", ctx)?;
            runend_decode_decimal(pends, values, array.offset(), array.len(), ctx)?
        }
        DType::Utf8(_) | DType::Binary(_) => {
            let values = array
                .values()
                .clone()
                .execute_as::<VarBinViewArray>("values", ctx)?;
            runend_decode_varbinview(pends, values, array.offset(), array.len(), ctx)?
        }
        DType::List(..) => {
            let values = array
                .values()
                .clone()
                .execute_as::<ListViewArray>("values", ctx)?;
            runend_decode_listview(pends, values, array.offset(), array.len())?.into_array()
        }
        _ => vortex_bail!("Unsupported RunEnd value type: {}", array.dtype()),
    })
}

fn runend_decode_listview(
    ends: PrimitiveArray,
    values: ListViewArray,
    offset: usize,
    length: usize,
) -> VortexResult<ListViewArray> {
    let validity = match values.validity()? {
        Validity::NonNullable => Validity::NonNullable,
        Validity::AllValid => Validity::AllValid,
        Validity::AllInvalid => Validity::AllInvalid,
        Validity::Array(validity) => Validity::Array(unsafe {
            RunEnd::new_unchecked(ends.clone().into_array(), validity, offset, length).into_array()
        }),
    };

    // SAFETY: the `RunEndArray`s re-express valid per-run ListView metadata over the logical output
    // length. The original `elements` child is reused, so every view still points at a valid range.
    Ok(unsafe {
        ListViewArray::new_unchecked(
            values.elements().clone(),
            RunEnd::new_unchecked(
                ends.clone().into_array(),
                values.offsets().clone(),
                offset,
                length,
            )
            .into_array(),
            RunEnd::new_unchecked(ends.into_array(), values.sizes().clone(), offset, length)
                .into_array(),
            validity,
        )
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::LazyLock;

    use vortex_array::IntoArray;
    use vortex_array::VortexSessionExecute;
    use vortex_array::arrays::BoolArray;
    use vortex_array::arrays::DecimalArray;
    use vortex_array::arrays::DictArray;
    use vortex_array::arrays::ListArray;
    use vortex_array::arrays::ListViewArray;
    use vortex_array::arrays::VarBinViewArray;
    use vortex_array::arrays::listview::ListViewArraySlotsExt;
    use vortex_array::assert_arrays_eq;
    use vortex_array::builders::VarBinBuilder;
    use vortex_array::dtype::DType;
    use vortex_array::dtype::DecimalDType;
    use vortex_array::dtype::Nullability;
    use vortex_array::dtype::PType;
    use vortex_array::dtype::i256;
    use vortex_array::validity::Validity;
    use vortex_buffer::buffer;
    use vortex_error::VortexResult;
    use vortex_session::VortexSession;

    use crate::RunEnd;
    use crate::RunEndArrayExt;
    use crate::RunEndArraySlotsExt;

    static SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
        let session = vortex_array::array_session();
        crate::initialize(&session);
        session
    });

    #[test]
    fn test_runend_constructor() {
        let mut ctx = SESSION.create_execution_ctx();
        let arr = RunEnd::new(
            buffer![2u32, 5, 10].into_array(),
            buffer![1i32, 2, 3].into_array(),
            &mut ctx,
        );
        assert_eq!(arr.len(), 10);
        assert_eq!(
            arr.dtype(),
            &DType::Primitive(PType::I32, Nullability::NonNullable)
        );

        // 0, 1 => 1
        // 2, 3, 4 => 2
        // 5, 6, 7, 8, 9 => 3
        let expected = buffer![1, 1, 2, 2, 2, 3, 3, 3, 3, 3].into_array();
        assert_arrays_eq!(arr.into_array(), expected, &mut ctx);
    }

    #[test]
    fn test_runend_utf8() {
        let mut ctx = SESSION.create_execution_ctx();
        let values =
            VarBinViewArray::from_iter_nullable_str([Some("a"), None, Some("c")]).into_array();
        let arr = RunEnd::new(buffer![2u32, 5, 10].into_array(), values, &mut ctx);
        assert_eq!(arr.len(), 10);
        assert_eq!(arr.dtype(), &DType::Utf8(Nullability::Nullable));

        let expected = VarBinViewArray::from_iter_nullable_str([
            Some("a"),
            Some("a"),
            None,
            None,
            None,
            Some("c"),
            Some("c"),
            Some("c"),
            Some("c"),
            Some("c"),
        ])
        .into_array();
        let mut builder = VarBinBuilder::<i32>::with_capacity_in(
            arr.dtype().clone(),
            arr.len(),
            vortex_buffer::BufferAllocatorRef::static_ref(),
        );
        arr.append_to_builder(&mut builder, &mut ctx).unwrap();
        assert_arrays_eq!(builder.finish_into_varbin(), expected, &mut ctx);
        assert_arrays_eq!(arr.into_array(), expected, &mut ctx);
    }

    #[test]
    fn test_runend_decimal() {
        let mut ctx = SESSION.create_execution_ctx();
        let decimal_dtype = DecimalDType::new(10, 2);
        let values = DecimalArray::from_iter([12345i64, 67890, -12300], decimal_dtype).into_array();
        let arr = RunEnd::new(buffer![2u32, 5, 10].into_array(), values, &mut ctx);
        assert_eq!(arr.len(), 10);
        assert_eq!(
            arr.dtype(),
            &DType::Decimal(decimal_dtype, Nullability::NonNullable)
        );

        let expected = DecimalArray::from_iter(
            [
                12345i64, 12345, 67890, 67890, 67890, -12300, -12300, -12300, -12300, -12300,
            ],
            decimal_dtype,
        )
        .into_array();
        assert_arrays_eq!(arr.into_array(), expected, &mut ctx);
    }

    #[test]
    fn test_runend_list_i64() {
        let mut ctx = SESSION.create_execution_ctx();
        let values = ListArray::from_iter_slow::<u32, _>(
            vec![vec![1i64, 2], vec![3], vec![4, 5, 6]],
            Arc::new(DType::Primitive(PType::I64, Nullability::NonNullable)),
        )
        .unwrap()
        .into_array();
        let arr = RunEnd::new(buffer![2u32, 5, 10].into_array(), values, &mut ctx);

        let expected = ListArray::from_iter_slow::<u32, _>(
            vec![
                vec![1i64, 2],
                vec![1, 2],
                vec![3],
                vec![3],
                vec![3],
                vec![4, 5, 6],
                vec![4, 5, 6],
                vec![4, 5, 6],
                vec![4, 5, 6],
                vec![4, 5, 6],
            ],
            Arc::new(DType::Primitive(PType::I64, Nullability::NonNullable)),
        )
        .unwrap()
        .into_array();
        assert_arrays_eq!(arr.into_array(), expected, &mut ctx);
    }

    #[test]
    fn test_runend_nullable_decimal() {
        let mut ctx = SESSION.create_execution_ctx();
        let decimal_dtype = DecimalDType::new(10, 2);
        let values =
            DecimalArray::from_option_iter([Some(12345i64), None, Some(-12300)], decimal_dtype)
                .into_array();
        let arr = RunEnd::new(buffer![2u32, 5, 10].into_array(), values, &mut ctx);
        assert_eq!(arr.len(), 10);
        assert_eq!(
            arr.dtype(),
            &DType::Decimal(decimal_dtype, Nullability::Nullable)
        );

        let expected = DecimalArray::from_option_iter(
            [
                Some(12345i64),
                Some(12345),
                None,
                None,
                None,
                Some(-12300),
                Some(-12300),
                Some(-12300),
                Some(-12300),
                Some(-12300),
            ],
            decimal_dtype,
        )
        .into_array();
        assert_arrays_eq!(arr.into_array(), expected, &mut ctx);
    }

    #[test]
    fn test_runend_list_bool() {
        let mut ctx = SESSION.create_execution_ctx();
        let values = ListArray::from_iter_slow::<u32, _>(
            vec![vec![true, false], vec![false], vec![true, true, false]],
            Arc::new(DType::Bool(Nullability::NonNullable)),
        )
        .unwrap()
        .into_array();
        let arr = RunEnd::new(buffer![2u32, 5, 10].into_array(), values, &mut ctx);

        let expected = ListArray::from_iter_slow::<u32, _>(
            vec![
                vec![true, false],
                vec![true, false],
                vec![false],
                vec![false],
                vec![false],
                vec![true, true, false],
                vec![true, true, false],
                vec![true, true, false],
                vec![true, true, false],
                vec![true, true, false],
            ],
            Arc::new(DType::Bool(Nullability::NonNullable)),
        )
        .unwrap()
        .into_array();
        assert_arrays_eq!(arr.into_array(), expected, &mut ctx);
    }

    #[test]
    fn test_runend_list_utf8() {
        let mut ctx = SESSION.create_execution_ctx();
        let values = ListArray::try_new(
            VarBinViewArray::from_iter_str(["a", "b", "c", "d", "e", "f"]).into_array(),
            buffer![0u32, 2, 3, 6].into_array(),
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        let arr = RunEnd::new(buffer![2u32, 5, 10].into_array(), values, &mut ctx);

        let expected = ListArray::try_new(
            VarBinViewArray::from_iter_str([
                "a", "b", "a", "b", "c", "c", "c", "d", "e", "f", "d", "e", "f", "d", "e", "f",
                "d", "e", "f", "d", "e", "f",
            ])
            .into_array(),
            buffer![0u32, 2, 4, 5, 6, 7, 10, 13, 16, 19, 22].into_array(),
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        assert_arrays_eq!(arr.into_array(), expected, &mut ctx);
    }

    #[test]
    fn test_runend_list_canonicalizes_to_runend_listview_slots() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let values = ListArray::try_new(
            buffer![1i64, 2, 3, 4, 5, 6].into_array(),
            buffer![0u32, 2, 3, 6].into_array(),
            Validity::from_iter([true, false, true]),
        )?
        .into_array();
        let arr = RunEnd::try_new(buffer![2u32, 5, 6].into_array(), values, &mut ctx)?;

        let listview = arr
            .clone()
            .into_array()
            .execute::<ListViewArray>(&mut ctx)?;
        assert!(listview.offsets().is::<RunEnd>());
        assert!(listview.sizes().is::<RunEnd>());
        match listview.validity()? {
            Validity::Array(validity) => assert!(validity.is::<RunEnd>()),
            validity => panic!("expected array-backed validity, got {validity:?}"),
        }

        let expected = ListArray::try_new(
            buffer![1i64, 2, 1, 2, 3, 3, 3, 4, 5, 6].into_array(),
            buffer![0u32, 2, 4, 5, 6, 7, 10].into_array(),
            Validity::from_iter([true, true, false, false, false, true]),
        )?
        .into_array();
        assert_arrays_eq!(arr.into_array(), expected, &mut ctx);
        Ok(())
    }

    #[test]
    fn test_runend_dict() {
        let mut ctx = SESSION.create_execution_ctx();
        let dict_values = VarBinViewArray::from_iter_str(["x", "y", "z"]).into_array();
        let dict_codes = buffer![0u32, 1, 2].into_array();
        let dict = DictArray::try_new(dict_codes, dict_values).unwrap();

        let arr = RunEnd::try_new(
            buffer![2u32, 5, 10].into_array(),
            dict.into_array(),
            &mut ctx,
        )
        .unwrap();
        assert_eq!(arr.len(), 10);

        let expected =
            VarBinViewArray::from_iter_str(["x", "x", "y", "y", "y", "z", "z", "z", "z", "z"])
                .into_array();
        assert_arrays_eq!(arr.into_array(), expected, &mut ctx);
    }

    #[test]
    fn test_runend_decimal_i128() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let decimal_dtype = DecimalDType::new(20, 2);
        let values = DecimalArray::from_iter([12_345i128, -67_890, 100], decimal_dtype);
        let arr = RunEnd::try_new(
            buffer![2u32, 5, 6].into_array(),
            values.into_array(),
            &mut ctx,
        )?;

        let decoded = arr.into_array().execute::<DecimalArray>(&mut ctx)?;
        let expected = DecimalArray::from_iter(
            [12_345i128, 12_345, -67_890, -67_890, -67_890, 100],
            decimal_dtype,
        );
        assert_arrays_eq!(decoded, expected, &mut ctx);
        Ok(())
    }

    #[test]
    fn test_runend_decimal_nullable() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let decimal_dtype = DecimalDType::new(20, 2);
        let values =
            DecimalArray::from_option_iter([Some(12_345i128), None, Some(-67_890)], decimal_dtype);
        let arr = RunEnd::try_new(
            buffer![2u32, 5, 7].into_array(),
            values.into_array(),
            &mut ctx,
        )?;

        let decoded = arr.into_array().execute::<DecimalArray>(&mut ctx)?;
        let expected = DecimalArray::from_option_iter(
            [
                Some(12_345i128),
                Some(12_345),
                None,
                None,
                None,
                Some(-67_890),
                Some(-67_890),
            ],
            decimal_dtype,
        );
        assert_arrays_eq!(decoded, expected, &mut ctx);
        Ok(())
    }

    #[test]
    fn test_runend_decimal_slice() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let decimal_dtype = DecimalDType::new(20, 2);
        let values = DecimalArray::from_iter([100i128, 200, 300], decimal_dtype);
        let arr = RunEnd::try_new(
            buffer![3u32, 5, 10].into_array(),
            values.into_array(),
            &mut ctx,
        )?;

        let sliced = arr.slice(2..8)?;
        let decoded = sliced.execute::<DecimalArray>(&mut ctx)?;
        let expected = DecimalArray::from_iter([100i128, 200, 200, 300, 300, 300], decimal_dtype);
        assert_arrays_eq!(decoded, expected, &mut ctx);
        Ok(())
    }

    #[test]
    fn test_runend_normalize_bool_merges_repeated_runs() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let arr = RunEnd::try_new_offset_length(
            buffer![2u32, 3, 5, 7, 9].into_array(),
            BoolArray::from_iter([true, false, false, true, true]).into_array(),
            1,
            7,
            &mut ctx,
        )?;
        assert_eq!(arr.alternating_start(), None);

        let normalized = RunEnd::normalize_bool(&arr, &mut ctx)?;
        assert_eq!(normalized.alternating_start(), Some(true));
        assert_eq!(normalized.offset(), 0);
        assert_arrays_eq!(normalized.ends(), buffer![1u8, 4, 7].into_array(), &mut ctx);
        assert_arrays_eq!(
            normalized.values(),
            BoolArray::from_iter([true, false, true]).into_array(),
            &mut ctx
        );
        assert_arrays_eq!(normalized.into_array(), arr.into_array(), &mut ctx);
        Ok(())
    }

    #[test]
    fn test_runend_encode_bool() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let bools =
            BoolArray::from_iter([Some(true), Some(true), None, None, Some(false), Some(true)]);
        let arr = RunEnd::encode(bools.clone().into_array(), &mut ctx)?;
        assert_eq!(arr.alternating_start(), None);
        assert_arrays_eq!(arr.ends(), buffer![2u8, 4, 5, 6].into_array(), &mut ctx);
        assert_arrays_eq!(arr.into_array(), bools.into_array(), &mut ctx);
        Ok(())
    }

    #[test]
    fn test_runend_decimal_i256() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let decimal_dtype = DecimalDType::new(40, 4);
        let first = i256::from_i128(123_456);
        let second = i256::from_i128(-789_012);
        let values = DecimalArray::from_iter([first, second], decimal_dtype);
        let arr = RunEnd::try_new(buffer![2u32, 5].into_array(), values.into_array(), &mut ctx)?;

        let decoded = arr.into_array().execute::<DecimalArray>(&mut ctx)?;
        let expected =
            DecimalArray::from_iter([first, first, second, second, second], decimal_dtype);
        assert_arrays_eq!(decoded, expected, &mut ctx);
        Ok(())
    }
}
