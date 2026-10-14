// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Run-end encoded booleans whose values alternate between runs.
//!
//! Merging adjacent equal runs leaves boolean runs that alternate, so their values are implied by
//! the value of the first run. [`RunEndBool`] stores only the run ends, that first value, and the
//! per-run validity of nullable arrays. Boolean [`RunEnd`] arrays, which keep an explicit value
//! per run and may repeat a value, as in `[false, false, true]`, remain valid.

use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::Range;

use prost::Message as _;
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
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::bool::BoolArrayExt;
use vortex_array::arrays::slice::SliceReduce;
use vortex_array::arrays::slice::SliceReduceAdaptor;
use vortex_array::buffer::BufferHandle;
use vortex_array::dtype::DType;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::PType;
use vortex_array::legacy_session;
use vortex_array::match_each_unsigned_integer_ptype;
use vortex_array::optimizer::rules::ParentRuleSet;
use vortex_array::scalar::Scalar;
use vortex_array::serde::ArrayChildren;
use vortex_array::validity::Validity;
use vortex_array::vtable::OperationsVTable;
use vortex_array::vtable::VTable;
use vortex_array::vtable::ValidityVTable;
use vortex_array::vtable::child_to_validity;
use vortex_array::vtable::validity_to_child;
use vortex_buffer::BitBuffer;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_panic;
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

use crate::RunEnd;
use crate::RunEndArray;
use crate::RunEndArrayExt;
use crate::RunEndArraySlotsExt;
use crate::RunEndData;
use crate::compress::BoolRuns;
use crate::iter::trimmed_ends_iter;
use crate::ops::find_physical_index;

#[cfg(test)]
mod tests;

/// A [`RunEndBool`]-encoded Vortex array.
pub type RunEndBoolArray = Array<RunEndBool>;

/// Metadata for `vortex.runend.v2`. The run ends are the first child and the per-run validity,
/// when present, is the second.
#[derive(Clone, prost::Message)]
pub struct RunEndBoolMetadata {
    #[prost(enumeration = "PType", tag = "1")]
    pub ends_ptype: i32,
    #[prost(uint64, tag = "2")]
    pub num_runs: u64,
    #[prost(uint64, tag = "3")]
    pub offset: u64,
    /// The value of the first run. Run `i` holds `start ^ (i % 2 == 1)`, or null.
    #[prost(bool, tag = "4")]
    pub start: bool,
}

#[array_slots(RunEndBool)]
pub struct RunEndBoolSlots {
    /// The run-end positions marking where each run terminates.
    #[slot(0)]
    pub ends: ArrayRef,
    /// The validity of each run, when the array is nullable and has nulls.
    #[slot(1)]
    pub run_validity: Option<ArrayRef>,
}

#[derive(Clone, Debug)]
pub struct RunEndBoolData {
    offset: usize,
    start: bool,
}

impl Display for RunEndBoolData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "offset: {}, start: {}", self.offset, self.start)
    }
}

impl ArrayHash for RunEndBoolData {
    fn array_hash<H: Hasher>(&self, state: &mut H, _accuracy: EqMode) {
        self.offset.hash(state);
        self.start.hash(state);
    }
}

impl ArrayEq for RunEndBoolData {
    fn array_eq(&self, other: &Self, _accuracy: EqMode) -> bool {
        self.offset == other.offset && self.start == other.start
    }
}

pub trait RunEndBoolArrayExt: RunEndBoolArraySlotsExt {
    fn offset(&self) -> usize {
        self.offset
    }

    /// The value of the first physical run.
    fn start(&self) -> bool {
        self.start
    }

    /// The value of physical run `run`, ignoring validity.
    fn run_value(&self, run: usize) -> bool {
        self.start ^ (run % 2 == 1)
    }

    /// The validity of each run.
    fn per_run_validity(&self) -> Validity {
        child_to_validity(self.run_validity(), self.as_ref().dtype().nullability())
    }

    /// The equivalent [`RunEndArray`], with an explicit value per run.
    fn to_run_end(&self) -> VortexResult<RunEndArray> {
        let values = alternating_values(self.start, self.ends().len(), self.per_run_validity())?;
        // SAFETY: the ends, offset, and length were validated for this array.
        Ok(unsafe {
            RunEnd::new_unchecked(
                self.ends().clone(),
                values.into_array(),
                self.offset,
                self.as_ref().len(),
            )
        })
    }
}

impl<T: TypedArrayRef<RunEndBool>> RunEndBoolArrayExt for T {}

#[derive(Clone, Debug)]
pub struct RunEndBool;

impl RunEndBool {
    /// Build a new [`RunEndBoolArray`] from run ends, the value of the first run, and the
    /// validity of each run.
    pub fn try_new(
        ends: ArrayRef,
        start: bool,
        validity: Validity,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<RunEndBoolArray> {
        let len = RunEndData::logical_len_from_ends(&ends, ctx)?;
        Self::try_new_offset_length(ends, start, validity, 0, len, ctx)
    }

    /// Build a new [`RunEndBoolArray`] covering `offset..offset + length` of its runs.
    pub fn try_new_offset_length(
        ends: ArrayRef,
        start: bool,
        validity: Validity,
        offset: usize,
        length: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<RunEndBoolArray> {
        RunEndData::validate_ends(&ends, offset, length, ctx)?;
        let dtype = DType::Bool(validity.nullability());
        let num_runs = ends.len();
        let slots = RunEndBoolSlots {
            ends,
            run_validity: validity_to_child(&validity, num_runs),
        }
        .into_slots();
        let data = RunEndBoolData { offset, start };
        Array::try_from_parts(ArrayParts::new(RunEndBool, dtype, length, data, slots))
    }

    /// Run-end encode a boolean array, or return `None` if a null run separates two runs of the
    /// same value, as in `[true, null, true]`. Use [`RunEnd::encode`] for such arrays.
    pub fn encode(
        array: &ArrayRef,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<RunEndBoolArray>> {
        let bools = array.clone().execute::<BoolArray>(ctx)?;
        let runs = BoolRuns::from_bools(bools.as_view(), ctx)?;
        Self::from_runs(runs, array.dtype().nullability(), array.len(), ctx)
    }

    /// Convert a boolean [`RunEndArray`] by merging adjacent runs with the same value, such as
    /// the first two runs of `[false, false, true]`. Returns `None` if a null run separates two
    /// runs of the same value.
    pub fn from_run_end(
        array: &RunEndArray,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<RunEndBoolArray>> {
        vortex_ensure!(
            array.dtype().is_boolean(),
            "RunEndBool requires boolean values, got {}",
            array.dtype()
        );
        let mut runs = BoolRuns::default();
        if !array.is_empty() {
            let ends = array.ends().clone().execute::<PrimitiveArray>(ctx)?;
            let values = array.values().clone().execute::<BoolArray>(ctx)?;
            let validity = values
                .as_ref()
                .validity()?
                .execute_mask(values.len(), ctx)?;
            let bits = values.to_bit_buffer();
            match_each_unsigned_integer_ptype!(ends.ptype(), |E| {
                let ends = trimmed_ends_iter(ends.as_slice::<E>(), array.offset(), array.len());
                for (run, end) in ends.enumerate() {
                    runs.push(end as u64, validity.value(run).then(|| bits.value(run)));
                }
            });
        }
        Self::from_runs(runs, array.dtype().nullability(), array.len(), ctx)
    }

    fn from_runs(
        runs: BoolRuns,
        nullability: Nullability,
        len: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<RunEndBoolArray>> {
        let Some((ends, start, validity)) = runs.into_alternating(nullability, ctx)? else {
            return Ok(None);
        };
        let dtype = DType::Bool(nullability);
        let num_runs = ends.len();
        let slots = RunEndBoolSlots {
            ends: ends.into_array(),
            run_validity: validity_to_child(&validity, num_runs),
        }
        .into_slots();
        let data = RunEndBoolData { offset: 0, start };
        Array::try_from_parts(ArrayParts::new(RunEndBool, dtype, len, data, slots)).map(Some)
    }
}

/// `num_runs` booleans that alternate, beginning with `start`.
fn alternating_values(start: bool, num_runs: usize, validity: Validity) -> VortexResult<BoolArray> {
    let bits = BitBuffer::from_iter((0..num_runs).map(|run| start ^ (run % 2 == 1)));
    BoolArray::try_new(bits, validity)
}

const RULES: ParentRuleSet<RunEndBool> =
    ParentRuleSet::new(&[ParentRuleSet::lift(&SliceReduceAdaptor(RunEndBool))]);

impl VTable for RunEndBool {
    type TypedArrayData = RunEndBoolData;

    type OperationsVTable = Self;
    type ValidityVTable = Self;

    fn id(&self) -> ArrayId {
        static ID: CachedId = CachedId::new("vortex.runend.v2");
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
        vortex_ensure!(
            dtype.is_boolean(),
            "RunEndBool requires a boolean dtype, got {dtype}"
        );
        let slots = RunEndBoolSlotsView::from_slots(slots);
        if let Some(validity) = slots.run_validity {
            vortex_ensure!(
                dtype.is_nullable(),
                "Non-nullable RunEndBool must not have a validity child"
            );
            vortex_ensure_eq!(validity.dtype(), &Validity::DTYPE);
            vortex_ensure_eq!(validity.len(), slots.ends.len());
        }
        // TODO(ctx): trait fixes - VTable::validate has a fixed signature.
        let mut ctx = legacy_session().create_execution_ctx();
        RunEndData::validate_ends(slots.ends, data.offset, len, &mut ctx)
    }

    fn nbuffers(_array: ArrayView<'_, Self>) -> usize {
        0
    }

    fn buffer(_array: ArrayView<'_, Self>, idx: usize) -> BufferHandle {
        vortex_panic!("RunEndBoolArray buffer index {idx} out of bounds")
    }

    fn buffer_name(_array: ArrayView<'_, Self>, idx: usize) -> Option<String> {
        vortex_panic!("RunEndBoolArray buffer_name index {idx} out of bounds")
    }

    fn with_buffers(
        &self,
        array: ArrayView<'_, Self>,
        buffers: &[BufferHandle],
    ) -> VortexResult<ArrayParts<Self>> {
        vortex_array::vtable::with_empty_buffers(self, array, buffers)
    }

    fn serialize(
        array: ArrayView<'_, Self>,
        _session: &VortexSession,
    ) -> VortexResult<Option<Vec<u8>>> {
        Ok(Some(
            RunEndBoolMetadata {
                ends_ptype: PType::try_from(array.ends().dtype())? as i32,
                num_runs: array.ends().len() as u64,
                offset: array.offset() as u64,
                start: array.start(),
            }
            .encode_to_vec(),
        ))
    }

    fn deserialize(
        &self,
        dtype: &DType,
        len: usize,
        metadata: &[u8],
        buffers: &[BufferHandle],
        children: &dyn ArrayChildren,
        _session: &VortexSession,
    ) -> VortexResult<ArrayParts<Self>> {
        vortex_ensure!(
            buffers.is_empty(),
            "RunEndBoolArray expects 0 buffers, got {}",
            buffers.len()
        );
        let metadata = RunEndBoolMetadata::decode(metadata)?;
        let ends_dtype = DType::Primitive(metadata.ends_ptype(), Nullability::NonNullable);
        let runs = usize::try_from(metadata.num_runs)?;
        let ends = children.get(0, &ends_dtype, runs)?;
        let run_validity = match children.len() {
            1 => None,
            2 => Some(children.get(1, &Validity::DTYPE, runs)?),
            n => vortex_bail!("Expected 1 or 2 children for RunEndBool, got {n}"),
        };
        let slots = RunEndBoolSlots { ends, run_validity }.into_slots();
        let data = RunEndBoolData {
            offset: usize::try_from(metadata.offset)?,
            start: metadata.start,
        };
        Ok(ArrayParts::new(
            self.clone(),
            dtype.clone(),
            len,
            data,
            slots,
        ))
    }

    fn slot_name(_array: ArrayView<'_, Self>, idx: usize) -> String {
        RunEndBoolSlots::NAMES[idx].to_string()
    }

    fn reduce_parent(
        array: ArrayView<'_, Self>,
        parent: &ArrayRef,
        child_idx: usize,
    ) -> VortexResult<Option<ArrayRef>> {
        RULES.evaluate(array, parent, child_idx)
    }

    /// Expand to a [`RunEnd`] array, so that its kernels and decoding apply.
    fn execute(array: Array<Self>, _ctx: &mut ExecutionCtx) -> VortexResult<ExecutionResult> {
        Ok(ExecutionResult::done(array.to_run_end()?))
    }
}

impl OperationsVTable<RunEndBool> for RunEndBool {
    type ProbeState = ();

    fn scalar_at(
        array: ArrayView<'_, RunEndBool>,
        index: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        let run = find_physical_index(array.ends(), index + array.offset(), ctx)?;
        Ok(Scalar::bool(
            array.run_value(run),
            array.dtype().nullability(),
        ))
    }
}

impl ValidityVTable<RunEndBool> for RunEndBool {
    fn validity(array: ArrayView<'_, RunEndBool>) -> VortexResult<Validity> {
        Ok(match array.per_run_validity() {
            Validity::Array(run_validity) => Validity::Array(
                // SAFETY: the run validity has one entry per run.
                unsafe {
                    RunEnd::new_unchecked(
                        array.ends().clone(),
                        run_validity,
                        array.offset(),
                        array.len(),
                    )
                }
                .into_array(),
            ),
            validity => validity,
        })
    }
}

impl SliceReduce for RunEndBool {
    fn slice(array: ArrayView<'_, Self>, range: Range<usize>) -> VortexResult<Option<ArrayRef>> {
        let slots = RunEndBoolSlots {
            ends: array.ends().clone(),
            run_validity: array.run_validity().cloned(),
        }
        .into_slots();
        let data = RunEndBoolData {
            offset: array.offset() + range.start,
            start: array.start(),
        };
        // SAFETY: a sub-range of a valid array stays covered by its ends.
        Ok(Some(
            unsafe {
                Array::from_parts_unchecked(ArrayParts::new(
                    RunEndBool,
                    array.dtype().clone(),
                    range.len(),
                    data,
                    slots,
                ))
            }
            .into_array(),
        ))
    }
}
