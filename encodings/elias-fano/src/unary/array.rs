// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;

use num_traits::AsPrimitive;
use prost::Message;
use vortex_array::Array;
use vortex_array::ArrayEq;
use vortex_array::ArrayHash;
use vortex_array::ArrayId;
use vortex_array::ArrayParts;
use vortex_array::ArrayRef;
use vortex_array::ArraySlots;
use vortex_array::ArrayView;
use vortex_array::EqMode;
use vortex_array::ExecutionCtx;
use vortex_array::ExecutionResult;
use vortex_array::IntoArray;
use vortex_array::buffer::BufferHandle;
use vortex_array::dtype::DType;
use vortex_array::dtype::Nullability::NonNullable;
use vortex_array::dtype::PType;
use vortex_array::expr::stats::Precision;
use vortex_array::expr::stats::Stat;
use vortex_array::match_each_unsigned_integer_ptype;
use vortex_array::scalar::Scalar;
use vortex_array::serde::ArrayChildren;
use vortex_array::stats::StatsSet;
use vortex_array::validity::Validity;
use vortex_array::vtable::OperationsVTable;
use vortex_array::vtable::VTable;
use vortex_array::vtable::ValidityVTable;
use vortex_buffer::BitBuffer;
use vortex_buffer::ByteBuffer;
use vortex_error::VortexError;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_err;
use vortex_error::vortex_panic;
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

use super::compress::unary_decode;

/// A [`Unary`]-encoded Vortex array.
pub type UnaryArray = Array<Unary>;

/// Metadata stored alongside the `bits` buffer.
#[derive(Clone, prost::Message)]
pub(super) struct UnaryMetadata {
    /// The first value.
    #[prost(uint64, tag = "1")]
    pub(super) base: u64,
    /// The last value.
    #[prost(uint64, tag = "2")]
    pub(super) max: u64,
}

/// The parts of a [`Unary`] array: its bits, and its first and last values.
#[derive(Clone, Debug)]
pub struct UnaryData {
    bits: BitBuffer,
    base: u64,
    max: u64,
}

impl Display for UnaryData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "base: {}, max: {}", self.base, self.max)
    }
}

impl ArrayHash for UnaryData {
    fn array_hash<H: Hasher>(&self, state: &mut H, accuracy: EqMode) {
        self.base.hash(state);
        self.max.hash(state);
        self.bits.array_hash(state, accuracy);
    }
}

impl ArrayEq for UnaryData {
    fn array_eq(&self, other: &Self, accuracy: EqMode) -> bool {
        self.base == other.base
            && self.max == other.max
            && self.bits.array_eq(&other.bits, accuracy)
    }
}

/// The number of bits `len` values spanning `span` take, if a `usize` can count them.
pub(super) fn bit_len(len: usize, span: u64) -> Option<usize> {
    u64::try_from(len)
        .ok()?
        .checked_add(span)
        .and_then(|bits| usize::try_from(bits).ok())
}

fn too_many_bits(len: usize, span: u64) -> VortexError {
    vortex_err!("UnaryArray of {len} values spanning {span} needs more bits than a usize counts")
}

impl UnaryData {
    /// Rebuild the parts from a stored buffer. The buffer's size is checked first, because
    /// [`BitBuffer::new`] panics on one too short for its bits.
    fn from_buffer(
        buffer: ByteBuffer,
        base: u64,
        max: u64,
        dtype: &DType,
        len: usize,
    ) -> VortexResult<Self> {
        let span = max
            .checked_sub(base)
            .ok_or_else(|| vortex_err!("UnaryArray base {base} exceeds max {max}"))?;
        let bits = bit_len(len, span).ok_or_else(|| too_many_bits(len, span))?;
        vortex_ensure!(
            buffer.len().checked_mul(8).is_some_and(|held| held >= bits),
            "UnaryArray needs {bits} bits, but its buffer holds {} bytes",
            buffer.len()
        );
        let data = Self {
            bits: BitBuffer::new(buffer, bits),
            base,
            max,
        };
        data.validate(dtype, len)?;
        Ok(data)
    }

    /// Check the parts against an array of `len` values of `dtype`.
    ///
    /// Besides the metadata and the first and last bits, this counts the set bits, one pass over
    /// the buffer. Any bits that pass then describe a valid array: with exactly `len` set bits
    /// among `len + span`, the `k`-th sits in `[k, k + span]`, so every value lies within
    /// `[base, max]`. The readers rely on this and check no value themselves.
    fn validate(&self, dtype: &DType, len: usize) -> VortexResult<()> {
        let DType::Primitive(ptype, NonNullable) = dtype else {
            vortex_bail!("UnaryArray requires a non-nullable unsigned integer dtype, got {dtype}");
        };
        vortex_ensure!(
            ptype.is_unsigned_int(),
            "UnaryArray requires a non-nullable unsigned integer dtype, got {dtype}"
        );
        vortex_ensure!(
            self.base <= self.max,
            "UnaryArray base {} exceeds max {}",
            self.base,
            self.max
        );
        vortex_ensure!(
            self.max <= ptype.max_value_as_u64(),
            "UnaryArray max {} does not fit {ptype}",
            self.max
        );
        let span = self.span();
        let bits = bit_len(len, span).ok_or_else(|| too_many_bits(len, span))?;
        vortex_ensure!(
            self.bits.offset() == 0,
            "UnaryArray bits must start at offset 0, got {}",
            self.bits.offset()
        );
        vortex_ensure_eq!(self.bits.len(), bits);
        if len == 0 {
            vortex_ensure!(
                span == 0,
                "An empty UnaryArray must have its base equal to its max, got a span of {span}"
            );
        } else {
            // Each value ends in a set bit, so these make `base` and `max` the real first and last
            // values.
            vortex_ensure!(
                self.bits.value(0) && self.bits.value(bits - 1),
                "UnaryArray bits must start and end with a set bit"
            );
        }
        // The only linear check, so it runs last.
        vortex_ensure_eq!(self.bits.true_count(), len);
        Ok(())
    }

    pub(super) fn bits(&self) -> &BitBuffer {
        &self.bits
    }

    /// The first value.
    pub(super) fn base(&self) -> u64 {
        self.base
    }

    /// The last value.
    pub(super) fn max(&self) -> u64 {
        self.max
    }

    /// `max - base`, which is also the number of unset bits.
    pub(super) fn span(&self) -> u64 {
        self.max - self.base
    }

    /// The value at `index`, found by scanning for its set bit from the start.
    pub(super) fn value_at(&self, index: usize) -> VortexResult<u64> {
        let position = self
            .bits
            .select(index)
            .ok_or_else(|| vortex_err!("UnaryArray has no set bit for index {index}"))?;
        // The `index`-th set bit has `index` set bits before it, so this cannot underflow, and
        // `validate` keeps it within `span`, so the sum cannot pass `max`.
        let delta = u64::try_from(position - index)?;
        Ok(self.base + delta)
    }
}

impl VTable for Unary {
    type TypedArrayData = UnaryData;

    type OperationsVTable = Self;
    type ValidityVTable = Self;

    fn id(&self) -> ArrayId {
        static ID: CachedId = CachedId::new("vortex.unary");
        *ID
    }

    fn validate(
        &self,
        data: &Self::TypedArrayData,
        dtype: &DType,
        len: usize,
        slots: &[Option<ArrayRef>],
    ) -> VortexResult<()> {
        vortex_ensure!(
            slots.is_empty(),
            "UnaryArray has no children, got {}",
            slots.len()
        );
        data.validate(dtype, len)
    }

    fn nbuffers(_array: ArrayView<'_, Self>) -> usize {
        1
    }

    fn buffer(array: ArrayView<'_, Self>, idx: usize) -> BufferHandle {
        match idx {
            0 => BufferHandle::new_host(array.bits().inner().clone()),
            _ => vortex_panic!("UnaryArray buffer index {idx} out of bounds"),
        }
    }

    fn buffer_name(_array: ArrayView<'_, Self>, idx: usize) -> Option<String> {
        match idx {
            0 => Some("bits".to_string()),
            _ => None,
        }
    }

    fn with_buffers(
        &self,
        array: ArrayView<'_, Self>,
        buffers: &[BufferHandle],
    ) -> VortexResult<ArrayParts<Self>> {
        vortex_ensure_eq!(buffers.len(), 1);
        let data = UnaryData::from_buffer(
            buffers[0].try_to_host_sync()?,
            array.base(),
            array.max(),
            array.dtype(),
            array.len(),
        )?;
        Ok(ArrayParts::new(
            self.clone(),
            array.dtype().clone(),
            array.len(),
            data,
            ArraySlots::new(),
        ))
    }

    fn serialize(
        array: ArrayView<'_, Self>,
        _session: &VortexSession,
    ) -> VortexResult<Option<Vec<u8>>> {
        Ok(Some(
            UnaryMetadata {
                base: array.base(),
                max: array.max(),
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
            buffers.len() == 1,
            "UnaryArray expects 1 buffer, got {}",
            buffers.len()
        );
        vortex_ensure!(
            children.is_empty(),
            "UnaryArray expects 0 children, got {}",
            children.len()
        );
        let metadata = UnaryMetadata::decode(metadata)?;
        let data = UnaryData::from_buffer(
            buffers[0].try_to_host_sync()?,
            metadata.base,
            metadata.max,
            dtype,
            len,
        )?;
        Ok(ArrayParts::new(
            self.clone(),
            dtype.clone(),
            len,
            data,
            ArraySlots::new(),
        ))
    }

    fn slot_name(_array: ArrayView<'_, Self>, idx: usize) -> String {
        vortex_panic!("UnaryArray has no slots, got index {idx}")
    }

    fn execute(array: Array<Self>, _ctx: &mut ExecutionCtx) -> VortexResult<ExecutionResult> {
        Ok(ExecutionResult::done(unary_decode(&array).into_array()))
    }
}

impl OperationsVTable<Unary> for Unary {
    type ProbeState = ();

    fn scalar_at(
        array: ArrayView<'_, Unary>,
        index: usize,
        _ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        let value = array.value_at(index)?;
        // `validate` keeps `max` within the ptype and every value within `max`.
        Ok(match_each_unsigned_integer_ptype!(
            array.dtype().as_ptype(),
            |T| { Scalar::primitive::<T>(value.as_(), NonNullable) }
        ))
    }
}

impl ValidityVTable<Unary> for Unary {
    fn validity(_array: ArrayView<'_, Unary>) -> VortexResult<Validity> {
        Ok(Validity::NonNullable)
    }
}

/// Unary encoding for non-decreasing, non-nullable unsigned integers.
///
/// The values are stored as one bit array. Each value's gap from the one before it is written in
/// unary, as that many unset bits followed by a set bit. The first value is the array's `base`, so
/// its gap is zero. The set bit of the value at index `i` therefore sits at `(value - base) + i`,
/// and the array takes `len + (max - base)` bits.
///
/// For `[3, 5, 5, 9]` the base is 3 and the bits are `1 001 1 00001`.
///
/// Reads are deliberately simple for now: `scalar_at` scans from the first bit, and a slice
/// decodes the whole array.
#[derive(Clone, Debug)]
pub struct Unary;

impl Unary {
    /// Assemble an array of `len` values from its bits and its first and last values.
    ///
    /// Prefer [`unary_encode`](crate::unary_encode) unless you already hold the bits.
    pub fn try_new(
        bits: BitBuffer,
        base: u64,
        max: u64,
        ptype: PType,
        len: usize,
    ) -> VortexResult<UnaryArray> {
        let dtype = DType::Primitive(ptype, NonNullable);
        let data = UnaryData { bits, base, max };
        Array::try_from_parts(ArrayParts::new(Unary, dtype, len, data, ArraySlots::new()))
            .map(|array| array.with_stats_set(Self::stats()))
    }

    /// Statistics that hold for every Unary array by construction.
    fn stats() -> StatsSet {
        StatsSet::of(Stat::IsSorted, Precision::Exact(true.into()))
    }
}
