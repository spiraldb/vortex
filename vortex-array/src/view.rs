use std::fmt::{Debug, Formatter};

use enum_iterator::all;
use itertools::Itertools;
use log::info;
use vortex_buffer::Buffer;
use vortex_dtype::flatbuffers::PType;
use vortex_dtype::half::f16;
use vortex_dtype::{DType, Nullability};
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};
use vortex_scalar::Scalar;
use vortex_scalar::Scalar::List;
use vortex_scalar::{flatbuffers as fbs, ListScalar};

use crate::encoding::{EncodingId, EncodingRef};
use crate::flatbuffers as fb;
use crate::stats::{Stat, Statistics, StatsSet};
use crate::Context;
use crate::{Array, IntoArray, ToArray};

#[derive(Clone)]
pub struct ArrayView<'v> {
    encoding: EncodingRef,
    dtype: &'v DType,
    array: fb::Array<'v>,
    buffers: &'v [Buffer],
    ctx: &'v ViewContext,
    // TODO(ngates): a store a Projection. A projected ArrayView contains the full fb::Array
    //  metadata, but only the buffers from the selected columns. Therefore we need to know
    //  which fb:Array children to skip when calculating how to slice into buffers.
}

impl<'a> Debug for ArrayView<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrayView")
            .field("encoding", &self.encoding)
            .field("dtype", self.dtype)
            // .field("array", &self.array)
            .field("buffers", &self.buffers)
            .field("ctx", &self.ctx)
            .finish()
    }
}

impl<'v> ArrayView<'v> {
    pub fn try_new(
        ctx: &'v ViewContext,
        dtype: &'v DType,
        array: fb::Array<'v>,
        buffers: &'v [Buffer],
    ) -> VortexResult<Self> {
        let encoding = ctx
            .find_encoding(array.encoding())
            .ok_or_else(|| vortex_err!(InvalidSerde: "Encoding ID out of bounds"))?;

        if buffers.len() != Self::cumulative_nbuffers(array) {
            vortex_bail!(InvalidSerde:
                "Incorrect number of buffers {}, expected {}",
                buffers.len(),
                Self::cumulative_nbuffers(array)
            )
        }
        let view = Self {
            encoding,
            dtype,
            array,
            buffers,
            ctx,
        };

        // Validate here that the metadata correctly parses, so that an encoding can infallibly
        // implement Encoding::with_view().
        // FIXME(ngates): validate the metadata
        view.to_array().with_dyn(|_| Ok::<(), VortexError>(()))?;

        Ok(view)
    }

    pub fn encoding(&self) -> EncodingRef {
        self.encoding
    }

    pub fn dtype(&self) -> &DType {
        self.dtype
    }

    pub fn metadata(&self) -> Option<&'v [u8]> {
        self.array.metadata().map(|m| m.bytes())
    }

    // TODO(ngates): should we separate self and DType lifetimes? Should DType be cloned?
    pub fn child(&'v self, idx: usize, dtype: &'v DType) -> Option<ArrayView<'v>> {
        let child = self.array_child(idx)?;

        // Figure out how many buffers to skip...
        // We store them depth-first.
        let buffer_offset = self
            .array
            .children()?
            .iter()
            .take(idx)
            .map(|child| Self::cumulative_nbuffers(child))
            .sum();
        let buffer_count = Self::cumulative_nbuffers(child);

        Some(
            Self::try_new(
                self.ctx,
                dtype,
                child,
                &self.buffers[buffer_offset..][0..buffer_count],
            )
            .unwrap(),
        )
    }

    fn array_child(&self, idx: usize) -> Option<fb::Array<'v>> {
        let children = self.array.children()?;
        if idx < children.len() {
            Some(children.get(idx))
        } else {
            None
        }
    }

    /// Whether the current Array makes use of a buffer
    pub fn has_buffer(&self) -> bool {
        self.array.has_buffer()
    }

    /// The number of buffers used by the current Array and all its children.
    fn cumulative_nbuffers(array: fb::Array) -> usize {
        let mut nbuffers = if array.has_buffer() { 1 } else { 0 };
        for child in array.children().unwrap_or_default() {
            nbuffers += Self::cumulative_nbuffers(child)
        }
        nbuffers
    }

    pub fn buffer(&self) -> Option<&'v Buffer> {
        self.has_buffer().then(|| &self.buffers[0])
    }

    pub fn statistics(&self) -> &dyn Statistics {
        self
    }
}

impl Statistics for ArrayView<'_> {
    fn get(&self, stat: Stat) -> Option<Scalar> {
        match stat {
            Stat::Max => {
                let max = self.array.stats()?.max();
                max.and_then(|v| v.type__as_primitive())
                    .and_then(primitive_to_scalar)
            }
            Stat::Min => {
                let min = self.array.stats()?.min();
                min.and_then(|v| v.type__as_primitive())
                    .and_then(primitive_to_scalar)
            }
            Stat::IsConstant => self.array.stats()?.is_constant().map(bool::into),
            Stat::IsSorted => self.array.stats()?.is_sorted().map(bool::into),
            Stat::IsStrictSorted => self.array.stats()?.is_strict_sorted().map(bool::into),
            Stat::RunCount => self.array.stats()?.run_count().map(u64::into),
            Stat::TrueCount => self.array.stats()?.true_count().map(u64::into),
            Stat::NullCount => self.array.stats()?.null_count().map(u64::into),
            Stat::BitWidthFreq => self
                .array
                .stats()?
                .bit_width_freq()
                .map(|v| v.iter().map(u64::into).collect_vec())
                .map(|v| {
                    List(ListScalar::new(
                        DType::Primitive(vortex_dtype::PType::U64, Nullability::NonNullable),
                        Some(v),
                    ))
                }),
            Stat::TrailingZeroFreq => self
                .array
                .stats()?
                .trailing_zero_freq()
                .map(|v| v.iter().map(u64::into).collect_vec())
                .map(|v| {
                    List(ListScalar::new(
                        DType::Primitive(vortex_dtype::PType::U64, Nullability::NonNullable),
                        Some(v),
                    ))
                }),
        }
    }

    /// NB: part of the contract for to_set is that it does not do any expensive computation.
    /// In other implementations, this means returning the underlying stats map, but for the flatbuffer
    /// implemetation, we have 'precalculated' stats in the flatbuffer itself, so we need to
    /// alllocate a stats map and populate it with those fields.
    fn to_set(&self) -> StatsSet {
        let mut result = StatsSet::new();
        for stat in all::<Stat>() {
            if let Some(value) = self.get(stat) {
                result.set(stat, value)
            }
        }
        result
    }

    /// We want to avoid any sort of allocation on instantiation of the ArrayView, so we
    /// do not allocate a stats_set to cache values.
    fn set(&self, _stat: Stat, _value: Scalar) {
        info!("Cannot write stats to a view")
    }

    fn compute(&self, stat: Stat) -> Option<Scalar> {
        if let Some(s) = self.get(stat) {
            return Some(s);
        }

        let calculated = self
            .to_array()
            .with_dyn(|a| a.compute_statistics(stat))
            .ok()?;

        calculated.into_iter().for_each(|(k, v)| self.set(k, v));
        self.get(stat)
    }

    fn with_stat_value<'a>(
        &self,
        stat: Stat,
        f: &'a mut dyn FnMut(&Scalar) -> VortexResult<()>,
    ) -> VortexResult<()> {
        if let Some(existing) = self.get(stat) {
            return f(&existing);
        }
        vortex_bail!(ComputeError: "statistic {} missing", stat);
    }

    fn with_computed_stat_value<'a>(
        &self,
        stat: Stat,
        f: &'a mut dyn FnMut(&Scalar) -> VortexResult<()>,
    ) -> VortexResult<()> {
        self.compute(stat)
            .map(|s| f(&s))
            .unwrap_or_else(|| vortex_bail!(ComputeError: "statistic {} missing", stat))
    }
}

// TODO(@jcasale): move this to serde and make serde crate public?
fn primitive_to_scalar(v: fbs::Primitive) -> Option<Scalar> {
    let err_msg = "failed to deserialize invalid primitive scalar";
    match v.ptype() {
        PType::U8 => v
            .bytes()
            .map(|bytes| u8::from_le_bytes(bytes.bytes().try_into().expect(err_msg)).into()),
        PType::U16 => v
            .bytes()
            .map(|bytes| u16::from_le_bytes(bytes.bytes().try_into().expect(err_msg)).into()),
        PType::U32 => v
            .bytes()
            .map(|bytes| u32::from_le_bytes(bytes.bytes().try_into().expect(err_msg)).into()),
        PType::U64 => v
            .bytes()
            .map(|bytes| u64::from_le_bytes(bytes.bytes().try_into().expect(err_msg)).into()),
        PType::I8 => v
            .bytes()
            .map(|bytes| i8::from_le_bytes(bytes.bytes().try_into().expect(err_msg)).into()),
        PType::I16 => v
            .bytes()
            .map(|bytes| i16::from_le_bytes(bytes.bytes().try_into().expect(err_msg)).into()),
        PType::I32 => v
            .bytes()
            .map(|bytes| i32::from_le_bytes(bytes.bytes().try_into().expect(err_msg)).into()),
        PType::I64 => v
            .bytes()
            .map(|bytes| i64::from_le_bytes(bytes.bytes().try_into().expect(err_msg)).into()),
        PType::F16 => v
            .bytes()
            .map(|bytes| f16::from_le_bytes(bytes.bytes().try_into().expect(err_msg)).into()),
        PType::F32 => v
            .bytes()
            .map(|bytes| f32::from_le_bytes(bytes.bytes().try_into().expect(err_msg)).into()),
        PType::F64 => v
            .bytes()
            .map(|bytes| f64::from_le_bytes(bytes.bytes().try_into().expect(err_msg)).into()),
        _ => unreachable!(),
    }
}

impl ToArray for ArrayView<'_> {
    fn to_array(&self) -> Array {
        Array::View(self.clone())
    }
}

impl<'v> IntoArray<'v> for ArrayView<'v> {
    fn into_array(self) -> Array<'v> {
        Array::View(self)
    }
}

#[derive(Debug)]
pub struct ViewContext {
    encodings: Vec<EncodingRef>,
    stats: Vec<Stat>,
}

impl ViewContext {
    pub fn new(encodings: Vec<EncodingRef>, stats: Vec<Stat>) -> Self {
        Self { encodings, stats }
    }

    pub fn set_stats(&mut self, to_enable: &[Stat]) {
        self.stats.clear();
        self.stats.extend(to_enable)
    }

    pub fn stats(&self) -> &[Stat] {
        self.stats.as_ref()
    }

    pub fn default_stats() -> Vec<Stat> {
        vec![
            Stat::Max,
            Stat::Min,
            Stat::IsSorted,
            Stat::IsStrictSorted,
            Stat::IsConstant,
            Stat::BitWidthFreq,
            Stat::TrailingZeroFreq,
            Stat::NullCount,
            Stat::RunCount,
            Stat::TrueCount,
        ]
    }

    pub fn encodings(&self) -> &[EncodingRef] {
        self.encodings.as_ref()
    }

    pub fn find_encoding(&self, encoding_id: u16) -> Option<EncodingRef> {
        self.encodings.get(encoding_id as usize).cloned()
    }

    pub fn encoding_idx(&self, encoding_id: EncodingId) -> Option<u16> {
        self.encodings
            .iter()
            .position(|e| e.id() == encoding_id)
            .map(|i| i as u16)
    }
}

impl Default for ViewContext {
    fn default() -> Self {
        todo!("FIXME(ngates): which encodings to enable?")
    }
}

impl From<&Context> for ViewContext {
    fn from(value: &Context) -> Self {
        ViewContext::new(value.encodings().collect_vec(), Self::default_stats())
    }
}
