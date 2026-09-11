// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! RLE probes route through the indices, offsets, and values slots.
//!
//! A slot that already holds a materialized primitive array is read directly from its typed
//! buffer. Any other child, including one with a lazy validity expression, is read through a
//! child probe that keeps its own state, so nesting works to any depth.

use num_traits::ToPrimitive;
use vortex_array::ArrayRef;
use vortex_array::ArrayView;
use vortex_array::ExecutionCtx;
use vortex_array::ProbeChildren;
use vortex_array::ProbeCtx;
use vortex_array::arrays::Bool;
use vortex_array::arrays::Primitive;
use vortex_array::arrays::primitive::PrimitiveArrayExt;
use vortex_array::match_each_native_ptype;
use vortex_array::match_each_unsigned_integer_ptype;
use vortex_array::scalar::Scalar;
use vortex_array::validity::Validity;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;
use vortex_mask::Mask;

use crate::FL_CHUNK_SIZE;
use crate::RLE;
use crate::rle::RLEArrayExt;
use crate::rle::RLEArraySlotsExt;
use crate::rle::RLESlots;

/// State for repeated RLE probes: one reader per child slot plus the slice's base value offset.
#[derive(Default)]
pub struct RleProbeState<'a> {
    children: Option<Children<'a>>,
}

struct Children<'a> {
    indices: Child<'a>,
    offsets: Child<'a>,
    values: Child<'a>,
    base: usize,
}

enum Child<'a> {
    /// A materialized primitive array with its validity resolved once.
    Primitive {
        view: ArrayView<'a, Primitive>,
        validity: Option<Mask>,
    },
    /// A slot read through the context's retained child probe.
    Probe(usize),
}

impl<'a> Child<'a> {
    fn new(
        slots: &'a [Option<ArrayRef>],
        slot: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Self> {
        let child = slots[slot]
            .as_ref()
            .ok_or_else(|| vortex_err!("RLE slot {slot} is missing"))?;
        let Some(view) = child.as_opt::<Primitive>() else {
            return Ok(Self::Probe(slot));
        };
        let validity = match PrimitiveArrayExt::validity(&view) {
            Validity::NonNullable | Validity::AllValid => None,
            // A lazy validity expression may fail on rows this probe never requests, so the
            // primitive probe evaluates it per row instead.
            Validity::Array(array) if !array.is::<Bool>() => {
                return Ok(Self::Probe(slot));
            }
            validity => Some(validity.execute_mask(view.len(), ctx)?),
        };
        Ok(Self::Primitive { view, validity })
    }

    /// Read an unsigned routing value, or `None` when the row is null.
    fn index_at(
        &mut self,
        index: usize,
        children: &mut ProbeChildren<'a>,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<usize>> {
        match self {
            Self::Primitive { view, validity } => {
                vortex_ensure!(index < view.len(), OutOfBounds: index, 0, view.len());
                if validity.as_ref().is_some_and(|mask| !mask.value(index)) {
                    return Ok(None);
                }
                match_each_unsigned_integer_ptype!(view.ptype(), |T| {
                    view.as_slice::<T>()[index]
                        .to_usize()
                        .map(Some)
                        .ok_or_else(|| vortex_err!("RLE index does not fit usize"))
                })
            }
            Self::Probe(slot) => Ok(children
                .child(*slot)?
                .scalar_at(index, ctx)?
                .as_primitive()
                .as_::<usize>()),
        }
    }

    /// Read a value, tagged with the RLE array's dtype.
    fn value_at(
        &mut self,
        index: usize,
        array: ArrayView<'_, RLE>,
        children: &mut ProbeChildren<'a>,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        match self {
            Self::Primitive { view, .. } => {
                vortex_ensure!(index < view.len(), OutOfBounds: index, 0, view.len());
                Ok(match_each_native_ptype!(view.ptype(), |T| {
                    Scalar::primitive(view.as_slice::<T>()[index], array.dtype().nullability())
                }))
            }
            Self::Probe(slot) => Scalar::try_new(
                array.dtype().clone(),
                children.child(*slot)?.scalar_at(index, ctx)?.into_value(),
            ),
        }
    }
}

pub(crate) fn scalar_at<'a>(
    array: ArrayView<'a, RLE>,
    index: usize,
    probe: Option<&mut ProbeCtx<'a, RleProbeState<'a>>>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Scalar> {
    let logical_index = array.offset() + index;
    let chunk = logical_index / FL_CHUNK_SIZE;
    let Some(probe) = probe else {
        // The index scalar supplies RLE's nullness and routing value in one lookup.
        let code = array.indices().execute_scalar(logical_index, ctx)?;
        let Some(code) = code.as_primitive().as_::<usize>() else {
            return Ok(Scalar::null(array.dtype().clone()));
        };
        let offset = if chunk == 0 {
            0
        } else {
            let offsets = array.values_idx_offsets();
            read_offset(offsets.execute_scalar(chunk, ctx)?)?
                .checked_sub(read_offset(offsets.execute_scalar(0, ctx)?)?)
                .ok_or_else(|| vortex_err!("RLE offsets precede the slice base"))?
        };
        let value_index = value_index(offset, code)?;
        return Scalar::try_new(
            array.dtype().clone(),
            array
                .values()
                .execute_scalar(value_index, ctx)?
                .into_value(),
        );
    };

    let (state, probes) = probe.parts();
    let children = match &mut state.children {
        Some(children) => children,
        slot @ None => {
            // Borrow from the source slots, whose lifetime is independent of the temporary view.
            let slots = array.slots();
            let mut offsets = Child::new(slots, RLESlots::VALUES_IDX_OFFSETS, ctx)?;
            let base = offsets
                .index_at(0, probes, ctx)?
                .ok_or_else(|| vortex_err!("RLE offset must be a non-null usize"))?;
            slot.insert(Children {
                indices: Child::new(slots, RLESlots::INDICES, ctx)?,
                offsets,
                values: Child::new(slots, RLESlots::VALUES, ctx)?,
                base,
            })
        }
    };
    let Some(code) = children.indices.index_at(logical_index, probes, ctx)? else {
        return Ok(Scalar::null(array.dtype().clone()));
    };
    let offset = children
        .offsets
        .index_at(chunk, probes, ctx)?
        .ok_or_else(|| vortex_err!("RLE offset must be a non-null usize"))?
        .checked_sub(children.base)
        .ok_or_else(|| vortex_err!("RLE offsets precede the slice base"))?;
    children
        .values
        .value_at(value_index(offset, code)?, array, probes, ctx)
}

fn read_offset(scalar: Scalar) -> VortexResult<usize> {
    scalar
        .as_primitive()
        .as_::<usize>()
        .ok_or_else(|| vortex_err!("RLE offset must be a non-null usize"))
}

fn value_index(offset: usize, code: usize) -> VortexResult<usize> {
    offset
        .checked_add(code)
        .ok_or_else(|| vortex_err!("RLE value index overflow"))
}

#[cfg(test)]
mod tests;
