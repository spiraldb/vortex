// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_mask::Mask;

use crate::ExecutionCtx;
use crate::ProbeCtx;
use crate::array::ArrayView;
use crate::array::OperationsVTable;
use crate::arrays::Bool;
use crate::arrays::Primitive;
use crate::arrays::primitive::PrimitiveArrayExt;
use crate::arrays::primitive::array::PrimitiveSlots;
use crate::match_each_native_ptype;
use crate::scalar::Scalar;

/// State for repeated primitive probes: the validity, resolved once.
///
/// Encodings that route through primitive children reach this state via their child probes,
/// so a nullable child costs one bit read per lookup rather than a validity execution.
#[derive(Default)]
pub struct PrimitiveProbeState {
    validity: Option<PreparedValidity>,
}

enum PreparedValidity {
    Mask(Mask),
    Lazy,
}

impl OperationsVTable<Primitive> for Primitive {
    type ProbeState<'a> = PrimitiveProbeState;

    fn probe_scalar<'a>(
        array: ArrayView<'a, Primitive>,
        index: usize,
        probe: Option<&mut ProbeCtx<'a, Self::ProbeState<'a>>>,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        let Some(probe) = probe else {
            return array.array().execute_scalar(index, ctx);
        };
        let (state, children) = probe.parts();
        let validity = match &mut state.validity {
            Some(validity) => validity,
            slot @ None => slot.insert(match array.slots()[PrimitiveSlots::VALIDITY].as_ref() {
                // A lazy validity expression may fail on rows this probe never requests.
                Some(child) if !child.is::<Bool>() => PreparedValidity::Lazy,
                _ => PreparedValidity::Mask(
                    PrimitiveArrayExt::validity(&array).execute_mask(array.len(), ctx)?,
                ),
            }),
        };
        let valid = match validity {
            PreparedValidity::Mask(mask) => mask.value(index),
            PreparedValidity::Lazy => children
                .child(PrimitiveSlots::VALIDITY)?
                .scalar_at(index, ctx)?
                .as_bool()
                .value()
                .ok_or_else(|| vortex_err!("validity value at index {index} is null"))?,
        };
        if !valid {
            return Ok(Scalar::null(array.dtype().clone()));
        }
        Self::scalar_at(array, index, ctx)
    }

    fn scalar_at(
        array: ArrayView<'_, Primitive>,
        index: usize,
        _ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        Ok(match_each_native_ptype!(array.ptype(), |T| {
            Scalar::primitive(array.as_slice::<T>()[index], array.dtype().nullability())
        }))
    }
}
