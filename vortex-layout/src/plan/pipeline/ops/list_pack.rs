// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Assembling lists from offsets and elements.

use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::arrays::ConstantArray;
use vortex_array::arrays::ListArray;
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::scalar_fn::fns::operators::Operator as BinaryOperator;
use vortex_array::validity::Validity;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;
use vortex_mask::Mask;

use super::join_inlet;
use crate::plan::ListPackPlan;
use crate::plan::pipeline::Blocked;
use crate::plan::pipeline::Cx;
use crate::plan::pipeline::Source;
use crate::plan::pipeline::Step;

/// Assembles lists from their offsets, their elements, and their validity, then keeps the
/// selected ones.
///
/// Every inlet is compiled up front: the offsets and validity of the lists read, and the list's
/// elements whole. Once all three have ended, the elements are sliced to the range the offsets
/// reference, so the source needs no plan it did not start with.
pub(crate) struct ListPackSource {
    plan: ListPackPlan,
    /// The selection over the lists read.
    mask: Mask,
    has_validity: bool,
    done: bool,
}

const OFFSETS: usize = 0;
const ELEMENTS: usize = 1;
const VALIDITY: usize = 2;

impl ListPackSource {
    pub(crate) fn new(plan: ListPackPlan, mask: Mask, has_validity: bool) -> Self {
        Self {
            plan,
            mask,
            has_validity,
            done: false,
        }
    }

    /// The lists, from the whole of every inlet.
    fn assemble(&self, cx: &mut Cx<'_>) -> VortexResult<ArrayRef> {
        let offsets = join_inlet(cx, OFFSETS, self.plan.offsets()?.dtype())?;
        vortex_ensure!(
            offsets.len() == self.mask.len() + 1,
            "Incomplete list offsets"
        );
        let first = offset(&offsets, 0, cx)?;
        let last = offset(&offsets, offsets.len() - 1, cx)?;
        let elements = join_inlet(cx, ELEMENTS, self.plan.elements()?.dtype())?;
        vortex_ensure!(
            first <= last && last <= elements.len() as u64,
            "List element range {first}..{last} is out of bounds"
        );
        let elements = elements.slice(usize::try_from(first)?..usize::try_from(last)?)?;
        let offsets = if first == 0 {
            offsets
        } else {
            let base = ConstantArray::new(first, offsets.len())
                .into_array()
                .cast(offsets.dtype().clone())?;
            offsets.binary(base, BinaryOperator::Sub)?
        };
        let validity = match self.plan.validity()? {
            Some(plan) => Validity::Array(join_inlet(cx, VALIDITY, plan.dtype())?),
            None => Validity::NonNullable,
        };
        ListArray::try_new(elements, offsets, validity)?
            .into_array()
            .filter(self.mask.clone())
    }
}

/// Offset `index` of `offsets`, as a row of the elements.
fn offset(offsets: &ArrayRef, index: usize, cx: &mut Cx<'_>) -> VortexResult<u64> {
    offsets
        .execute_scalar(index, cx.exec())?
        .as_primitive()
        .as_::<u64>()
        .ok_or_else(|| vortex_err!("Invalid list offset at {index}"))
}

impl ListPackSource {
    fn inlet_count(&self) -> usize {
        if self.has_validity { 3 } else { 2 }
    }
}

impl Source for ListPackSource {
    fn ready(&mut self, cx: &mut Cx<'_>) -> VortexResult<Step> {
        if self.done {
            return Ok(Step::Finished);
        }
        for index in 0..self.inlet_count() {
            if !cx.inlet(index).closed() {
                return Ok(Step::Blocked(Blocked::Inlet(index)));
            }
        }
        self.done = true;
        Ok(Step::Batch(self.assemble(cx)?))
    }
}
