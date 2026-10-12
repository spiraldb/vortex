// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Looking codes up in dictionary values.

use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::arrays::DictArray;
use vortex_array::arrays::Shared;
use vortex_array::arrays::SharedArray;
use vortex_error::VortexResult;

use super::UNBOUNDED;
use super::join_inlet;
use super::next_batch;
use crate::plan::TakePlan;
use crate::plan::pipeline::Blocked;
use crate::plan::pipeline::Cx;
use crate::plan::pipeline::DEFAULT_CAPACITY;
use crate::plan::pipeline::Source;
use crate::plan::pipeline::Step;

/// Wraps each batch of codes as a dictionary over the values, once the values inlet has ended.
pub(crate) struct TakeSource {
    plan: TakePlan,
    /// The values, once their inlet has ended.
    values: Option<ArrayRef>,
}

const CODES: usize = 0;
const VALUES: usize = 1;

impl TakeSource {
    /// A take over `values`, or over what its values inlet produces when `None`.
    pub(crate) fn new(plan: TakePlan, values: Option<ArrayRef>) -> Self {
        Self { plan, values }
    }
}

impl Source for TakeSource {
    fn inlet_count(&self) -> usize {
        if self.values.is_some() { 1 } else { 2 }
    }

    fn capacity(&self, inlet: usize) -> usize {
        if inlet == VALUES {
            UNBOUNDED
        } else {
            DEFAULT_CAPACITY
        }
    }

    fn next(&mut self, cx: &mut Cx<'_>) -> VortexResult<Step> {
        let values = match &self.values {
            Some(values) => values.clone(),
            None => {
                if !cx.inlet(VALUES).closed() {
                    return Ok(Step::Blocked(Blocked::Inlet(VALUES)));
                }
                let values = shared(join_inlet(cx, VALUES, self.plan.values()?.dtype())?);
                self.values = Some(values.clone());
                values
            }
        };
        Ok(match next_batch(cx, CODES) {
            Step::Batch(codes) => Step::Batch(DictArray::try_new(codes, values)?.into_array()),
            step => step,
        })
    }
}

/// `array` as a [`SharedArray`], so however many dictionaries use it, it is canonicalized once.
fn shared(array: ArrayRef) -> ArrayRef {
    if array.is::<Shared>() {
        array
    } else {
        SharedArray::new(array).into_array()
    }
}
