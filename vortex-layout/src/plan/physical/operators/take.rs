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
use crate::plan::physical::TakePlan;
use crate::plan::physical::operator::Cx;
use crate::plan::physical::operator::Input;
use crate::plan::physical::operator::Operator;
use crate::plan::physical::operator::Source;
use crate::plan::physical::operator::SourceBlocked;
use crate::plan::physical::operator::SourceReady;
use crate::plan::physical::operator::Step;
use crate::plan::physical::pipeline::DEFAULT_CAPACITY;

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

    /// The batches inlet `inlet` holds: the values are taken whole, so they never block.
    pub(crate) fn capacity(inlet: usize) -> usize {
        if inlet == VALUES {
            UNBOUNDED
        } else {
            DEFAULT_CAPACITY
        }
    }
}

impl Source for TakeSource {
    fn ready(&mut self, cx: &mut Cx<'_>) -> VortexResult<SourceReady> {
        if self.values.is_none() && !cx.inlet(VALUES).closed() {
            return Ok(SourceReady::Blocked(SourceBlocked::Inlet(VALUES)));
        }
        let codes = cx.inlet(CODES);
        Ok(if codes.is_empty() && !codes.closed() {
            SourceReady::Blocked(SourceBlocked::Inlet(CODES))
        } else {
            SourceReady::Ready
        })
    }
}

impl Operator for TakeSource {
    fn compute(&mut self, _input: Input, cx: &mut Cx<'_>) -> VortexResult<Step> {
        let values = match &self.values {
            Some(values) => values.clone(),
            None => {
                let values = shared(join_inlet(cx, VALUES, self.plan.values()?.dtype())?);
                self.values = Some(values.clone());
                values
            }
        };
        Ok(match cx.inlet(CODES).take() {
            Some(codes) => Step::Final(DictArray::try_new(codes, values)?.into_array()),
            None => Step::End,
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
