// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Applying an expression to each batch.

use vortex_error::VortexResult;

use crate::plan::physical::EvalPlan;
use crate::plan::physical::operator::Cx;
use crate::plan::physical::operator::Input;
use crate::plan::physical::operator::Operator;
use crate::plan::physical::operator::Step;

/// Applies an expression to each batch.
pub(crate) struct EvalStage {
    plan: EvalPlan,
}

impl EvalStage {
    pub(crate) fn new(plan: EvalPlan) -> Self {
        Self { plan }
    }
}

impl Operator for EvalStage {
    fn compute(&mut self, input: Input, cx: &mut Cx<'_>) -> VortexResult<Step> {
        let Input::Batch(batch) = input else {
            return Ok(Step::End);
        };
        Ok(Step::Final(self.plan.apply(batch, cx.session())?))
    }
}
