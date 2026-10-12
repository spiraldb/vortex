// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Applying an expression to each batch.

use vortex_array::ArrayRef;
use vortex_array::expr::BoundExpression;
use vortex_error::VortexResult;

use crate::plan::pipeline::Cx;
use crate::plan::pipeline::Stage;

/// Applies an expression to each batch.
pub(crate) struct EvalStage {
    expression: BoundExpression,
}

impl EvalStage {
    pub(crate) fn new(expression: BoundExpression) -> Self {
        Self { expression }
    }
}

impl Stage for EvalStage {
    fn push(&mut self, batch: ArrayRef, _cx: &mut Cx<'_>) -> VortexResult<Option<ArrayRef>> {
        Ok(Some(batch.apply_bound(&self.expression)?))
    }
}
