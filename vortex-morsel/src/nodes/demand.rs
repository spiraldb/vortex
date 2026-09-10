// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_error::VortexResult;
use vortex_mask::Mask;

use crate::demand::RowDomain;
use crate::node::Batch;
use crate::node::Cx;
use crate::node::LookAhead;
use crate::node::Operator;
use crate::node::Step;

/// The mask producer of an unfiltered scan: the morsel's own demand, as a selection.
///
/// A scan without a predicate still has a selection, the rows the caller asked for, so its
/// filters read it from the mask buffer the root fills from here. Answers once per morsel.
pub struct DemandExec {
    domain: RowDomain,
    emitted: bool,
}

impl DemandExec {
    /// Build the demand operator.
    pub fn new(domain: RowDomain) -> Self {
        Self {
            domain,
            emitted: false,
        }
    }
}

impl Operator for DemandExec {
    fn row_domain(&self) -> &RowDomain {
        &self.domain
    }

    fn look_ahead(&mut self, _cx: &mut Cx<'_>) -> VortexResult<LookAhead> {
        Ok(LookAhead::Complete)
    }

    fn next(&mut self, _hint: &Mask, cx: &mut Cx<'_>) -> VortexResult<Step> {
        if self.emitted {
            return Ok(Step::Finished);
        }
        self.emitted = true;
        Ok(Step::Batch(Batch::mask(
            self.domain.range().clone(),
            cx.morsel.demand.clone(),
        )))
    }

    fn close(&mut self, _cx: &mut Cx<'_>) {
        self.emitted = true;
    }

    fn describe(&self) -> String {
        "Demand(the morsel's own selection)".to_string()
    }
}
