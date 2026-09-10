// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_array::Canonical;
use vortex_array::IntoArray;
use vortex_array::dtype::DType;
use vortex_array::expr::BoundExpression;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_mask::Mask;

use crate::demand::RowDomain;
use crate::io::IoPriority;
use crate::node::Batch;
use crate::node::Child;
use crate::node::Cx;
use crate::node::LookAhead;
use crate::node::Operator;
use crate::node::Step;

/// The root of a scan: the projection's `Eval` over a body whose leaves are filtered.
///
/// The root owns the morsel's mask producer and its body. It plans the mask under `Required`
/// with the morsel range and demand, then the body under `Speculative` when there is a real
/// predicate: the mask decides which of the body's reads execution ever needs. When pulled it
/// drains the mask producer into the mask buffer every filter in the body reads, then pulls the
/// body and applies the projection expression to the selected rows that come back.
pub struct ProjectExec {
    mask: Child,
    body: Child,
    /// The mask buffer the mask producer fills and the body's filters read.
    tee: usize,
    /// Whether the mask is a predicate rather than the morsel's own demand.
    filtered: bool,
    expr: BoundExpression,
    dtype: DType,

    domain: RowDomain,
    state: State,
}

enum State {
    FillMask,
    PullBody,
    Emitted,
}

impl ProjectExec {
    /// Build the root over `mask` and `body`, whose filters read mask buffer `tee`, applying
    /// `expr` to the result.
    pub fn new(
        mask: Child,
        body: Child,
        tee: usize,
        filtered: bool,
        expr: BoundExpression,
        dtype: DType,
        domain: RowDomain,
    ) -> Self {
        Self {
            mask,
            body,
            tee,
            filtered,
            expr,
            dtype,
            domain,
            state: State::FillMask,
        }
    }
}

impl Operator for ProjectExec {
    fn row_domain(&self) -> &RowDomain {
        &self.domain
    }

    fn look_ahead(&mut self, cx: &mut Cx<'_>) -> VortexResult<LookAhead> {
        let mask = cx.with_priority(IoPriority::Required, |cx| self.mask.look_ahead(cx))?;
        let priority = if self.filtered {
            IoPriority::Speculative
        } else {
            IoPriority::Required
        };
        let body = cx.with_priority(priority, |cx| self.body.look_ahead(cx))?;
        Ok(mask.merge(body))
    }

    fn next(&mut self, hint: &Mask, cx: &mut Cx<'_>) -> VortexResult<Step> {
        if matches!(self.state, State::FillMask) {
            let demand = cx.morsel.demand;
            loop {
                match self.mask.next(demand, cx)? {
                    Step::Batch(piece) => {
                        let mask = piece.value.into_mask()?;
                        self.body
                            .row_domain()
                            .demand(piece.coverage.clone())?
                            .refine(&mask)?;
                        cx.tees[self.tee].push(Batch::mask(piece.coverage, mask))?;
                    }
                    Step::Blocked => return Ok(Step::Blocked),
                    Step::Finished => {
                        cx.tees[self.tee].finish();
                        break;
                    }
                }
            }
            self.state = State::PullBody;
        }
        if matches!(self.state, State::Emitted) {
            return Ok(Step::Finished);
        }

        let body = match self.body.next(hint, cx)? {
            Step::Batch(batch) => batch.value.into_array()?,
            Step::Blocked => return Ok(Step::Blocked),
            Step::Finished => return Err(vortex_err!("scan body produced no value")),
        };
        cx.stats().rows_selected += body.len() as u64;
        let array = if body.is_empty() {
            cx.stats().morsels_empty += 1;
            Canonical::empty(&self.dtype).into_array()
        } else {
            body.apply_bound(&self.expr)?
        };
        self.state = State::Emitted;
        Ok(Step::Batch(Batch::array(
            self.domain.range().clone(),
            array,
        )))
    }

    fn close(&mut self, cx: &mut Cx<'_>) {
        self.mask.close(cx);
        self.body.close(cx);
        self.state = State::Emitted;
    }

    fn describe(&self) -> String {
        format!(
            "Project({} over node {}, mask node {})",
            self.expr,
            self.body.id(),
            self.mask.id()
        )
    }
}
