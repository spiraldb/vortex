// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::arrays::ConstantArray;
use vortex_array::dtype::DType;
use vortex_array::expr::BoundExpression;
use vortex_array::scalar::Scalar;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_mask::Mask;

use crate::demand::RowDomain;
use crate::node::Batch;
use crate::node::Child;
use crate::node::Cx;
use crate::node::LookAhead;
use crate::node::Operator;
use crate::node::Step;
use crate::nodes::flat::placeholder_scalar;

/// An `Eval` operator: applies an expression to its child's value.
///
/// Below a filter the child already holds only selected rows, so the expression never runs over
/// rows nobody will look at; like a leaf it stands in placeholder rows when the hint says nothing
/// in its range is wanted.
pub struct EvalExec {
    child: Child,
    expression: BoundExpression,
    dtype: DType,
    placeholder: Option<Scalar>,

    domain: RowDomain,
    emitted: bool,
}

impl EvalExec {
    /// Build an evaluation of `expression`, bound to the child's dtype, producing `dtype`.
    pub fn new(child: Child, expression: BoundExpression, dtype: DType, domain: RowDomain) -> Self {
        Self {
            child,
            expression,
            dtype,
            placeholder: None,
            domain,
            emitted: false,
        }
    }
}

impl Operator for EvalExec {
    fn row_domain(&self) -> &RowDomain {
        &self.domain
    }

    fn look_ahead(&mut self, cx: &mut Cx<'_>) -> VortexResult<LookAhead> {
        self.child.look_ahead(cx)
    }

    fn next(&mut self, hint: &Mask, cx: &mut Cx<'_>) -> VortexResult<Step> {
        if self.emitted {
            return Ok(Step::Finished);
        }
        let array: ArrayRef = if hint.all_false() {
            let rows = usize::try_from(self.domain.range().end - self.domain.range().start)
                .vortex_expect("eval range fits usize");
            let scalar = self
                .placeholder
                .get_or_insert_with(|| placeholder_scalar(&self.dtype))
                .clone();
            ConstantArray::new(scalar, rows).into_array()
        } else {
            match self.child.next(hint, cx)? {
                Step::Batch(batch) => batch.value.into_array()?.apply_bound(&self.expression)?,
                Step::Blocked => return Ok(Step::Blocked),
                Step::Finished => {
                    return Err(vortex_err!(
                        "eval input {} produced no value",
                        self.child.id()
                    ));
                }
            }
        };
        self.emitted = true;
        Ok(Step::Batch(Batch::array(
            self.domain.range().clone(),
            array,
        )))
    }

    fn close(&mut self, cx: &mut Cx<'_>) {
        self.child.close(cx);
        self.emitted = true;
    }

    fn describe(&self) -> String {
        format!("Eval({} over node {})", self.expression, self.child.id())
    }
}
