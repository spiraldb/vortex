// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::Range;

use vortex_array::Canonical;
use vortex_array::IntoArray;
use vortex_array::dtype::DType;
use vortex_array::expr::BoundExpression;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_mask::Mask;

use crate::build::NodeBlueprint;
use crate::io::IoPriority;
use crate::node::ChildPoll;
use crate::node::ExecCx;
use crate::node::ExecNode;
use crate::node::ExecPoll;
use crate::node::NodeId;
use crate::node::PlanCx;
use crate::node::PlanPoll;
use crate::node::RetireCx;
use crate::node::Value;
use crate::node::ValueBatch;
use crate::node::filter_rows;

/// The blueprint of the root filter node.
pub struct FilterSpec {
    /// The conjunct node, if the scan has a filter.
    pub predicate: Option<NodeId>,
    /// The projection input subtree.
    pub projection: NodeId,
    /// The projection, bound to the input subtree's dtype.
    pub expr: BoundExpression,
    /// The output dtype.
    pub dtype: DType,
}

impl NodeBlueprint for FilterSpec {
    fn instantiate(&self, _id: NodeId) -> Box<dyn ExecNode> {
        Box::new(FilterExec::new(
            self.predicate,
            self.projection,
            self.expr.clone(),
            self.dtype.clone(),
        ))
    }
}

/// The root of a morsel: run the conjuncts to get the actual selection, hint the projection
/// with it, then apply it to whatever the projection materialized.
///
/// This is the only node that turns a selection into dropped rows. Leaves below it never filter;
/// they hand up their whole range (or placeholder rows where the hint told them nothing is
/// wanted), and this node applies the mask it computed.
pub struct FilterExec {
    predicate: Option<NodeId>,
    projection: NodeId,
    projection_expr: BoundExpression,
    output_dtype: DType,

    // Per-morsel state.
    range: Range<u64>,
    mask: Option<Mask>,
    done: bool,
    children: Vec<NodeId>,
}

impl FilterExec {
    /// Build a filter node.
    pub fn new(
        predicate: Option<NodeId>,
        projection: NodeId,
        projection_expr: BoundExpression,
        output_dtype: DType,
    ) -> Self {
        let children = predicate.into_iter().chain([projection]).collect();
        Self {
            predicate,
            projection,
            projection_expr,
            output_dtype,
            range: 0..0,
            mask: None,
            done: false,
            children,
        }
    }
}

impl ExecNode for FilterExec {
    fn reset(&mut self, range: Range<u64>) {
        self.range = range;
        self.mask = None;
        self.done = false;
    }

    fn next_plan(&mut self, cx: &mut PlanCx<'_>) -> VortexResult<PlanPoll> {
        // The predicate's reads are required; the projection's are speculative until the mask
        // proves them needed.
        if let Some(predicate) = self.predicate
            && let PlanPoll::Blocked(waits) =
                cx.plan_child_with_priority(predicate, self.range.clone(), IoPriority::Required)?
        {
            return Ok(PlanPoll::Blocked(waits));
        }
        let priority = if self.predicate.is_some() {
            IoPriority::Speculative
        } else {
            IoPriority::Required
        };
        cx.plan_child_with_priority(self.projection, self.range.clone(), priority)
    }

    fn execute(&mut self, cx: &mut ExecCx<'_>) -> VortexResult<ExecPoll> {
        if self.done {
            return Ok(ExecPoll::Done);
        }
        if self.mask.is_none() {
            // The morsel's own selection is the caller's, and therefore actual; the conjuncts
            // only narrow it.
            let selection = cx.hint().clone();
            let mask = match self.predicate {
                Some(predicate) => match cx.child_mask(predicate, selection)? {
                    ChildPoll::Value(mask) => mask,
                    ChildPoll::Blocked(waits) => return Ok(ExecPoll::Blocked(waits)),
                    ChildPoll::Done => {
                        return Err(vortex_err!("filter predicate produced no value"));
                    }
                },
                None => selection,
            };

            if mask.all_false() {
                self.done = true;
                cx.stats().morsels_empty += 1;
                return Ok(ExecPoll::Value(ValueBatch {
                    coverage: self.range.clone(),
                    value: Value::Array(Canonical::empty(&self.output_dtype).into_array()),
                }));
            }
            self.mask = Some(mask);
        }

        // The selection is the projection's hint: chunks it leaves untouched are never read or
        // decoded. The projection comes back dense over the range, and the mask is applied here.
        let mask = self
            .mask
            .as_ref()
            .vortex_expect("non-empty predicate mask is retained")
            .clone();
        let projected = match cx.child_array(self.projection, mask.clone())? {
            ChildPoll::Value(projected) => projected,
            ChildPoll::Blocked(waits) => return Ok(ExecPoll::Blocked(waits)),
            ChildPoll::Done => return Err(vortex_err!("filter projection produced no value")),
        };
        let array = filter_rows(projected, mask)?;
        cx.stats().rows_selected += array.len() as u64;
        let array = array.apply_bound(&self.projection_expr)?;
        self.mask = None;
        self.done = true;

        Ok(ExecPoll::Value(ValueBatch {
            coverage: self.range.clone(),
            value: Value::Array(array),
        }))
    }

    fn retire(&mut self, cx: &mut RetireCx<'_>) {
        self.mask = None;
        for &child in &self.children {
            cx.retire_child(child);
        }
    }

    fn children(&self) -> &[NodeId] {
        &self.children
    }
}
