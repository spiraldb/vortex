// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::Range;

use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::arrays::DictArray;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_mask::Mask;

use crate::build::NodeBlueprint;
use crate::node::ChildPoll;
use crate::node::ExecCx;
use crate::node::ExecNode;
use crate::node::ExecPoll;
use crate::node::NodeId;
use crate::node::PlanCx;
use crate::node::PlanItem;
use crate::node::PlanPoll;
use crate::node::RetireCx;
use crate::node::Value;
use crate::node::ValueBatch;

/// The blueprint of a dictionary node.
pub struct DictSpec {
    /// The values subtree, planned over the whole values array.
    pub values: NodeId,
    /// The codes subtree, planned over the morsel's rows.
    pub codes: NodeId,
    /// Number of dictionary values.
    pub values_len: usize,
}

impl NodeBlueprint for DictSpec {
    fn instantiate(&self, id: NodeId) -> Box<dyn ExecNode> {
        Box::new(DictExec::new(id, self.values, self.codes, self.values_len))
    }
}

/// A dictionary values array paired with range-scoped codes.
///
/// Decoded values are shared through the current scan's execution context. They deliberately do
/// not live in this reusable node because arenas can survive across independent scans.
pub struct DictExec {
    node: NodeId,
    values: NodeId,
    codes: NodeId,
    values_len: usize,

    range: Range<u64>,
    plan_cursor: usize,
    plan_started: bool,
    values_array: Option<ArrayRef>,
    codes_array: Option<ArrayRef>,
    values_active: bool,
    done: bool,
}

impl DictExec {
    /// Build a dictionary node from its full values child and row-aligned codes child.
    pub fn new(node: NodeId, values: NodeId, codes: NodeId, values_len: usize) -> Self {
        Self {
            node,
            values,
            codes,
            values_len,
            range: 0..0,
            plan_cursor: 0,
            plan_started: false,
            values_array: None,
            codes_array: None,
            values_active: false,
            done: false,
        }
    }

    fn child_range(&self) -> Range<u64> {
        if self.plan_cursor == 0 {
            0..self.values_len as u64
        } else {
            self.range.clone()
        }
    }

    fn child(&self) -> NodeId {
        if self.plan_cursor == 0 {
            self.values
        } else {
            self.codes
        }
    }
}

impl ExecNode for DictExec {
    fn reset(&mut self, range: Range<u64>) {
        self.range = range;
        self.plan_cursor = 0;
        self.plan_started = false;
        self.values_array = None;
        self.codes_array = None;
        self.values_active = false;
        self.done = false;
    }

    fn next_plan(&mut self, cx: &mut PlanCx<'_>) -> VortexResult<PlanPoll> {
        while self.plan_cursor < 2 {
            if self.plan_cursor == 0 && cx.dictionary_available(self.node) {
                self.plan_cursor = 1;
                self.plan_started = false;
                continue;
            }
            if cx.out_of_budget() {
                return Ok(PlanPoll::Item(PlanItem::Plan));
            }
            let fresh = !self.plan_started;
            self.plan_started = true;
            let demand = if self.plan_cursor == 0 {
                Mask::new_true(self.values_len)
            } else {
                cx.demand().clone()
            };
            if cx.plan_child_with_demand(self.child(), self.child_range(), fresh, demand)? {
                if self.plan_cursor == 0 {
                    self.values_active = true;
                }
                self.plan_cursor += 1;
                self.plan_started = false;
            } else {
                return Ok(PlanPoll::Item(PlanItem::Plan));
            }
        }
        Ok(PlanPoll::Complete)
    }

    fn execute(&mut self, cx: &mut ExecCx<'_>) -> VortexResult<ExecPoll> {
        if self.done {
            return Ok(ExecPoll::Done);
        }

        if let Some(values) = cx.shared_dictionary(self.node) {
            self.values_array = Some(values);
        } else if self.values_array.is_none() {
            match cx.child_array(self.values, Mask::new_true(self.values_len))? {
                ChildPoll::Value(values) => {
                    self.values_array = Some(cx.publish_dictionary(self.node, values));
                }
                ChildPoll::Blocked(waits) => return Ok(ExecPoll::Blocked(waits)),
                ChildPoll::Done => return Err(vortex_err!("dictionary values produced no value")),
            }
        }
        if self.codes_array.is_none() {
            let demand = cx.demand().clone();
            match cx.child_array(self.codes, demand)? {
                ChildPoll::Value(codes) => self.codes_array = Some(codes),
                ChildPoll::Blocked(waits) => return Ok(ExecPoll::Blocked(waits)),
                ChildPoll::Done => return Err(vortex_err!("dictionary codes produced no value")),
            }
        }

        let values = self
            .values_array
            .take()
            .ok_or_else(|| vortex_err!("dictionary values were not initialized"))?;
        let codes = self
            .codes_array
            .take()
            .ok_or_else(|| vortex_err!("dictionary codes were not initialized"))?;
        let array = DictArray::try_new(codes, values)?.into_array();
        self.done = true;
        Ok(ExecPoll::Value(ValueBatch {
            coverage: self.range.clone(),
            value: Value::Array(array),
        }))
    }

    fn retire(&mut self, cx: &mut RetireCx<'_>) {
        if self.values_active {
            cx.retire_child(self.values);
        }
        cx.retire_child(self.codes);
    }

    fn children(&self) -> &[NodeId] {
        &[]
    }
}
