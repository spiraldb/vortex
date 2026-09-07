// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::Range;
use std::sync::Arc;

use vortex_array::IntoArray;
use vortex_array::arrays::StructArray;
use vortex_array::dtype::FieldNames;
use vortex_array::validity::Validity;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_mask::Mask;

use crate::build::NodeBlueprint;
use crate::node::ChildPoll;
use crate::node::ExecCx;
use crate::node::ExecNode;
use crate::node::ExecPoll;
use crate::node::Materialized;
use crate::node::NodeId;
use crate::node::PlanCx;
use crate::node::PlanItem;
use crate::node::PlanPoll;
use crate::node::RetireCx;
use crate::node::Value;
use crate::node::ValueBatch;

/// The blueprint of a struct node.
pub struct StructSpec {
    /// Field names, one per child.
    pub names: FieldNames,
    /// One child per field.
    pub children: Arc<[NodeId]>,
    /// The validity child of a nullable struct.
    pub validity: Option<NodeId>,
}

impl NodeBlueprint for StructSpec {
    fn instantiate(&self, _id: NodeId) -> Box<dyn ExecNode> {
        Box::new(StructExec::new(
            self.names.clone(),
            Arc::clone(&self.children),
            self.validity,
        ))
    }
}

/// Struct is almost nothing: identity edges to each field, then a zip.
///
/// Every field is planned and executed under the *same* hint. Fields may still materialize
/// different rows, because each column's chunks are cut and skipped on its own boundaries, so
/// the zip aligns every field to the rows they all hold before building the struct.
pub struct StructExec {
    names: FieldNames,
    children: Arc<[NodeId]>,
    validity: Option<NodeId>,

    // Per-morsel state.
    range: Range<u64>,
    plan_cursor: usize,
    plan_started: bool,
    exec_cursor: usize,
    fields: Vec<Materialized>,
    validity_array: Option<Materialized>,
    done: bool,
}

impl StructExec {
    /// Build a struct node over one child per projected field.
    pub fn new(names: FieldNames, children: Arc<[NodeId]>, validity: Option<NodeId>) -> Self {
        debug_assert_eq!(names.len(), children.len());
        Self {
            names,
            children,
            validity,
            range: 0..0,
            plan_cursor: 0,
            plan_started: false,
            exec_cursor: 0,
            fields: Vec::new(),
            validity_array: None,
            done: false,
        }
    }
}

impl ExecNode for StructExec {
    fn reset(&mut self, range: Range<u64>) {
        self.range = range;
        self.plan_cursor = 0;
        self.plan_started = false;
        self.exec_cursor = 0;
        self.fields.clear();
        self.validity_array = None;
        self.done = false;
    }

    fn next_plan(&mut self, cx: &mut PlanCx<'_>) -> VortexResult<PlanPoll> {
        let child_count = self.children.len() + usize::from(self.validity.is_some());
        while self.plan_cursor < child_count {
            if cx.out_of_budget() {
                return Ok(PlanPoll::Item(PlanItem::Plan));
            }
            let fresh = !self.plan_started;
            self.plan_started = true;
            let child = if self.plan_cursor < self.children.len() {
                self.children[self.plan_cursor]
            } else {
                self.validity
                    .ok_or_else(|| vortex_err!("struct validity child is missing"))?
            };
            if cx.plan_child(child, self.range.clone(), fresh)? {
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

        let hint = cx.hint().clone();
        if let Some(validity) = self.validity
            && self.validity_array.is_none()
        {
            match cx.child_array(validity, hint.clone())? {
                ChildPoll::Value(array) => self.validity_array = Some(array),
                ChildPoll::Blocked(waits) => return Ok(ExecPoll::Blocked(waits)),
                ChildPoll::Done => {
                    return Err(vortex_err!("struct validity child produced no value"));
                }
            }
        }
        if self.fields.capacity() < self.children.len() {
            self.fields
                .reserve(self.children.len().saturating_sub(self.fields.len()));
        }
        while self.exec_cursor < self.children.len() {
            let child = self.children[self.exec_cursor];
            match cx.child_array(child, hint.clone())? {
                ChildPoll::Value(array) => {
                    self.fields.push(array);
                    self.exec_cursor += 1;
                }
                ChildPoll::Blocked(waits) => return Ok(ExecPoll::Blocked(waits)),
                ChildPoll::Done => {
                    return Err(vortex_err!("struct child {child} produced no value"));
                }
            }
        }

        // Fields agree on their rows unless a column skipped a chunk the others kept. Align
        // everything to the rows every field holds; the hint guarantees nobody needs the rest.
        let common = self
            .fields
            .iter()
            .chain(self.validity_array.iter())
            .map(|field| &field.rows)
            .fold(None::<Mask>, |common, rows| {
                Some(match common {
                    None => rows.clone(),
                    Some(common) if &common == rows => common,
                    Some(common) => &common & rows,
                })
            })
            .unwrap_or_else(|| hint.clone());
        if common.len() != hint.len() {
            return Err(vortex_err!(
                "struct fields materialized {} rows for a range of {}",
                common.len(),
                hint.len()
            ));
        }
        let mut fields = Vec::with_capacity(self.fields.len());
        for field in self.fields.drain(..) {
            fields.push(field.select(&common)?);
        }
        let validity = match self.validity_array.take() {
            Some(validity) => Validity::Array(validity.select(&common)?),
            None => Validity::NonNullable,
        };
        let len = common.true_count();
        let array = StructArray::try_new(self.names.clone(), fields, len, validity)?.into_array();
        self.done = true;

        Ok(ExecPoll::Value(ValueBatch {
            coverage: self.range.clone(),
            materialized: common,
            value: Value::Array(array),
        }))
    }

    fn retire(&mut self, cx: &mut RetireCx<'_>) {
        for &child in self.children.iter() {
            cx.retire_child(child);
        }
        if let Some(validity) = self.validity {
            cx.retire_child(validity);
        }
    }

    fn children(&self) -> &[NodeId] {
        &self.children
    }
}
