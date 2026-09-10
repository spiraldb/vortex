// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::BitAnd;

use vortex_array::VortexSessionExecute;
use vortex_array::expr::BoundExpression;
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
use crate::node::filter_rows;
use crate::nodes::EXPR_EVAL_THRESHOLD;

/// One conjunct: the subtree producing its input, and the predicate applied to that input.
pub struct ConjunctSlot {
    /// The subtree producing the fields this predicate reads.
    pub input: Child,
    /// The predicate, bound to the input subtree's output dtype.
    pub predicate: BoundExpression,
}

/// The mask producer of a filtered scan: a cascade of predicate evaluations.
///
/// Each conjunct is evaluated under the mask the previous one produced, and an all-false mask
/// ends the morsel early, as the V1 reader does. Its range is the morsel and its hint is the
/// morsel demand; the root passes both. It answers once per morsel; the root buffers the mask
/// for every filter that reads it.
pub struct ConjunctExec {
    slots: Vec<ConjunctSlot>,

    domain: RowDomain,
    state: State,
}

enum State {
    Evaluating { slot: usize, mask: Mask },
    Emitted,
}

impl ConjunctExec {
    /// Build a conjunct operator over its slots, evaluated in order.
    pub fn new(slots: Vec<ConjunctSlot>, domain: RowDomain) -> Self {
        let mask = domain.snapshot();
        Self {
            slots,
            domain,
            state: State::Evaluating { slot: 0, mask },
        }
    }
}

impl Operator for ConjunctExec {
    fn row_domain(&self) -> &RowDomain {
        &self.domain
    }

    fn look_ahead(&mut self, cx: &mut Cx<'_>) -> VortexResult<LookAhead> {
        let mut result = LookAhead::Complete;
        for slot in &mut self.slots {
            result = result.merge(slot.input.look_ahead(cx)?);
        }
        Ok(result)
    }

    fn next(&mut self, _hint: &Mask, cx: &mut Cx<'_>) -> VortexResult<Step> {
        match &mut self.state {
            State::Emitted => Ok(Step::Finished),
            State::Evaluating { slot, mask } => {
                while *slot < self.slots.len() {
                    if mask.all_false() {
                        cx.stats().conjuncts_short_circuited += (self.slots.len() - *slot) as u64;
                        break;
                    }

                    // The regime switch: over a sparse selection, reduce the input to the
                    // selected rows and evaluate only those; over a dense one, evaluate the
                    // whole range and intersect. Same choice the V1 flat reader makes. The input
                    // comes back dense either way; the hint only lets its leaves skip reads.
                    let eval_demand = mask.clone();
                    let sparse = eval_demand.density() < EXPR_EVAL_THRESHOLD;
                    let child_hint = if sparse {
                        eval_demand.clone()
                    } else {
                        Mask::new_true(eval_demand.len())
                    };
                    let current = &mut self.slots[*slot];
                    let input = match current.input.next(&child_hint, cx)? {
                        Step::Batch(batch) => batch.value.into_array()?,
                        Step::Blocked => return Ok(Step::Blocked),
                        Step::Finished => {
                            return Err(vortex_err!(
                                "conjunct input {} produced no value",
                                current.input.id()
                            ));
                        }
                    };
                    let input = if sparse {
                        filter_rows(input, eval_demand.clone())?
                    } else {
                        input
                    };
                    let predicate = input.apply_bound(&current.predicate)?;
                    let mut ctx = cx.session().create_execution_ctx();
                    let predicate_mask = predicate.null_as_false().execute(&mut ctx)?;
                    *mask = if sparse {
                        eval_demand.intersect_by_rank(&predicate_mask)
                    } else {
                        eval_demand.bitand(&predicate_mask)
                    };
                    *slot += 1;
                    for remaining in &self.slots[*slot..] {
                        remaining.input.row_domain().refine(mask)?;
                    }
                }

                let mask = mask.clone();
                self.state = State::Emitted;
                Ok(Step::Batch(Batch::mask(self.domain.range().clone(), mask)))
            }
        }
    }

    fn close(&mut self, cx: &mut Cx<'_>) {
        for slot in &mut self.slots {
            slot.input.close(cx);
        }
        self.state = State::Emitted;
    }

    fn describe(&self) -> String {
        let slots: Vec<String> = self
            .slots
            .iter()
            .map(|slot| format!("{} over node {}", slot.predicate, slot.input.id()))
            .collect();
        format!("Conjunct({})", slots.join("; "))
    }
}
