// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::collections::VecDeque;
use std::ops::Range;

use vortex_array::IntoArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_mask::AllOr;

use crate::node::ActivationRows;
use crate::node::ExecNode;
use crate::node::NodeState;
use crate::node::PlanCx;
use crate::node::PlanPoll;
use crate::node::PushBatch;
use crate::node::PushCx;
use crate::node::RetireCx;
use crate::node::StageOutput;
use crate::node::Value;

/// A zero-I/O source that materializes the absolute row index for the requested rows.
pub struct RowIdxExec {
    row_offset: u64,
    range: Range<u64>,
    received_end: u64,
    pending: VecDeque<(Range<u64>, ActivationRows)>,
    output_credit: bool,
    done: bool,
}

impl RowIdxExec {
    pub(crate) fn new(row_offset: u64) -> Self {
        Self {
            row_offset,
            range: 0..0,
            received_end: 0,
            pending: VecDeque::new(),
            output_credit: true,
            done: false,
        }
    }

    fn emit(&mut self, out: &mut StageOutput) -> VortexResult<NodeState> {
        if !self.output_credit {
            return Ok(NodeState::NeedInput);
        }
        let Some((span, rows)) = self.pending.pop_front() else {
            return Ok(NodeState::NeedInput);
        };
        let first = self
            .row_offset
            .checked_add(span.start)
            .ok_or_else(|| vortex_err!("row-index offset overflow"))?;
        let end = self
            .row_offset
            .checked_add(span.end)
            .ok_or_else(|| vortex_err!("row-index offset overflow"))?;
        let array = match rows.materialized().indices() {
            AllOr::All => PrimitiveArray::from_iter(first..end).into_array(),
            AllOr::None => PrimitiveArray::from_iter(std::iter::empty::<u64>()).into_array(),
            AllOr::Some(indices) => {
                let values = indices
                    .iter()
                    .map(|&index| {
                        first
                            .checked_add(u64::try_from(index).map_err(|_| {
                                vortex_err!("row-index materialization offset exceeds u64")
                            })?)
                            .ok_or_else(|| vortex_err!("row-index offset overflow"))
                    })
                    .collect::<VortexResult<Vec<_>>>()?;
                PrimitiveArray::from_iter(values).into_array()
            }
        };
        let last = span.end == self.range.end;
        self.output_credit = false;
        self.done = last;
        out.set_batch(
            PushBatch::try_new_materialized(span, rows, Value::Array(array))?,
            last,
        );
        Ok(if last {
            NodeState::Done
        } else {
            NodeState::NeedInput
        })
    }
}

impl ExecNode for RowIdxExec {
    fn reset(&mut self, range: Range<u64>) {
        self.range = range;
        self.received_end = self.range.start;
        self.pending.clear();
        self.output_credit = true;
        self.done = false;
    }

    fn next_plan(&mut self, _cx: &mut PlanCx<'_>) -> VortexResult<PlanPoll> {
        Ok(PlanPoll::Complete)
    }

    fn push_start(
        &mut self,
        span: Range<u64>,
        rows: ActivationRows,
        _cx: &mut PushCx<'_>,
        out: &mut StageOutput,
    ) -> VortexResult<NodeState> {
        if self.done
            || span.start < self.range.start
            || span.end > self.range.end
            || span.is_empty()
            || span.start != self.received_end
            || u64::try_from(rows.logical().len()).ok() != Some(span.end - span.start)
        {
            return Err(vortex_err!(
                "invalid row-index activation {span:?} for {:?}",
                self.range
            ));
        }

        self.received_end = span.end;
        self.pending.push_back((span, rows));
        self.emit(out)
    }

    fn push_credit(
        &mut self,
        _cx: &mut PushCx<'_>,
        out: &mut StageOutput,
    ) -> VortexResult<NodeState> {
        if self.output_credit {
            return Err(vortex_err!(
                "row-index source received duplicate output credit"
            ));
        }
        self.output_credit = true;
        self.emit(out)
    }

    fn retire(&mut self, _cx: &mut RetireCx<'_>) {
        self.pending.clear();
        self.done = false;
    }
}
