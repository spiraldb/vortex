// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::Range;
use std::sync::Arc;

use vortex_error::VortexResult;

use crate::IoGroupKind;
use crate::io::IoKey;
use crate::node::ExecNode;
use crate::node::IoPrewalkCx;
use crate::node::IoPrewalkPoll;
use crate::node::NodeId;
use crate::node::PlanCx;
use crate::node::PlanPoll;
use crate::node::RetireCx;

/// Pre-walk-only root that orders pruning before the executable scan tree.
pub(crate) struct IoRootExec {
    pruning_uses: Arc<[(IoKey, Range<u64>)]>,
    execution: NodeId,
    include_pruning: bool,
    range: Range<u64>,
    phase: u8,
    pruning_cursor: usize,
    pruning_open: bool,
    execution_started: bool,
}

impl IoRootExec {
    pub(crate) fn new(
        pruning_uses: Arc<[(IoKey, Range<u64>)]>,
        execution: NodeId,
        include_pruning: bool,
    ) -> Self {
        Self {
            pruning_uses,
            execution,
            include_pruning,
            range: 0..0,
            phase: 0,
            pruning_cursor: 0,
            pruning_open: false,
            execution_started: false,
        }
    }
}

impl ExecNode for IoRootExec {
    fn reset(&mut self, range: Range<u64>) {
        self.range = range;
        self.phase = u8::from(!self.include_pruning || self.pruning_uses.is_empty());
        self.pruning_cursor = 0;
        self.pruning_open = false;
        self.execution_started = false;
    }

    fn next_plan(&mut self, _cx: &mut PlanCx<'_>) -> VortexResult<PlanPoll> {
        Ok(PlanPoll::Complete)
    }

    fn next_io(&mut self, cx: &mut IoPrewalkCx<'_>) -> VortexResult<IoPrewalkPoll> {
        if self.phase == 0 {
            if !self.pruning_open {
                cx.group_begin(IoGroupKind::Pruning)?;
                self.pruning_open = true;
            }
            while self.pruning_cursor < self.pruning_uses.len() {
                let (key, source_range) = &self.pruning_uses[self.pruning_cursor];
                if source_range.end <= self.range.start || source_range.start >= self.range.end {
                    self.pruning_cursor += 1;
                    continue;
                }
                if cx.out_of_budget() {
                    return Ok(IoPrewalkPoll::Yield);
                }
                cx.read(*key)?;
                self.pruning_cursor += 1;
            }
            cx.group_end()?;
            self.pruning_open = false;
            self.phase = 1;
            return Ok(IoPrewalkPoll::GroupEnd {
                subtree_complete: false,
            });
        }

        let poll = cx.prewalk_child(self.execution, self.range.clone(), !self.execution_started)?;
        self.execution_started = true;
        match poll {
            IoPrewalkPoll::GroupEnd { subtree_complete } => {
                if subtree_complete {
                    self.phase = 2;
                }
                Ok(IoPrewalkPoll::GroupEnd { subtree_complete })
            }
            IoPrewalkPoll::Complete => {
                self.phase = 2;
                Ok(IoPrewalkPoll::Complete)
            }
            IoPrewalkPoll::Yield => Ok(IoPrewalkPoll::Yield),
        }
    }

    fn retire(&mut self, _cx: &mut RetireCx<'_>) {}
}
