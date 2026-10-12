// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Concatenating inlets in order.

use vortex_error::VortexResult;

use super::next_batch;
use crate::plan::pipeline::Cx;
use crate::plan::pipeline::Source;
use crate::plan::pipeline::Step;

/// Emits its inlets one after another, each to its end.
pub(crate) struct ConcatSource {
    inlets: usize,
    /// The inlet being emitted; those before it have ended.
    current: usize,
}

impl ConcatSource {
    pub(crate) fn new(inlets: usize) -> Self {
        Self { inlets, current: 0 }
    }
}

impl Source for ConcatSource {
    fn inlet_count(&self) -> usize {
        self.inlets
    }

    fn next(&mut self, cx: &mut Cx<'_>) -> VortexResult<Step> {
        while self.current < self.inlets {
            match next_batch(cx, self.current) {
                Step::Finished => self.current += 1,
                step => return Ok(step),
            }
        }
        Ok(Step::Finished)
    }
}
