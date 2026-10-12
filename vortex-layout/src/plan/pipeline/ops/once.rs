// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Emitting one batch.

use vortex_array::ArrayRef;
use vortex_error::VortexResult;

use crate::plan::pipeline::Cx;
use crate::plan::pipeline::Source;
use crate::plan::pipeline::Step;

/// Emits one batch it was built with.
pub(crate) struct OnceSource(Option<ArrayRef>);

impl OnceSource {
    pub(crate) fn new(batch: ArrayRef) -> Self {
        Self(Some(batch))
    }
}

impl Source for OnceSource {
    fn ready(&mut self, _cx: &mut Cx<'_>) -> VortexResult<Step> {
        Ok(match self.0.take() {
            Some(batch) => Step::Batch(batch),
            None => Step::Finished,
        })
    }
}
