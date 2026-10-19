// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Descriptions of child projections for the scan planner to schedule.

use std::fmt;

use super::Split;
use super::ValueCache;
use crate::plan::physical::PlanRef;

/// A source's child projection, handed to its enclosing planner for execution.
/// Only scans configured by that planner produce these requests.
pub struct ChildProjection {
    pub(crate) id: u64,
    pub(crate) plan: PlanRef,
    pub(crate) splits: Vec<Split>,
    pub(crate) values: ValueCache,
}

impl fmt::Debug for ChildProjection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChildProjection")
            .field("id", &self.id)
            .field("plan", &self.plan)
            .field("splits", &self.splits)
            .finish_non_exhaustive()
    }
}
