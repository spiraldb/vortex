// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Selected plan views handed to the enclosing planner for execution.

use std::fmt;

use vortex_error::VortexResult;
use vortex_session::VortexSession;

use super::Scan;
use super::Split;
use super::ValueCache;
use crate::plan::physical::PlanRef;

/// A shared physical plan and its selected row domains. Constructing the view does not lower
/// children or compile a graph; the planner compiles only children touched by these splits.
#[derive(Debug)]
pub(crate) struct SelectedPlan {
    pub(crate) plan: PlanRef,
    pub(crate) splits: Vec<Split>,
    max_active: usize,
}

impl SelectedPlan {
    pub(crate) fn new(plan: PlanRef, splits: Vec<Split>, max_active: usize) -> Self {
        Self {
            plan,
            splits,
            max_active,
        }
    }

    pub(crate) fn into_scan(self, session: VortexSession, values: ValueCache) -> VortexResult<Scan> {
        Ok(Scan::try_new(session, self.plan, self.splits)?
            .with_value_cache(values)
            .with_max_active(self.max_active))
    }
}

/// A source's selected child projection, handed to its enclosing planner for execution.
pub struct ChildProjection {
    pub(crate) id: u64,
    pub(crate) view: SelectedPlan,
    /// The planner must execute this view even when parallel subdivision is unsuitable.
    pub(crate) planner_owned: bool,
    pub(crate) values: ValueCache,
}

impl ChildProjection {
    /// Creates the selected child scan at the planner boundary, returning its completion id.
    /// Drive this scan and pass its output to [`Scan::complete_child`] on the requesting scan.
    pub fn into_scan(self, session: VortexSession) -> VortexResult<(u64, Scan)> {
        Ok((
            self.id,
            self.view
                .into_scan(session, self.values)?
                .with_child_planning(true),
        ))
    }
}

impl fmt::Debug for ChildProjection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChildProjection")
            .field("id", &self.id)
            .field("view", &self.view)
            .field("planner_owned", &self.planner_owned)
            .finish_non_exhaustive()
    }
}
