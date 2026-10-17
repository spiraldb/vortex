// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Optional execution of independently admitted child projections.

use std::sync::Arc;

use futures::future::BoxFuture;
use vortex_array::ArrayRef;
use vortex_error::VortexResult;

use super::Split;
use super::ValueCache;
use crate::plan::physical::PlanRef;

pub(crate) struct ChildWork {
    pub(crate) split: Split,
    /// Whole touched fixed-width chunks, including nullable validity.
    pub(crate) bytes: u64,
}

pub(crate) trait ChildExecutor: Send + Sync {
    /// Returns results in work order. Dropping the future cancels its admitted children.
    fn execute(
        self: Arc<Self>,
        plan: PlanRef,
        work: Vec<ChildWork>,
        values: ValueCache,
    ) -> BoxFuture<'static, VortexResult<Vec<ArrayRef>>>;
}
