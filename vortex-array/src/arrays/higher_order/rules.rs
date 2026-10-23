// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Slice and filter pushdown for [`HigherOrderArray`], as scalar function
//! arrays have: selection comes before evaluation, so a lambda never runs on unselected rows.

use std::sync::Arc;

use itertools::Itertools;
use vortex_error::VortexResult;

use super::HigherOrder;
use super::HigherOrderArray;
use super::HigherOrderArrayExt;
use crate::ArrayRef;
use crate::IntoArray;
use crate::array::ArrayView;
use crate::arrays::Filter;
use crate::arrays::Slice;
use crate::optimizer::rules::ArrayParentReduceRule;
use crate::optimizer::rules::ParentRuleSet;

pub(super) const PARENT_RULES: ParentRuleSet<HigherOrder> = ParentRuleSet::new(&[
    ParentRuleSet::lift(&HigherOrderFilterPushdownRule),
    ParentRuleSet::lift(&HigherOrderSliceReduceRule),
]);

#[derive(Debug)]
struct HigherOrderSliceReduceRule;

impl ArrayParentReduceRule<HigherOrder> for HigherOrderSliceReduceRule {
    type Parent = Slice;

    fn reduce_parent(
        &self,
        array: ArrayView<'_, HigherOrder>,
        parent: ArrayView<'_, Slice>,
        _child_idx: usize,
    ) -> VortexResult<Option<ArrayRef>> {
        let range = parent.slice_range();
        let children: Vec<_> = array
            .children()
            .iter()
            .map(|child| child.slice(range.clone()))
            .try_collect()?;
        Ok(Some(
            HigherOrderArray::try_new(
                array.function.clone(),
                Arc::clone(&array.lambdas),
                children,
                range.len(),
            )?
            .into_array(),
        ))
    }
}

#[derive(Debug)]
struct HigherOrderFilterPushdownRule;

impl ArrayParentReduceRule<HigherOrder> for HigherOrderFilterPushdownRule {
    type Parent = Filter;

    fn reduce_parent(
        &self,
        array: ArrayView<'_, HigherOrder>,
        parent: ArrayView<'_, Filter>,
        _child_idx: usize,
    ) -> VortexResult<Option<ArrayRef>> {
        let children: Vec<_> = array
            .children()
            .iter()
            .map(|child| child.filter(parent.filter_mask().clone()))
            .try_collect()?;
        Ok(Some(
            HigherOrderArray::try_new(
                array.function.clone(),
                Arc::clone(&array.lambdas),
                children,
                parent.len(),
            )?
            .into_array(),
        ))
    }
}
