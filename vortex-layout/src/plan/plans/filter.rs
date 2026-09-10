// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::borrow::Cow;

use vortex_array::EmptyMetadata;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use vortex_session::registry::CachedId;

use crate::plan::Plan;
use crate::plan::PlanChildren;
use crate::plan::PlanId;
use crate::plan::PlanParts;
use crate::plan::PlanRef;
use crate::plan::PlanVTable;
use crate::plan::check_child_count;

const INPUT: usize = 0;
const MASK: usize = 1;

/// Keeps the rows of `input` that `mask` selects, with children ordered as `[input, mask]`.
///
/// The row domain is the input's: the plan accounts for every input row and its value holds
/// the subset the mask keeps, in order. The mask is a boolean plan over the same domain; nulls
/// count as false.
#[derive(Clone, Debug)]
pub struct Filter;

/// A plan that keeps the rows of one child selected by another.
pub type FilterPlan = Plan<Filter>;

impl FilterPlan {
    /// Creates a filter of `input` by `mask`.
    pub fn try_new(input: PlanRef, mask: PlanRef) -> VortexResult<Self> {
        validate_mask(&input, &mask)?;
        // SAFETY: The mask dtype and row count were validated against the input above.
        Ok(unsafe { Self::new_unchecked(input, mask) })
    }

    /// Creates a filter without validating the mask.
    ///
    /// # Safety
    ///
    /// `mask` must produce booleans over exactly `input`'s row count.
    pub unsafe fn new_unchecked(input: PlanRef, mask: PlanRef) -> Self {
        PlanParts {
            vtable: Filter,
            dtype: input.dtype().clone(),
            row_count: input.row_count(),
            children: vec![input, mask].into(),
            data: (),
        }
        .into_typed()
    }

    /// Returns the plan whose rows are kept.
    pub fn input(&self) -> VortexResult<PlanRef> {
        self.child_required(INPUT)
    }

    /// Returns the boolean plan selecting the rows to keep.
    pub fn mask(&self) -> VortexResult<PlanRef> {
        self.child_required(MASK)
    }
}

fn validate_mask(input: &PlanRef, mask: &PlanRef) -> VortexResult<()> {
    if !mask.dtype().is_boolean() {
        vortex_bail!("Filter mask must be boolean, got {}", mask.dtype());
    }
    if mask.row_count() != input.row_count() {
        vortex_bail!(
            "Filter mask has {} rows but the input has {}",
            mask.row_count(),
            input.row_count()
        );
    }
    Ok(())
}

impl PlanVTable for Filter {
    type PlanData = ();
    type Metadata = EmptyMetadata;

    fn id(&self) -> PlanId {
        static ID: CachedId = CachedId::new("vortex.plan.filter");
        *ID
    }

    fn metadata(_plan: &Plan<Self>) -> Option<Self::Metadata> {
        Some(EmptyMetadata)
    }

    fn with_children(
        plan: &Plan<Self>,
        children: &PlanChildren,
        _data: &mut Self::PlanData,
    ) -> VortexResult<()> {
        check_child_count("Filter", children, 2)?;
        let input = children
            .get(INPUT)?
            .ok_or_else(|| vortex_err!("Filter input child is absent"))?;
        let mask = children
            .get(MASK)?
            .ok_or_else(|| vortex_err!("Filter mask child is absent"))?;
        if input.row_count() != plan.row_count() || input.dtype() != plan.dtype() {
            vortex_bail!("Filter input shape does not match the plan output");
        }
        validate_mask(&input, &mask)
    }

    fn child_name(_plan: &Plan<Self>, index: usize) -> Cow<'_, str> {
        match index {
            INPUT => Cow::Borrowed("input"),
            MASK => Cow::Borrowed("mask"),
            _ => Cow::Owned(format!("child[{index}]")),
        }
    }
}
