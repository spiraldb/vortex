// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_error::VortexExpect;
use vortex_error::VortexResult;

use super::PreparedSet;
use super::PreparedSetData;
use crate::ArrayRef;
use crate::array::ArrayView;
use crate::arrays::Constant;
use crate::arrays::ScalarFn;
use crate::arrays::scalar_fn::ExactScalarFn;
use crate::arrays::scalar_fn::ScalarFnArrayExt;
use crate::arrays::scalar_fn::ScalarFnArrayView;
use crate::dtype::DType;
use crate::optimizer::rules::ArrayParentReduceRule;
use crate::scalar_fn::fns::list_contains::ListContains;

/// Replaces the constant list of a [`ListContains`] with a [`PreparedSet`], so that push-down
/// rules share one set instead of preparing the constant again for each part of the needle.
#[derive(Debug)]
pub(crate) struct PrepareConstantListRule;

impl ArrayParentReduceRule<Constant> for PrepareConstantListRule {
    type Parent = ExactScalarFn<ListContains>;

    fn reduce_parent(
        &self,
        list: ArrayView<'_, Constant>,
        parent: ScalarFnArrayView<'_, ListContains>,
        child_idx: usize,
    ) -> VortexResult<Option<ArrayRef>> {
        if child_idx != 0 {
            return Ok(None);
        }
        let scalar = list.scalar();
        let DType::List(element_dtype, _) = scalar.dtype() else {
            return Ok(None);
        };
        // A null list has no set, and a needle of another dtype is an error for execution to
        // report.
        let parent_array = parent
            .as_opt::<ScalarFn>()
            .vortex_expect("ExactScalarFn matcher confirmed ScalarFnArray");
        let needle = parent_array.get_child(1);
        if scalar.is_null() || !element_dtype.eq_ignore_nullability(needle.dtype()) {
            return Ok(None);
        }

        let set = PreparedSet::new_array(PreparedSetData::try_new(scalar.clone())?, list.len());
        // SAFETY: every row of the prepared set holds the constant's list.
        Ok(Some(unsafe { (*parent).clone().with_slot(0, set)? }))
    }
}
