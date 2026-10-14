// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::arrays::FixedSizeListArray;
use vortex_array::arrays::fixed_size_list::FixedSizeListArraySlotsExt;
use vortex_error::VortexResult;

use super::all_non_distinct;
use super::filter::filter_valid_rows_if_needed;

pub(super) fn check_fixed_size_list_identical(
    lhs: &FixedSizeListArray,
    rhs: &FixedSizeListArray,
    ctx: &mut ExecutionCtx,
) -> VortexResult<bool> {
    if let Some((lhs, rhs)) =
        filter_valid_rows_if_needed(&lhs.clone().into_array(), &rhs.clone().into_array(), ctx)?
    {
        return all_non_distinct(&lhs, &rhs, ctx);
    }

    all_non_distinct(lhs.elements(), rhs.elements(), ctx)
}
