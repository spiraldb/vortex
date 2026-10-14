// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_error::VortexResult;

use super::all_non_distinct;
use super::filter::filter_valid_rows_if_needed;

pub(super) fn check_struct_identical(
    lhs: &StructArray,
    rhs: &StructArray,
    ctx: &mut ExecutionCtx,
) -> VortexResult<bool> {
    if let Some((lhs, rhs)) =
        filter_valid_rows_if_needed(&lhs.clone().into_array(), &rhs.clone().into_array(), ctx)?
    {
        return all_non_distinct(&lhs, &rhs, ctx);
    }

    for (lhs_field, rhs_field) in lhs.iter_unmasked_fields().zip(rhs.iter_unmasked_fields()) {
        if !all_non_distinct(lhs_field, rhs_field, ctx)? {
            return Ok(false);
        }
    }

    Ok(true)
}
