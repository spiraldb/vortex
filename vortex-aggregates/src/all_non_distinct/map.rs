// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_array::ExecutionCtx;
use vortex_array::arrays::ListView;
use vortex_array::arrays::MapArray;
use vortex_array::arrays::map::MapArrayExt;
use vortex_array::arrays::map::MapArraySlotsExt;
use vortex_error::VortexResult;

use super::list::check_list_identical;

pub(super) fn check_map_identical(
    lhs: &MapArray,
    rhs: &MapArray,
    ctx: &mut ExecutionCtx,
) -> VortexResult<bool> {
    if lhs.map_dtype() != rhs.map_dtype() {
        return Ok(false);
    }

    check_list_identical(
        &lhs.entries().as_::<ListView>().into_owned(),
        &rhs.entries().as_::<ListView>().into_owned(),
        ctx,
    )
}
