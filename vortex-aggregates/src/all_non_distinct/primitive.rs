// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_array::arrays::primitive::PrimitiveArrayExt;
use vortex_array::dtype::NativePType;
use vortex_array::match_each_native_ptype;
use vortex_error::VortexResult;

pub(super) fn check_primitive_identical<L, R>(lhs: &L, rhs: &R) -> VortexResult<bool>
where
    L: PrimitiveArrayExt,
    R: PrimitiveArrayExt,
{
    match_each_native_ptype!(lhs.ptype(), |P| {
        Ok(lhs
            .as_slice::<P>()
            .iter()
            .zip(rhs.as_slice::<P>())
            .all(|(l, r)| l.is_eq(*r)))
    })
}
