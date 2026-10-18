// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_array::ArrayRef;
use vortex_array::ArrayView;
use vortex_array::ExecutionCtx;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::dict::TakeExecute;
use vortex_array::match_each_integer_ptype;
use vortex_error::VortexResult;
use vortex_error::vortex_err;

use super::rows::NO_VALUE;
use super::rows::gather_array;
use super::rows::row_values;
use crate::Pco;

/// Decodes only the blocks holding the taken rows.
impl TakeExecute for Pco {
    fn take(
        array: ArrayView<'_, Self>,
        indices: &ArrayRef,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<ArrayRef>> {
        let primitive_indices = indices.clone().execute::<PrimitiveArray>(ctx)?;
        let indices_mask = primitive_indices
            .validity()?
            .execute_mask(primitive_indices.len(), ctx)?;
        let rows = row_values(array, ctx)?;
        let len = array.len();
        let slice_start = array.slice_start();

        let mut positions = Vec::with_capacity(primitive_indices.len());
        match_each_integer_ptype!(primitive_indices.ptype(), |I| {
            for (i, &index) in primitive_indices.as_slice::<I>().iter().enumerate() {
                if !indices_mask.value(i) {
                    positions.push(NO_VALUE);
                    continue;
                }
                // Widening to i128 first accepts every index type, signed or not.
                let row = usize::try_from(i128::from(index))
                    .ok()
                    .filter(|&row| row < len)
                    .ok_or_else(|| vortex_err!("Pco take index {index} out of bounds for {len}"))?;
                positions.push(rows.value_index(slice_start + row).unwrap_or(NO_VALUE));
            }
        });

        let validity = array.validity()?.take(indices)?;
        gather_array(array.data(), &positions, validity, ctx).map(Some)
    }
}
