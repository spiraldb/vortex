use arrow_buffer::BooleanBuffer;
use itertools::Itertools;
use vortex_error::VortexResult;

use crate::array::bool::{BoolArray, BoolData};
use crate::compute::as_contiguous::AsContiguousFn;
use crate::compute::flatten::flatten_bool;
use crate::validity::Validity;
use crate::{Array, ArrayTrait, IntoArray, WithArray};

impl AsContiguousFn for BoolArray<'_> {
    fn as_contiguous(&self, arrays: &[Array]) -> VortexResult<Array<'static>> {
        let validity = if self.dtype().is_nullable() {
            Validity::from_iter(
                arrays
                    .iter()
                    .map(|a| a.with_array(|a| a.logical_validity())),
            )
        } else {
            Validity::NonNullable
        };

        let mut bools = Vec::with_capacity(arrays.iter().map(|a| a.len()).sum());
        for buffer in arrays
            .iter()
            .map(|a| flatten_bool(a).map(|bool_data| bool_data.as_typed_array().buffer()))
        {
            bools.extend(buffer?.iter().collect_vec())
        }

        Ok(BoolData::try_new(BooleanBuffer::from(bools), validity)?.into_array())
    }
}
