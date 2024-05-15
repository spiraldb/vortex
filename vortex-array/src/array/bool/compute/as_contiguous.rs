use arrow_buffer::BooleanBuffer;
use vortex_error::VortexResult;

use crate::array::bool::BoolArray;
use crate::compute::as_contiguous::AsContiguousFn;
use crate::validity::Validity;
use crate::{Array, ArrayDType, IntoArray};

impl AsContiguousFn for BoolArray {
    fn as_contiguous(&self, arrays: &[Array]) -> VortexResult<Array> {
        let validity = if self.dtype().is_nullable() {
            Validity::from_iter(arrays.iter().map(|a| a.with_dyn(|a| a.logical_validity())))
        } else {
            Validity::NonNullable
        };

        let mut bools = Vec::with_capacity(arrays.iter().map(|a| a.len()).sum());
        for buffer in arrays
            .iter()
            .map(|a| Self::try_from(a.clone()).unwrap().boolean_buffer())
        {
            bools.extend(buffer.iter())
        }

        Ok(Self::try_new(BooleanBuffer::from(bools), validity)?.into_array())
    }
}
