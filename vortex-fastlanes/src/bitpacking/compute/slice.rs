use std::cmp::max;

use vortex::compute::slice::{slice, SliceFn};
use vortex::{ArrayDType, IntoArray, OwnedArray, ToStatic};
use vortex_error::VortexResult;

use crate::BitPackedArray;

impl SliceFn for BitPackedArray<'_> {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<OwnedArray> {
        let offset = start % 1024;
        let block_start = max(0, start - offset);
        let block_stop = ((stop + 1023) / 1024) * 1024;

        let encoded_start = (block_start / 8) * self.bit_width();
        let encoded_stop = (block_stop / 8) * self.bit_width();
        Self::try_new_from_offset(
            slice(&self.encoded(), encoded_start, encoded_stop)?,
            self.validity().slice(start, stop)?,
            self.patches().map(|p| slice(&p, start, stop)).transpose()?,
            self.bit_width(),
            self.dtype().clone(),
            stop - start,
            offset,
        )
        .map(|a| a.into_array().to_static())
    }
}
