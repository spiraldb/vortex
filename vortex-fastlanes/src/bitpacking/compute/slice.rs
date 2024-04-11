use std::cmp::max;

use vortex::array::{Array, ArrayRef};
use vortex::compute::slice::{slice, SliceFn};
use vortex::validity::OwnedValidity;
use vortex_error::VortexResult;

use crate::BitPackedArray;

impl SliceFn for BitPackedArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        let offset = start % 1024;
        let block_start = max(0, start - offset);
        let block_stop = ((stop + 1023) / 1024) * 1024;

        let encoded_start = (block_start / 8) * self.bit_width;
        let encoded_stop = (block_stop / 8) * self.bit_width;
        Ok(Self::try_new_from_offset(
            slice(self.encoded(), encoded_start, encoded_stop)?,
            self.validity().map(|v| v.slice(start, stop)).transpose()?,
            self.patches().map(|p| slice(p, start, stop)).transpose()?,
            self.bit_width(),
            self.dtype().clone(),
            stop - start,
            offset,
        )?
        .into_array())
    }
}
