use std::cmp::max;

use vortex::compute::slice::{slice, SliceFn};
use vortex::{Array, IntoArray};
use vortex_error::VortexResult;

use crate::BitPackedArray;

impl SliceFn for BitPackedArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        let offset = start % 1024;
        let block_start = max(0, start - offset);
        let block_stop = ((stop + 1023) / 1024) * 1024;

        let encoded_start = (block_start / 8) * self.bit_width();
        let encoded_stop = (block_stop / 8) * self.bit_width();
        Self::try_new_from_offset(
            slice(&self.packed(), encoded_start, encoded_stop)?,
            self.validity().slice(start, stop)?,
            self.patches().map(|p| slice(&p, start, stop)).transpose()?,
            self.bit_width(),
            stop - start,
            offset,
        )
        .map(|a| a.into_array())
    }
}
