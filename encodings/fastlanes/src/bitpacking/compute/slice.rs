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

        let encoded_start = (block_start / 8) * self.bit_width() / self.ptype().byte_width();
        let encoded_stop = (block_stop / 8) * self.bit_width() / self.ptype().byte_width();
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

#[cfg(test)]
mod test {
    use vortex::array::primitive::PrimitiveArray;
    use vortex::compute::slice::slice;
    use vortex::{ArrayTrait, IntoArray};

    use crate::BitPackedArray;

    #[test]
    pub fn slice_block() {
        let arr = BitPackedArray::encode(
            &PrimitiveArray::from((0u32..2048).map(|v| v % 64).collect::<Vec<_>>()).into_array(),
            6,
        )
        .unwrap()
        .into_array();
        let sliced = BitPackedArray::try_from(slice(&arr, 1024, 2048).unwrap()).unwrap();
        assert_eq!(sliced.offset(), 0);
        assert_eq!(sliced.len(), 1024);
    }

    #[test]
    pub fn slice_within_block() {
        let arr = BitPackedArray::encode(
            &PrimitiveArray::from((0u32..2048).map(|v| v % 64).collect::<Vec<_>>()).into_array(),
            6,
        )
        .unwrap()
        .into_array();
        let sliced = BitPackedArray::try_from(slice(&arr, 512, 1434).unwrap()).unwrap();
        assert_eq!(sliced.offset(), 512);
        assert_eq!(sliced.len(), 922);
    }
}
