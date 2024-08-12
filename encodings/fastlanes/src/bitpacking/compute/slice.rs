use std::cmp::max;

use vortex::compute::{slice, SliceFn};
use vortex::Array;
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
        .map(vortex::IntoArray::into_array)
    }
}

#[cfg(test)]
mod test {
    use vortex::array::PrimitiveArray;
    use vortex::compute::slice;
    use vortex::compute::unary::scalar_at;
    use vortex::IntoArray;

    use crate::BitPackedArray;

    #[test]
    pub fn slice_block() {
        let arr = BitPackedArray::encode(
            PrimitiveArray::from((0u32..2048).map(|v| v % 64).collect::<Vec<_>>()).array(),
            6,
        )
        .unwrap()
        .into_array();
        let sliced = BitPackedArray::try_from(slice(&arr, 1024, 2048).unwrap()).unwrap();
        assert_eq!(scalar_at(sliced.array(), 0).unwrap(), (1024u32 % 64).into());
        assert_eq!(
            scalar_at(sliced.array(), 1023).unwrap(),
            (2047u32 % 64).into()
        );
        assert_eq!(sliced.offset(), 0);
        assert_eq!(sliced.len(), 1024);
    }

    #[test]
    pub fn slice_within_block() {
        let arr = BitPackedArray::encode(
            PrimitiveArray::from((0u32..2048).map(|v| v % 64).collect::<Vec<_>>()).array(),
            6,
        )
        .unwrap()
        .into_array();
        let sliced = BitPackedArray::try_from(slice(&arr, 512, 1434).unwrap()).unwrap();
        assert_eq!(scalar_at(sliced.array(), 0).unwrap(), (512u32 % 64).into());
        assert_eq!(
            scalar_at(sliced.array(), 921).unwrap(),
            (1433u32 % 64).into()
        );
        assert_eq!(sliced.offset(), 512);
        assert_eq!(sliced.len(), 922);
    }

    #[test]
    fn slice_within_block_u8s() {
        let packed = BitPackedArray::encode(
            PrimitiveArray::from((0..10_000).map(|i| (i % 63) as u8).collect::<Vec<_>>()).array(),
            7,
        )
        .unwrap();

        let compressed = slice(packed.array(), 768, 9999).unwrap();
        assert_eq!(
            scalar_at(&compressed, 0).unwrap(),
            ((768 % 63) as u8).into()
        );
        assert_eq!(
            scalar_at(&compressed, compressed.len() - 1).unwrap(),
            ((9998 % 63) as u8).into()
        );
    }

    #[test]
    fn slice_block_boundary_u8s() {
        let packed = BitPackedArray::encode(
            PrimitiveArray::from((0..10_000).map(|i| (i % 63) as u8).collect::<Vec<_>>()).array(),
            7,
        )
        .unwrap();

        let compressed = slice(packed.array(), 7168, 9216).unwrap();
        assert_eq!(
            scalar_at(&compressed, 0).unwrap(),
            ((7168 % 63) as u8).into()
        );
        assert_eq!(
            scalar_at(&compressed, compressed.len() - 1).unwrap(),
            ((9215 % 63) as u8).into()
        );
    }
}
