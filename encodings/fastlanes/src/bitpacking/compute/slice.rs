use std::cmp::max;

use vortex::compute::{slice, SliceFn};
use vortex::{Array, IntoArray};
use vortex_error::VortexResult;

use crate::BitPackedArray;

impl SliceFn for BitPackedArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        let offset_start = start + self.offset();
        let offset_stop = stop + self.offset();
        let offset = offset_start % 1024;
        let block_start = max(0, offset_start - offset);
        let block_stop = ((offset_stop + 1023) / 1024) * 1024;

        let encoded_start = (block_start / 8) * self.bit_width();
        let encoded_stop = (block_stop / 8) * self.bit_width();
        let new_packed = self.packed().slice(encoded_start..encoded_stop);

        println!(
            "slice {:?}",
            (
                start,
                stop,
                self.offset(),
                offset,
                self.packed_len(),
                self.len(),
                encoded_start,
                encoded_stop,
            )
        );
        let patch_start = offset_start.saturating_sub(self.packed_len());
        let patch_stop = offset_stop.saturating_sub(self.packed_len());
        let new_patches = if patch_start == patch_stop {
            None
        } else {
            self._patches()
                .map(|(indices, values, indices_offset)| -> VortexResult<_> {
                    Ok((
                        slice(&indices, patch_start, patch_stop)?,
                        slice(&values, patch_start, patch_stop)?,
                        indices_offset + start,
                    ))
                })
                .transpose()?
                .filter(|(indices, ..)| {
                    // No need to keep an empty set of patches
                    !indices.is_empty()
                })
        }; // FIXME(DK): Do I need to apply the offset to the indices eagerly?
        println!("slice {:?}", (patch_start, patch_stop, new_patches.clone()));
        println!("slice {:?}", new_packed.clone());

        // slice the buffer using the encoded start/stop values
        Self::try_new_from_offset(
            new_packed,
            self.ptype(),
            self.validity().slice(start, stop)?,
            new_patches,
            self.bit_width(),
            stop - start,
            offset,
        )
        .map(|a| a.into_array())
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use vortex::array::PrimitiveArray;
    use vortex::compute::unary::scalar_at;
    use vortex::compute::{slice, take};
    use vortex::IntoArray;

    use crate::BitPackedArray;

    #[test]
    pub fn slice_block() {
        let arr = BitPackedArray::encode(
            PrimitiveArray::from((0u32..2048).map(|v| v % 64).collect::<Vec<_>>()).as_ref(),
            6,
        )
        .unwrap()
        .into_array();
        let sliced = BitPackedArray::try_from(slice(&arr, 1024, 2048).unwrap()).unwrap();
        assert_eq!(
            scalar_at(sliced.as_ref(), 0).unwrap(),
            (1024u32 % 64).into()
        );
        assert_eq!(
            scalar_at(sliced.as_ref(), 1023).unwrap(),
            (2047u32 % 64).into()
        );
        assert_eq!(sliced.offset(), 0);
        assert_eq!(sliced.len(), 1024);
    }

    #[test]
    pub fn slice_within_block() {
        let arr = BitPackedArray::encode(
            PrimitiveArray::from((0u32..2048).map(|v| v % 64).collect::<Vec<_>>()).as_ref(),
            6,
        )
        .unwrap()
        .into_array();
        let sliced = BitPackedArray::try_from(slice(&arr, 512, 1434).unwrap()).unwrap();
        assert_eq!(scalar_at(sliced.as_ref(), 0).unwrap(), (512u32 % 64).into());
        assert_eq!(
            scalar_at(sliced.as_ref(), 921).unwrap(),
            (1433u32 % 64).into()
        );
        assert_eq!(sliced.offset(), 512);
        assert_eq!(sliced.len(), 922);
    }

    #[test]
    fn slice_within_block_u8s() {
        let packed = BitPackedArray::encode(
            PrimitiveArray::from((0..10_000).map(|i| (i % 63) as u8).collect::<Vec<_>>()).as_ref(),
            7,
        )
        .unwrap();

        let compressed = slice(packed.as_ref(), 768, 9999).unwrap();
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
            PrimitiveArray::from((0..10_000).map(|i| (i % 63) as u8).collect::<Vec<_>>()).as_ref(),
            7,
        )
        .unwrap();

        let compressed = slice(packed.as_ref(), 7168, 9216).unwrap();
        assert_eq!(
            scalar_at(&compressed, 0).unwrap(),
            ((7168 % 63) as u8).into()
        );
        assert_eq!(
            scalar_at(&compressed, compressed.len() - 1).unwrap(),
            ((9215 % 63) as u8).into()
        );
    }

    #[test]
    fn double_slice_within_block() {
        let arr = BitPackedArray::encode(
            PrimitiveArray::from((0u32..2048).map(|v| v % 64).collect::<Vec<_>>()).as_ref(),
            6,
        )
        .unwrap()
        .into_array();
        let sliced = BitPackedArray::try_from(slice(&arr, 512, 1434).unwrap()).unwrap();
        assert_eq!(scalar_at(sliced.as_ref(), 0).unwrap(), (512u32 % 64).into());
        assert_eq!(
            scalar_at(sliced.as_ref(), 921).unwrap(),
            (1433u32 % 64).into()
        );
        assert_eq!(sliced.offset(), 512);
        assert_eq!(sliced.len(), 922);
        let doubly_sliced =
            BitPackedArray::try_from(slice(sliced.as_ref(), 127, 911).unwrap()).unwrap();
        assert_eq!(
            scalar_at(doubly_sliced.as_ref(), 0).unwrap(),
            ((512u32 + 127) % 64).into()
        );
        assert_eq!(
            scalar_at(doubly_sliced.as_ref(), 783).unwrap(),
            ((512u32 + 910) % 64).into()
        );
        assert_eq!(doubly_sliced.offset(), 639);
        assert_eq!(doubly_sliced.len(), 784);
    }

    #[test]
    fn slice_empty_patches() {
        // We create an array that has 1 element that does not fit in the 6-bit range.
        let array =
            BitPackedArray::encode(PrimitiveArray::from((0u32..=64).collect_vec()).as_ref(), 6)
                .unwrap();

        assert!(array._patches().is_some());

        let patch_indices = array._patches().unwrap().0;
        assert_eq!(patch_indices.len(), 1);

        // Slicing drops the empty patches array.
        let sliced = slice(array, 0, 64).unwrap();
        let sliced_bp = BitPackedArray::try_from(sliced).unwrap();
        assert!(sliced_bp._patches().is_none());
    }

    #[test]
    fn take_after_slice() {
        // Check that our take implementation respects the offsets applied after slicing.

        let array = BitPackedArray::encode(
            PrimitiveArray::from((63u32..).take(3072).collect_vec()).as_ref(),
            6,
        )
        .unwrap();

        // Slice the array.
        // The resulting array will still have 3 1024-element chunks.
        let sliced = slice(array.as_ref(), 922, 2061).unwrap();

        // Take one element from each chunk.
        // Chunk 1: physical indices  922-1023, logical indices    0-101
        // Chunk 2: physical indices 1024-2047, logical indices  102-1125
        // Chunk 3: physical indices 2048-2060, logical indices 1126-1138

        let taken = take(
            &sliced,
            PrimitiveArray::from(vec![101i64, 1125i64, 1138i64]).as_ref(),
        )
        .unwrap();
        assert_eq!(taken.len(), 3);
    }
}
